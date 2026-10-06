package io.github.mgeladzerezo.pgscheduler.cron;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * A five-field cron expression ({@code minute hour day-of-month month day-of-week}) and its next-fire-time
 * calculator. Immutable and thread-safe.
 *
 * <h2>Supported syntax</h2>
 * <ul>
 *   <li>{@code *}, single values, lists ({@code 1,15}), ranges ({@code 9-17}, wrapping ranges such as
 *       {@code 22-2} or {@code FRI-MON}), steps ({@code *}{@code /5}, {@code 10-40/10}, {@code 5/15}).</li>
 *   <li>Month names {@code JAN}-{@code DEC} and day names {@code SUN}-{@code SAT}, case-insensitive.
 *       Day-of-week accepts 0-7, where both 0 and 7 are Sunday.</li>
 *   <li>Day-of-month: {@code L} (last day of the month) and {@code L-n} (n days before the last day).</li>
 *   <li>Day-of-week: {@code 5L} / {@code FRIL} (last Friday of the month) and {@code 1#2} / {@code MON#2}
 *       (second Monday of the month, n from 1 to 5).</li>
 *   <li>{@code ?} as a synonym for {@code *} in the two day fields.</li>
 *   <li>Macros {@code @yearly}, {@code @annually}, {@code @monthly}, {@code @weekly}, {@code @daily},
 *       {@code @midnight}, {@code @hourly}.</li>
 * </ul>
 * Not supported, and rejected at parse time: seconds and year fields, {@code W} (nearest weekday),
 * {@code LW}, a bare {@code L} in day-of-week, and {@code @reboot}.
 *
 * <h2>Day-of-month and day-of-week together</h2>
 * As in Vixie cron: when both day fields are restricted (neither is {@code *} or {@code ?}) a date matches
 * when <em>either</em> field matches. When only one is restricted, only that one counts.
 *
 * <h2>Time zones and daylight saving time</h2>
 * The expression is evaluated against the wall clock of the zone passed to {@link #next}.
 * <ul>
 *   <li><b>Spring-forward gap</b>: wall-clock times that do not exist fire once, at the first instant after
 *       the gap. {@code 30 2 * * *} on a day when 02:00-03:00 is skipped fires at 03:00. Several matching
 *       times inside one gap collapse into that single fire; a job is never skipped and never doubled.</li>
 *   <li><b>Fall-back overlap</b>: a wall-clock time that occurs twice fires on its first occurrence only,
 *       unless the hour field selects every hour. Schedules that run every hour ({@code * * * * *},
 *       {@code *}{@code /15 * * * *}) are interval-like, so they keep firing through both passes and never
 *       leave an hour of real time uncovered.</li>
 * </ul>
 */
public final class CronExpression {

    /**
     * How far ahead to look for a matching date. The rarest satisfiable date pattern this syntax can express
     * (a fifth given weekday in February) recurs every 28 years and skips to 40 across a non-leap century.
     */
    private static final int SEARCH_YEARS = 100;

    private static final Map<String, Integer> NO_NAMES = Map.of();
    private static final Map<String, Integer> MONTH_NAMES = Map.ofEntries(
            Map.entry("JAN", 1), Map.entry("FEB", 2), Map.entry("MAR", 3), Map.entry("APR", 4),
            Map.entry("MAY", 5), Map.entry("JUN", 6), Map.entry("JUL", 7), Map.entry("AUG", 8),
            Map.entry("SEP", 9), Map.entry("OCT", 10), Map.entry("NOV", 11), Map.entry("DEC", 12));
    private static final Map<String, Integer> DAY_NAMES = Map.of(
            "SUN", 0, "MON", 1, "TUE", 2, "WED", 3, "THU", 4, "FRI", 5, "SAT", 6);
    private static final Map<String, String> MACROS = Map.of(
            "@yearly", "0 0 1 1 *",
            "@annually", "0 0 1 1 *",
            "@monthly", "0 0 1 * *",
            "@weekly", "0 0 * * 0",
            "@daily", "0 0 * * *",
            "@midnight", "0 0 * * *",
            "@hourly", "0 * * * *");

    private static final long ALL_HOURS = (1L << 24) - 1;

    private final String source;
    private final long minutes;
    private final long hours;
    private final long months;

    /** Plain days of month (bits 1-31) and "n days before the last day" offsets (bits 0-30). */
    private final long daysOfMonth;
    private final long lastDayOffsets;
    private final boolean dayOfMonthRestricted;

    /** Plain weekdays (bits 0-6, Sunday = 0), weekdays whose last occurrence matches, and nth occurrences. */
    private final long daysOfWeek;
    private final long lastWeekdays;
    private final long[] nthWeekdays;
    private final boolean dayOfWeekRestricted;

    private CronExpression(String source, long minutes, long hours, long months, long daysOfMonth,
                           long lastDayOffsets, boolean dayOfMonthRestricted, long daysOfWeek,
                           long lastWeekdays, long[] nthWeekdays, boolean dayOfWeekRestricted) {
        this.source = source;
        this.minutes = minutes;
        this.hours = hours;
        this.months = months;
        this.daysOfMonth = daysOfMonth;
        this.lastDayOffsets = lastDayOffsets;
        this.dayOfMonthRestricted = dayOfMonthRestricted;
        this.daysOfWeek = daysOfWeek;
        this.lastWeekdays = lastWeekdays;
        this.nthWeekdays = nthWeekdays;
        this.dayOfWeekRestricted = dayOfWeekRestricted;
    }

    /**
     * Parses a five-field expression or one of the supported macros.
     *
     * @throws CronParseException if the expression is malformed, uses an unsupported feature, or describes a
     *                            date that never occurs (for example {@code 0 0 31 2 *})
     */
    public static CronExpression parse(String expression) {
        if (expression == null) {
            throw new CronParseException("cron expression is null");
        }
        String trimmed = expression.trim();
        String expanded = trimmed;
        if (trimmed.startsWith("@")) {
            expanded = MACROS.get(trimmed.toLowerCase(Locale.ROOT));
            if (expanded == null) {
                throw new CronParseException("unknown macro '" + trimmed + "'");
            }
        }
        String[] fields = expanded.split("\\s+");
        if (fields.length != 5) {
            throw new CronParseException("expected 5 fields (minute hour day-of-month month day-of-week) but got "
                    + (trimmed.isEmpty() ? 0 : fields.length) + " in '" + trimmed + "'");
        }

        long minutes = new CronFieldParser("minute", 0, 59, NO_NAMES).parse(fields[0]);
        long hours = new CronFieldParser("hour", 0, 23, NO_NAMES).parse(fields[1]);
        long months = new CronFieldParser("month", 1, 12, MONTH_NAMES).parse(fields[3]);

        String domText = fields[2].equals("?") ? "*" : fields[2];
        CronFieldParser domParser = new CronFieldParser("day-of-month", 1, 31, NO_NAMES);
        long daysOfMonth = 0;
        long lastDayOffsets = 0;
        for (String element : domParser.splitList(domText)) {
            String upper = element.toUpperCase(Locale.ROOT);
            if (upper.contains("W")) {
                throw domParser.error("'W' (nearest weekday) is not supported");
            } else if (upper.equals("L")) {
                lastDayOffsets |= 1L;
            } else if (upper.startsWith("L-")) {
                int offset = domParser.parseNumber(upper.substring(2), "offset");
                if (offset < 1 || offset > 30) {
                    throw domParser.error("L-n offset must be between 1 and 30");
                }
                lastDayOffsets |= 1L << offset;
            } else {
                daysOfMonth |= domParser.parseElement(element);
            }
        }

        String dowText = fields[4].equals("?") ? "*" : fields[4];
        CronFieldParser dowParser = new CronFieldParser("day-of-week", 0, 6, DAY_NAMES, 7);
        long daysOfWeek = 0;
        long lastWeekdays = 0;
        long[] nthWeekdays = new long[7];
        for (String element : dowParser.splitList(dowText)) {
            String upper = element.toUpperCase(Locale.ROOT);
            int hash = upper.indexOf('#');
            if (hash > 0) {
                int weekday = dowParser.parseValue(upper.substring(0, hash)) % 7;
                int nth = dowParser.parseNumber(upper.substring(hash + 1), "occurrence");
                if (nth < 1 || nth > 5) {
                    throw dowParser.error("occurrence after '#' must be between 1 and 5");
                }
                nthWeekdays[weekday] |= 1L << nth;
            } else if (upper.equals("L")) {
                throw dowParser.error("a bare 'L' is not supported; write the weekday, for example 5L or FRIL");
            } else if (upper.length() > 1 && upper.endsWith("L") && !upper.contains("-") && !upper.contains("/")) {
                lastWeekdays |= 1L << (dowParser.parseValue(upper.substring(0, upper.length() - 1)) % 7);
            } else {
                daysOfWeek |= dowParser.parseElement(element);
            }
        }

        CronExpression parsed = new CronExpression(trimmed, minutes, hours, months, daysOfMonth, lastDayOffsets,
                !domText.equals("*"), daysOfWeek, lastWeekdays, nthWeekdays, !dowText.equals("*"));
        if (parsed.nextLocalMatch(LocalDateTime.of(2000, 1, 1, 0, 0)) == null) {
            throw new CronParseException("expression '" + trimmed + "' never fires");
        }
        return parsed;
    }

    /** Whether the given wall-clock time (seconds ignored) matches all five fields. */
    public boolean matches(LocalDateTime dateTime) {
        return test(minutes, dateTime.getMinute())
                && test(hours, dateTime.getHour())
                && dateMatches(dateTime.toLocalDate());
    }

    /**
     * The first fire time strictly after {@code after}, evaluated on the wall clock of {@code zone}.
     * Empty only if no fire time exists within the next {@value #SEARCH_YEARS} years.
     *
     * <p>The zone's timeline is walked one constant-offset segment at a time. Inside a segment wall-clock
     * time and instants map one to one, so the field arithmetic in {@link #nextLocalMatch} is exact; the
     * DST rules documented on the class are applied only at the segment boundaries.
     */
    public Optional<Instant> next(Instant after, ZoneId zone) {
        ZoneRules rules = zone.getRules();
        Instant searchFrom = after;
        // Each iteration either returns, skips one repeated wall-clock time, or advances one transition.
        for (int guard = 0; guard < 100_000; guard++) {
            ZoneOffset offset = rules.getOffset(searchFrom);
            LocalDateTime candidate = nextLocalMatch(LocalDateTime.ofInstant(searchFrom, offset));
            if (candidate == null) {
                return Optional.empty();
            }
            Instant candidateInstant = candidate.toInstant(offset);
            ZoneOffsetTransition upcoming = rules.nextTransition(searchFrom);

            if (upcoming == null || candidateInstant.isBefore(upcoming.getInstant())) {
                // previousTransition is exclusive, so nudge forward to include a transition at searchFrom.
                ZoneOffsetTransition segmentStart = rules.previousTransition(searchFrom.plusNanos(1));
                boolean secondPassOfOverlap = segmentStart != null && segmentStart.isOverlap()
                        && candidate.isBefore(segmentStart.getDateTimeBefore());
                if (secondPassOfOverlap && !firesEveryHour()) {
                    searchFrom = candidateInstant;
                    continue;
                }
                return Optional.of(candidateInstant);
            }

            // The candidate lies beyond this segment; decide what happens at the transition itself.
            if (upcoming.isGap()) {
                // candidate is a wall-clock time at or after the start of the gap. If it is not later than
                // the first valid time after the gap, it was skipped (or is that first time): fire there.
                if (!candidate.isAfter(upcoming.getDateTimeAfter())) {
                    return Optional.of(upcoming.getInstant());
                }
            } else if (firesEveryHour() && matches(upcoming.getDateTimeAfter())) {
                return Optional.of(upcoming.getInstant());
            }
            searchFrom = upcoming.getInstant();
        }
        throw new IllegalStateException("no progress computing next fire time of '" + source + "' in " + zone);
    }

    /** The next {@code count} fire times strictly after {@code after}; shorter if the schedule runs out. */
    public List<Instant> next(Instant after, ZoneId zone, int count) {
        List<Instant> result = new ArrayList<>(count);
        Instant cursor = after;
        while (result.size() < count) {
            Optional<Instant> next = next(cursor, zone);
            if (next.isEmpty()) {
                break;
            }
            result.add(next.get());
            cursor = next.get();
        }
        return result;
    }

    /**
     * Pure calendar arithmetic: the first matching wall-clock minute strictly after {@code after}, or
     * {@code null} if there is none within the search horizon. Knows nothing about zones.
     */
    LocalDateTime nextLocalMatch(LocalDateTime after) {
        LocalDateTime start = after.truncatedTo(ChronoUnit.MINUTES).plusMinutes(1);
        LocalDate date = start.toLocalDate();
        int hour = start.getHour();
        int minute = start.getMinute();
        int lastYear = date.getYear() + SEARCH_YEARS;

        while (date.getYear() <= lastYear) {
            if (!test(months, date.getMonthValue())) {
                date = date.withDayOfMonth(1).plusMonths(1);
                hour = 0;
                minute = 0;
                continue;
            }
            if (!dayMatches(date)) {
                date = date.plusDays(1);
                hour = 0;
                minute = 0;
                continue;
            }
            int matchedHour = nextSetBit(hours, hour, 23);
            if (matchedHour < 0) {
                date = date.plusDays(1);
                hour = 0;
                minute = 0;
                continue;
            }
            if (matchedHour > hour) {
                minute = 0;
            }
            int matchedMinute = nextSetBit(minutes, minute, 59);
            if (matchedMinute < 0) {
                // No minute left in this hour: retry from the top of the next hour (or the next day).
                hour = matchedHour + 1;
                minute = 0;
                if (hour > 23) {
                    date = date.plusDays(1);
                    hour = 0;
                }
                continue;
            }
            return date.atTime(matchedHour, matchedMinute);
        }
        return null;
    }

    private boolean dateMatches(LocalDate date) {
        return test(months, date.getMonthValue()) && dayMatches(date);
    }

    private boolean dayMatches(LocalDate date) {
        if (dayOfMonthRestricted && dayOfWeekRestricted) {
            return dayOfMonthMatches(date) || dayOfWeekMatches(date);
        }
        if (dayOfMonthRestricted) {
            return dayOfMonthMatches(date);
        }
        if (dayOfWeekRestricted) {
            return dayOfWeekMatches(date);
        }
        return true;
    }

    private boolean dayOfMonthMatches(LocalDate date) {
        int day = date.getDayOfMonth();
        return test(daysOfMonth, day) || test(lastDayOffsets, date.lengthOfMonth() - day);
    }

    private boolean dayOfWeekMatches(LocalDate date) {
        int weekday = date.getDayOfWeek().getValue() % 7; // ISO Monday=1..Sunday=7 -> Sunday=0
        int day = date.getDayOfMonth();
        if (test(daysOfWeek, weekday)) {
            return true;
        }
        if (test(lastWeekdays, weekday) && day + 7 > date.lengthOfMonth()) {
            return true;
        }
        return test(nthWeekdays[weekday], (day - 1) / 7 + 1);
    }

    /**
     * True when the hour field selects all 24 hours. Such schedules are treated as interval-like in a
     * fall-back overlap: they fire in both passes through the repeated hour.
     */
    private boolean firesEveryHour() {
        return hours == ALL_HOURS;
    }

    private static boolean test(long bits, int index) {
        return index >= 0 && index < 64 && (bits >>> index & 1L) != 0;
    }

    private static int nextSetBit(long bits, int from, int max) {
        for (int i = from; i <= max; i++) {
            if ((bits >>> i & 1L) != 0) {
                return i;
            }
        }
        return -1;
    }

    /** The expression exactly as given to {@link #parse}. */
    @Override
    public String toString() {
        return source;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof CronExpression that && source.equals(that.source);
    }

    @Override
    public int hashCode() {
        return source.hashCode();
    }
}
