package io.github.mgeladzerezo.pgscheduler.cron;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;

/**
 * Property-style tests: random expressions are checked against a brute-force oracle that walks the timeline
 * one minute at a time.
 *
 * <p>The oracle shares no code with the implementation. The generator builds each expression together with
 * the set of values it means (computed here with naive loops), so the parser, the bit sets and the
 * skip-ahead arithmetic are all under test. The DST rules are restated in {@link #oracle} in their most
 * literal form.
 *
 * <p>Runs are deterministic. Set {@code -Dcron.seed=N} to explore a different part of the space.
 */
class CronPropertyTest {

    private static final long SEED = Long.getLong("cron.seed", 20260329L);

    private static final List<ZoneId> ZONES = List.of(
            ZoneOffset.UTC,
            ZoneId.of("Europe/Berlin"),
            ZoneId.of("America/New_York"),
            ZoneId.of("Australia/Lord_Howe"),   // 30-minute DST shift
            ZoneId.of("America/Sao_Paulo"),     // until 2019 the gap started at midnight
            ZoneId.of("Pacific/Chatham"),       // +12:45 / +13:45
            ZoneId.of("Asia/Kolkata"),          // +05:30, no DST
            ZoneId.of("Asia/Tbilisi"));

    /** An expression plus an independent statement of what it means. */
    private record Spec(String expression, Set<Integer> minutes, Set<Integer> hours, Set<Integer> months,
                        boolean domRestricted, Predicate<LocalDate> dom,
                        boolean dowRestricted, Predicate<LocalDate> dow) {

        boolean matches(LocalDateTime t) {
            if (!minutes.contains(t.getMinute()) || !hours.contains(t.getHour())
                    || !months.contains(t.getMonthValue())) {
                return false;
            }
            LocalDate d = t.toLocalDate();
            if (domRestricted && dowRestricted) {
                return dom.test(d) || dow.test(d);
            }
            return (!domRestricted || dom.test(d)) && (!dowRestricted || dow.test(d));
        }

        boolean everyHour() {
            return hours.size() == 24;
        }
    }

    @Test
    void denseExpressionsMatchOracleOverTenDays() {
        Random random = new Random(SEED);
        long compared = 0;
        for (int i = 0; i < 1_500; i++) {
            Spec spec = randomSpec(random, true);
            ZoneId zone = ZONES.get(random.nextInt(ZONES.size()));
            Instant start = randomInstant(random);
            compared += assertAgreesWithOracle(spec, zone, start, start.plus(10, ChronoUnit.DAYS));
        }
        assertThat(compared).as("fire times compared").isGreaterThan(100_000);
    }

    @Test
    void sparseExpressionsMatchOracleOverFourHundredDays() {
        Random random = new Random(SEED + 1);
        long compared = 0;
        for (int i = 0; i < 120; i++) {
            Spec spec = randomSpec(random, false);
            ZoneId zone = ZONES.get(random.nextInt(ZONES.size()));
            Instant start = randomInstant(random);
            compared += assertAgreesWithOracle(spec, zone, start, start.plus(400, ChronoUnit.DAYS));
        }
        assertThat(compared).as("fire times compared").isGreaterThan(100_000);
    }

    @Test
    void expressionsMatchOracleAroundEveryDstTransition() {
        Random random = new Random(SEED + 2);
        Instant from = Instant.parse("2012-01-01T00:00:00Z");
        Instant to = Instant.parse("2032-01-01T00:00:00Z");
        int windows = 0;
        long compared = 0;
        for (ZoneId zone : ZONES) {
            ZoneRules rules = zone.getRules();
            ZoneOffsetTransition transition = rules.nextTransition(from);
            while (transition != null && transition.getInstant().isBefore(to)) {
                Instant start = transition.getInstant().minus(26, ChronoUnit.HOURS)
                        .plusSeconds(random.nextInt(3600));
                for (int i = 0; i < 12; i++) {
                    compared += assertAgreesWithOracle(randomSpec(random, true), zone, start,
                            start.plus(52, ChronoUnit.HOURS));
                }
                windows++;
                transition = rules.nextTransition(transition.getInstant());
            }
        }
        assertThat(windows).as("DST transitions exercised").isGreaterThan(100);
        assertThat(compared).as("fire times compared").isGreaterThan(50_000);
        System.out.printf("DST windows: %d, fire times compared: %d%n", windows, compared);
    }

    // ---- oracle -----------------------------------------------------------------------------------------

    /** Every fire instant in (start, end], found by testing each minute of real time. */
    private static List<Instant> oracle(Spec spec, ZoneId zone, Instant start, Instant end) {
        ZoneRules rules = zone.getRules();
        Map<Instant, ZoneOffsetTransition> gaps = new HashMap<>();
        for (ZoneOffsetTransition tr = rules.nextTransition(start.minusSeconds(1));
             tr != null && !tr.getInstant().isAfter(end); tr = rules.nextTransition(tr.getInstant())) {
            if (tr.isGap()) {
                gaps.put(tr.getInstant(), tr);
            }
        }
        List<Instant> fires = new ArrayList<>();
        Instant t = start.truncatedTo(ChronoUnit.MINUTES).plus(1, ChronoUnit.MINUTES);
        for (; !t.isAfter(end); t = t.plus(1, ChronoUnit.MINUTES)) {
            ZoneOffset offset = rules.getOffset(t);
            LocalDateTime local = LocalDateTime.ofInstant(t, offset);
            boolean fire = false;

            if (spec.matches(local)) {
                // Rule: the second occurrence of a repeated wall-clock time fires only for every-hour schedules.
                ZoneOffsetTransition around = rules.getTransition(local);
                boolean secondOccurrence = around != null && around.isOverlap()
                        && offset.equals(around.getOffsetAfter());
                fire = !secondOccurrence || spec.everyHour();
            }

            // Rule: wall-clock times skipped by a gap fire once, at the first instant after the gap.
            ZoneOffsetTransition here = gaps.get(t);
            if (here != null) {
                for (LocalDateTime skipped = here.getDateTimeBefore(); skipped.isBefore(here.getDateTimeAfter());
                     skipped = skipped.plusMinutes(1)) {
                    fire |= spec.matches(skipped);
                }
            }
            if (fire) {
                fires.add(t);
            }
        }
        return fires;
    }

    /** Returns the number of fire times compared, so callers can assert the run was not vacuous. */
    private static int assertAgreesWithOracle(Spec spec, ZoneId zone, Instant start, Instant end) {
        CronExpression cron;
        try {
            cron = CronExpression.parse(spec.expression());
        } catch (CronParseException e) {
            // The only legitimate rejection of a generated expression is an impossible date.
            assertThat(e).hasMessageContaining("never fires");
            assertThat(everFires(spec)).as("'%s' rejected as never firing", spec.expression()).isFalse();
            return 0;
        }

        List<Instant> expected = oracle(spec, zone, start, end);
        List<Instant> actual = new ArrayList<>();
        Instant cursor = start;
        while (true) {
            Optional<Instant> next = cron.next(cursor, zone);
            if (next.isEmpty() || next.get().isAfter(end)) {
                break;
            }
            assertThat(next.get()).as("next() must move strictly forward").isAfter(cursor);
            actual.add(next.get());
            cursor = next.get();
        }
        assertThat(actual)
                .as("cron '%s' in %s from %s to %s (seed %d)", spec.expression(), zone, start, end, SEED)
                .isEqualTo(expected);
        return expected.size();
    }

    /** Day-level brute force over a full 400-year Gregorian cycle's worth of leap patterns (2000-2130). */
    private static boolean everFires(Spec spec) {
        for (LocalDate d = LocalDate.of(2000, 1, 1); d.getYear() < 2130; d = d.plusDays(1)) {
            if (spec.matches(d.atTime(spec.hours().iterator().next(), spec.minutes().iterator().next()))) {
                return true;
            }
        }
        return false;
    }

    // ---- generator --------------------------------------------------------------------------------------

    private static Instant randomInstant(Random random) {
        long from = Instant.parse("2015-01-01T00:00:00Z").getEpochSecond();
        long to = Instant.parse("2031-01-01T00:00:00Z").getEpochSecond();
        return Instant.ofEpochSecond(from + (long) (random.nextDouble() * (to - from)));
    }

    private record Field(String text, Set<Integer> values) {
    }

    private static Spec randomSpec(Random random, boolean dense) {
        Field minute = random.nextInt(4) == 0 ? star(0, 59) : numericField(random, 0, 59);
        Field hour = random.nextInt(dense ? 2 : 4) == 0 ? star(0, 23) : numericField(random, 0, 23);
        Field month = random.nextInt(dense ? 4 : 2) == 0 ? numericField(random, 1, 12) : star(1, 12);

        boolean domRestricted = random.nextInt(dense ? 4 : 2) == 0;
        boolean dowRestricted = random.nextInt(dense ? 4 : 2) == 0;
        String domText = "*";
        Predicate<LocalDate> dom = d -> true;
        if (domRestricted) {
            List<String> texts = new ArrayList<>();
            dom = d -> false;
            for (int i = 0, n = 1 + random.nextInt(2); i < n; i++) {
                Predicate<LocalDate> previous = dom;
                Predicate<LocalDate> element;
                int kind = random.nextInt(6);
                if (kind == 0) {
                    texts.add(random.nextBoolean() ? "L" : "l");
                    element = d -> d.getDayOfMonth() == d.lengthOfMonth();
                } else if (kind == 1) {
                    int offset = 1 + random.nextInt(6);
                    texts.add("L-" + offset);
                    element = d -> d.getDayOfMonth() == d.lengthOfMonth() - offset;
                } else {
                    Field f = numericElement(random, 1, 31);
                    texts.add(f.text());
                    element = d -> f.values().contains(d.getDayOfMonth());
                }
                dom = previous.or(element);
            }
            domText = String.join(",", texts);
        } else if (random.nextInt(5) == 0) {
            domText = "?";
        }

        String dowText = "*";
        Predicate<LocalDate> dow = d -> true;
        if (dowRestricted) {
            List<String> texts = new ArrayList<>();
            dow = d -> false;
            String[] names = {"SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"};
            for (int i = 0, n = 1 + random.nextInt(2); i < n; i++) {
                Predicate<LocalDate> previous = dow;
                Predicate<LocalDate> element;
                int kind = random.nextInt(6);
                int weekday = random.nextInt(7);
                String token = random.nextBoolean() ? names[weekday]
                        : (weekday == 0 && random.nextBoolean() ? "7" : String.valueOf(weekday));
                if (kind == 0) {
                    texts.add(token + "L");
                    element = d -> sundayZero(d) == weekday && d.plusDays(7).getMonth() != d.getMonth();
                } else if (kind == 1) {
                    int nth = 1 + random.nextInt(5);
                    texts.add(token + "#" + nth);
                    element = d -> sundayZero(d) == weekday && occurrenceInMonth(d) == nth;
                } else if (kind == 2) {
                    texts.add(token);
                    element = d -> sundayZero(d) == weekday;
                } else {
                    Field f = numericElement(random, 0, 6);
                    texts.add(f.text());
                    element = d -> f.values().contains(sundayZero(d));
                }
                dow = previous.or(element);
            }
            dowText = String.join(",", texts);
        } else if (random.nextInt(5) == 0) {
            dowText = "?";
        }

        String expression = String.join(" ", minute.text(), hour.text(), domText, month.text(), dowText);
        return new Spec(expression, minute.values(), hour.values(), month.values(), domRestricted, dom,
                dowRestricted, dow);
    }

    private static int sundayZero(LocalDate d) {
        return d.getDayOfWeek().getValue() % 7;
    }

    /** 1 for the first Monday (or whichever weekday {@code d} is) of its month, 2 for the second, ... */
    private static int occurrenceInMonth(LocalDate d) {
        int count = 0;
        for (LocalDate x = d; x.getMonth() == d.getMonth(); x = x.minusDays(7)) {
            count++;
        }
        return count;
    }

    private static Field star(int min, int max) {
        Set<Integer> all = new TreeSet<>();
        for (int v = min; v <= max; v++) {
            all.add(v);
        }
        return new Field("*", all);
    }

    /** A list of one to three elements. */
    private static Field numericField(Random random, int min, int max) {
        List<String> texts = new ArrayList<>();
        Set<Integer> values = new TreeSet<>();
        for (int i = 0, n = 1 + random.nextInt(3); i < n; i++) {
            Field element = numericElement(random, min, max);
            texts.add(element.text());
            values.addAll(element.values());
        }
        return new Field(String.join(",", texts), values);
    }

    /** One element: a value, a range (possibly wrapping), or any of those with a step. */
    private static Field numericElement(Random random, int min, int max) {
        int span = max - min + 1;
        int a = min + random.nextInt(span);
        int b = min + random.nextInt(span);
        int step = 1 + random.nextInt(Math.max(1, span / 3));
        List<Integer> sequence = new ArrayList<>();
        String text;
        switch (random.nextInt(6)) {
            case 0 -> {                                   // */step
                text = "*/" + step;
                for (int v = min; v <= max; v++) {
                    sequence.add(v);
                }
            }
            case 1 -> {                                   // a/step: from a to the field maximum
                text = a + "/" + step;
                for (int v = a; v <= max; v++) {
                    sequence.add(v);
                }
            }
            case 2, 3 -> {                                // a-b, wrapping when a > b; optional step
                boolean withStep = random.nextBoolean();
                text = a + "-" + b + (withStep ? "/" + step : "");
                if (!withStep) {
                    step = 1;
                }
                for (int v = a; v != b; v = v == max ? min : v + 1) {
                    sequence.add(v);
                }
                sequence.add(b);
            }
            default -> {                                  // single value
                text = String.valueOf(a);
                step = 1;
                sequence.add(a);
            }
        }
        Set<Integer> values = new TreeSet<>();
        for (int i = 0; i < sequence.size(); i += step) {
            values.add(sequence.get(i));
        }
        return new Field(text, values);
    }
}
