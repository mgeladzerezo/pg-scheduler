package io.github.mgeladzerezo.pgscheduler.cron;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Parses one comma-separated cron field into a bit set of the values it selects.
 *
 * <p>Grammar of one list element: {@code range[/step]} where {@code range} is {@code *}, a single value
 * {@code a}, or {@code a-b}. A range with {@code a > b} wraps around the end of the field
 * ({@code 22-2} in the hour field is 22, 23, 0, 1, 2). {@code a/step} without an upper bound runs from
 * {@code a} to the field maximum. The step is applied to the expanded sequence, so {@code 22-2/2} selects
 * 22, 0, 2.
 *
 * <p>Day-of-week is the field 0-6 with 7 accepted as a second spelling of Sunday: {@code 7} is 0,
 * {@code 5-7} is Friday, Saturday, Sunday, and {@code 7-2} is Sunday to Tuesday.
 */
final class CronFieldParser {

    private final String fieldName;
    private final int min;
    private final int max;
    private final Map<String, Integer> names;
    /** A value above {@code max} that is another spelling of {@code min} (7 for Sunday), or -1 for none. */
    private final int minAlias;

    CronFieldParser(String fieldName, int min, int max, Map<String, Integer> names) {
        this(fieldName, min, max, names, -1);
    }

    CronFieldParser(String fieldName, int min, int max, Map<String, Integer> names, int minAlias) {
        this.fieldName = fieldName;
        this.min = min;
        this.max = max;
        this.names = names;
        this.minAlias = minAlias;
    }

    /** Bit {@code v} of the result is set when value {@code v} is selected. */
    long parse(String text) {
        long bits = 0;
        for (String element : splitList(text)) {
            bits |= parseElement(element);
        }
        return bits;
    }

    List<String> splitList(String text) {
        if (text.isEmpty() || text.startsWith(",") || text.endsWith(",") || text.contains(",,")) {
            throw error("empty list element in '" + text + "'");
        }
        return List.of(text.split(","));
    }

    long parseElement(String element) {
        String rangePart = element;
        int step = 1;
        boolean hasStep = false;
        int slash = element.indexOf('/');
        if (slash >= 0) {
            rangePart = element.substring(0, slash);
            step = parseNumber(element.substring(slash + 1), "step");
            hasStep = true;
            if (step <= 0) {
                throw error("step must be positive in '" + element + "'");
            }
        }

        List<Integer> sequence = new ArrayList<>();
        if (rangePart.equals("*")) {
            addRange(sequence, min, max);
        } else {
            int dash = rangePart.indexOf('-');
            if (dash > 0) {
                int from = normalise(parseValue(rangePart.substring(0, dash)));
                int rawTo = parseValue(rangePart.substring(dash + 1));
                if (rawTo == minAlias) {
                    // "a-7": run up to Saturday, then include Sunday once at the end.
                    addRange(sequence, from, max);
                    sequence.add(min);
                } else if (from <= rawTo) {
                    addRange(sequence, from, rawTo);
                } else {
                    addRange(sequence, from, max);
                    addRange(sequence, min, rawTo);
                }
            } else {
                int value = normalise(parseValue(rangePart));
                addRange(sequence, value, hasStep ? max : value);
            }
        }

        long bits = 0;
        for (int i = 0; i < sequence.size(); i += step) {
            bits |= 1L << sequence.get(i);
        }
        return bits;
    }

    private static void addRange(List<Integer> sequence, int from, int to) {
        for (int v = from; v <= to; v++) {
            sequence.add(v);
        }
    }

    private int normalise(int value) {
        return value == minAlias ? min : value;
    }

    /**
     * A number or a name such as {@code MON} or {@code JAN}, range-checked against the field. The alias of
     * the minimum (7 in day-of-week) is returned as is so that callers can tell {@code 5-7} from {@code 5-0}.
     */
    int parseValue(String token) {
        Integer named = names.get(token.toUpperCase(Locale.ROOT));
        int value = named != null ? named : parseNumber(token, "value");
        if (value < min || (value > max && value != minAlias)) {
            throw error("value " + token + " out of range " + min + "-" + (minAlias >= 0 ? minAlias : max));
        }
        return value;
    }

    int parseNumber(String token, String what) {
        if (token.isEmpty() || token.length() > 4 || !token.chars().allMatch(Character::isDigit)) {
            throw error("invalid " + what + " '" + token + "'");
        }
        return Integer.parseInt(token);
    }

    CronParseException error(String message) {
        return new CronParseException(fieldName + " field: " + message);
    }
}
