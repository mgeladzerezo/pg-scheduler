package io.github.mgeladzerezo.pgscheduler.cron;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Example-based tests of the syntax and of next-fire-time arithmetic in UTC (no DST involved). */
class CronExpressionTest {

    private static final ZoneId UTC = ZoneOffset.UTC;

    private static Instant utc(String localDateTime) {
        return LocalDateTime.parse(localDateTime).toInstant(ZoneOffset.UTC);
    }

    private static Instant nextUtc(String cron, String after) {
        return CronExpression.parse(cron).next(utc(after), UTC).orElseThrow();
    }

    @ParameterizedTest(name = "[{0}] after {1} -> {2}")
    @CsvSource(delimiter = '|', value = {
            "* * * * *            | 2026-01-01T00:00:00 | 2026-01-01T00:01:00",
            "* * * * *            | 2026-01-01T00:00:59 | 2026-01-01T00:01:00",
            "*/15 * * * *         | 2026-01-01T00:46:00 | 2026-01-01T01:00:00",
            "5/20 * * * *         | 2026-01-01T00:05:00 | 2026-01-01T00:25:00",
            "10-40/10 3 * * *     | 2026-01-01T03:30:00 | 2026-01-01T03:40:00",
            "0 9-17 * * MON-FRI   | 2026-01-02T17:00:00 | 2026-01-05T09:00:00",
            "0 22-2 * * *         | 2026-01-01T23:00:00 | 2026-01-02T00:00:00",
            "0 22-2 * * *         | 2026-01-02T02:00:00 | 2026-01-02T22:00:00",
            "0 0 * * fri-mon      | 2026-01-05T00:00:00 | 2026-01-09T00:00:00",
            "0 0 1 jan *          | 2026-01-01T00:00:00 | 2027-01-01T00:00:00",
            "0 0 * * 7            | 2026-01-01T00:00:00 | 2026-01-04T00:00:00",
            "0 0 * * 0            | 2026-01-01T00:00:00 | 2026-01-04T00:00:00",
            "30 4 1,15 * *        | 2026-01-15T04:30:00 | 2026-02-01T04:30:00",
            "0 0 29 2 *           | 2026-01-01T00:00:00 | 2028-02-29T00:00:00",
            "0 0 29 2 *           | 2096-03-01T00:00:00 | 2104-02-29T00:00:00",
            "0 0 31 * *           | 2026-01-31T00:00:00 | 2026-03-31T00:00:00",
            "59 23 31 12 *        | 2026-12-31T23:59:00 | 2027-12-31T23:59:00",
            "@hourly              | 2026-06-01T10:20:00 | 2026-06-01T11:00:00",
            "@daily               | 2026-06-01T10:20:00 | 2026-06-02T00:00:00",
            "@weekly              | 2026-06-01T10:20:00 | 2026-06-07T00:00:00",
            "@monthly             | 2026-06-01T10:20:00 | 2026-07-01T00:00:00",
            "@yearly              | 2026-06-01T10:20:00 | 2027-01-01T00:00:00",
    })
    void nextFireTime(String cron, String after, String expected) {
        assertThat(nextUtc(cron.trim(), after.trim())).isEqualTo(utc(expected.trim()));
    }

    @ParameterizedTest(name = "[{0}] after {1} -> {2}")
    @CsvSource(delimiter = '|', value = {
            // L: last day of the month, including February in leap and non-leap years
            "0 0 L * *      | 2026-01-31T00:00:00 | 2026-02-28T00:00:00",
            "0 0 L * *      | 2028-02-01T00:00:00 | 2028-02-29T00:00:00",
            "0 0 L-2 * *    | 2026-04-01T00:00:00 | 2026-04-28T00:00:00",
            "0 0 L-30 * *   | 2026-01-01T00:00:00 | 2026-03-01T00:00:00",
            // 5L: last Friday of the month
            "0 0 * * 5L     | 2026-01-01T00:00:00 | 2026-01-30T00:00:00",
            "0 0 * * FRIL   | 2026-01-30T00:00:00 | 2026-02-27T00:00:00",
            // MON#2: second Monday; #5 only exists in some months
            "0 0 * * MON#2  | 2026-01-01T00:00:00 | 2026-01-12T00:00:00",
            "0 0 * * 1#5    | 2026-01-01T00:00:00 | 2026-03-30T00:00:00",
            "0 0 * 2 MON#5  | 2026-01-01T00:00:00 | 2044-02-29T00:00:00",
    })
    void lastAndNthForms(String cron, String after, String expected) {
        assertThat(nextUtc(cron.trim(), after.trim())).isEqualTo(utc(expected.trim()));
    }

    @Test
    void restrictedDayOfMonthAndDayOfWeekAreOredLikeVixieCron() {
        // 13th of the month OR any Friday.
        CronExpression cron = CronExpression.parse("0 0 13 * FRI");
        assertThat(cron.next(utc("2026-01-01T00:00:00"), UTC, 4)).containsExactly(
                utc("2026-01-02T00:00:00"), utc("2026-01-09T00:00:00"),
                utc("2026-01-13T00:00:00"), utc("2026-01-16T00:00:00"));
    }

    @Test
    void sevenIsSundayAlsoInsideRanges() {
        // 1 January 2026 is a Thursday.
        assertThat(CronExpression.parse("0 0 * * 5-7").next(utc("2026-01-01T00:00:00"), UTC, 4)).containsExactly(
                utc("2026-01-02T00:00:00"), utc("2026-01-03T00:00:00"),
                utc("2026-01-04T00:00:00"), utc("2026-01-09T00:00:00"));
        assertThat(CronExpression.parse("0 0 * * 7-2").next(utc("2026-01-01T00:00:00"), UTC, 4)).containsExactly(
                utc("2026-01-04T00:00:00"), utc("2026-01-05T00:00:00"),
                utc("2026-01-06T00:00:00"), utc("2026-01-11T00:00:00"));
        // A wrapping range with a step walks Fri, Sat, Sun, Mon, Tue and keeps every second day.
        assertThat(CronExpression.parse("0 0 * * 5-2/2").next(utc("2026-01-01T00:00:00"), UTC, 4)).containsExactly(
                utc("2026-01-02T00:00:00"), utc("2026-01-04T00:00:00"),
                utc("2026-01-06T00:00:00"), utc("2026-01-09T00:00:00"));
    }

    @Test
    void questionMarkCountsAsUnrestricted() {
        CronExpression cron = CronExpression.parse("0 0 13 * ?");
        assertThat(cron.next(utc("2026-01-01T00:00:00"), UTC, 2)).containsExactly(
                utc("2026-01-13T00:00:00"), utc("2026-02-13T00:00:00"));
    }

    @Test
    void stepInDayFieldStillCountsAsRestricted() {
        // "*/2" restricts day-of-month, so the OR rule with day-of-week applies (odd days OR Mondays).
        CronExpression cron = CronExpression.parse("0 0 */2 * MON");
        assertThat(cron.matches(LocalDateTime.parse("2026-01-12T00:00:00"))).isTrue();  // Monday the 12th
        assertThat(cron.matches(LocalDateTime.parse("2026-01-13T00:00:00"))).isTrue();  // odd day
        assertThat(cron.matches(LocalDateTime.parse("2026-01-14T00:00:00"))).isFalse(); // even, Wednesday
    }

    @Test
    void matchesIgnoresSeconds() {
        CronExpression cron = CronExpression.parse("30 12 * * *");
        assertThat(cron.matches(LocalDateTime.parse("2026-05-05T12:30:45"))).isTrue();
        assertThat(cron.matches(LocalDateTime.parse("2026-05-05T12:31:00"))).isFalse();
    }

    @Test
    void nextReturnsStrictlyIncreasingTimes() {
        List<Instant> times = CronExpression.parse("*/7 */5 * * *").next(utc("2026-01-01T00:00:00"), UTC, 500);
        assertThat(times).hasSize(500).isSorted().doesNotHaveDuplicates();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "", "* * * *", "* * * * * *", "0 0 * * * 2026",
            "60 * * * *", "* 24 * * *", "* * 0 * *", "* * 32 * *", "* * * 13 * ", "* * * * 8",
            "*/0 * * * *", "1,,2 * * * *", ",1 * * * *", "a * * * *", "1-x * * * *", "-1 * * * *",
            "* * 15W * *", "* * LW * *", "* * L-31 * *", "* * * * L", "* * * * MON#6", "* * * * MON#0",
            "* * * FOO *", "* * * * FUNDAY", "@reboot", "@every5m",
            "0 0 31 2 *", "0 0 30 2 *", "0 0 31 4,6,9,11 *"
    })
    void rejectsInvalidUnsupportedOrImpossibleExpressions(String cron) {
        assertThatThrownBy(() -> CronExpression.parse(cron)).isInstanceOf(CronParseException.class);
    }

    @Test
    void errorMessagesNameTheOffendingField() {
        assertThatThrownBy(() -> CronExpression.parse("* 25 * * *"))
                .hasMessageContaining("hour").hasMessageContaining("25");
        assertThatThrownBy(() -> CronExpression.parse("* * 15W * *"))
                .hasMessageContaining("day-of-month").hasMessageContaining("not supported");
        assertThatThrownBy(() -> CronExpression.parse("0 0 31 2 *"))
                .hasMessageContaining("never fires");
    }
}
