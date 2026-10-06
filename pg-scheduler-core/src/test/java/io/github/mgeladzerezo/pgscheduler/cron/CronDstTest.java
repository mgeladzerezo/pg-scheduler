package io.github.mgeladzerezo.pgscheduler.cron;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Pins down the documented DST behaviour: a wall-clock time lost in a spring-forward gap fires once at the
 * first instant after the gap; a wall-clock time repeated by a fall-back overlap fires once, unless the
 * schedule runs every hour, in which case it keeps its real-time cadence through both passes.
 *
 * <p>Europe/Berlin 2026: clocks jump 02:00 to 03:00 on 29 March and fall back 03:00 to 02:00 on 25 October.
 */
class CronDstTest {

    private static final ZoneId BERLIN = ZoneId.of("Europe/Berlin");
    private static final ZoneId NEW_YORK = ZoneId.of("America/New_York");
    private static final ZoneId LORD_HOWE = ZoneId.of("Australia/Lord_Howe");

    private static Instant at(String zonedDateTime) {
        return ZonedDateTime.parse(zonedDateTime).toInstant();
    }

    private static List<Instant> fires(String cron, ZoneId zone, String after, int count) {
        return CronExpression.parse(cron).next(at(after), zone, count);
    }

    // ---- spring forward ---------------------------------------------------------------------------------

    @Test
    void timeInsideGapFiresOnceAtFirstInstantAfterGap() {
        assertThat(fires("30 2 * * *", BERLIN, "2026-03-28T12:00:00+01:00", 3)).containsExactly(
                at("2026-03-29T03:00:00+02:00"),   // 02:30 does not exist on this day
                at("2026-03-30T02:30:00+02:00"),
                at("2026-03-31T02:30:00+02:00"));
    }

    @Test
    void severalTimesInsideOneGapCollapseIntoOneFire() {
        assertThat(fires("*/15 2 * * *", BERLIN, "2026-03-28T02:40:00+01:00", 3)).containsExactly(
                at("2026-03-28T02:45:00+01:00"),
                at("2026-03-29T03:00:00+02:00"),   // 02:00, 02:15, 02:30, 02:45 collapsed
                at("2026-03-30T02:00:00+02:00"));
    }

    @Test
    void gapFireIsNotDoubledWhenTheFirstInstantAfterTheGapAlsoMatches() {
        assertThat(fires("0,30 2,3 * * *", BERLIN, "2026-03-29T00:00:00+01:00", 3)).containsExactly(
                at("2026-03-29T03:00:00+02:00"),
                at("2026-03-29T03:30:00+02:00"),
                at("2026-03-30T02:00:00+02:00"));
    }

    @Test
    void hourlyAndMinutelySchedulesKeepRealTimeCadenceAcrossGap() {
        assertThat(fires("0 * * * *", BERLIN, "2026-03-29T00:30:00+01:00", 3)).containsExactly(
                at("2026-03-29T01:00:00+01:00"),
                at("2026-03-29T03:00:00+02:00"),
                at("2026-03-29T04:00:00+02:00"));

        List<Instant> everyMinute = fires("* * * * *", BERLIN, "2026-03-29T01:30:00+01:00", 120);
        for (int i = 1; i < everyMinute.size(); i++) {
            assertThat(Duration.between(everyMinute.get(i - 1), everyMinute.get(i))).isEqualTo(Duration.ofMinutes(1));
        }
    }

    // ---- fall back --------------------------------------------------------------------------------------

    @Test
    void fixedTimeInsideOverlapFiresOnlyOnFirstOccurrence() {
        assertThat(fires("30 2 * * *", BERLIN, "2026-10-24T12:00:00+02:00", 2)).containsExactly(
                at("2026-10-25T02:30:00+02:00"),   // first pass (still summer time)
                at("2026-10-26T02:30:00+01:00"));
    }

    @Test
    void searchStartingInSecondPassDoesNotFireTheRepeatedTimeAgain() {
        // 02:10 winter time is the second pass through 02:xx; 02:30 already fired an hour earlier.
        assertThat(fires("30 2 * * *", BERLIN, "2026-10-25T02:10:00+01:00", 1)).containsExactly(
                at("2026-10-26T02:30:00+01:00"));
    }

    @Test
    void everyHourSchedulesFireInBothPassesOfOverlap() {
        assertThat(fires("*/30 * * * *", BERLIN, "2026-10-25T01:45:00+02:00", 5)).containsExactly(
                at("2026-10-25T02:00:00+02:00"),
                at("2026-10-25T02:30:00+02:00"),
                at("2026-10-25T02:00:00+01:00"),
                at("2026-10-25T02:30:00+01:00"),
                at("2026-10-25T03:00:00+01:00"));
    }

    @Test
    void scheduleNamingSpecificHoursSkipsTheSecondPass() {
        assertThat(fires("0 */2 * * *", BERLIN, "2026-10-25T01:00:00+02:00", 2)).containsExactly(
                at("2026-10-25T02:00:00+02:00"),
                at("2026-10-25T04:00:00+01:00"));
        assertThat(fires("* 2 * * *", BERLIN, "2026-10-25T02:58:00+02:00", 2)).containsExactly(
                at("2026-10-25T02:59:00+02:00"),
                at("2026-10-26T02:00:00+01:00"));
    }

    // ---- whole-year invariants and other zones ----------------------------------------------------------

    @Test
    void dailyJobFiresExactlyOncePerCalendarDayAllYear() {
        CronExpression cron = CronExpression.parse("30 2 * * *");
        Instant cursor = at("2026-01-01T00:00:00+01:00");
        Instant end = at("2027-01-01T00:00:00+01:00");
        LocalDate previousDay = null;
        int count = 0;
        while (true) {
            cursor = cron.next(cursor, BERLIN).orElseThrow();
            if (!cursor.isBefore(end)) {
                break;
            }
            LocalDate day = cursor.atZone(BERLIN).toLocalDate();
            assertThat(day).isNotEqualTo(previousDay);
            previousDay = day;
            count++;
        }
        assertThat(count).isEqualTo(365);
    }

    @Test
    void newYorkTransitions() {
        // 8 March 2026: 02:00 EST -> 03:00 EDT. 1 November 2026: 02:00 EDT -> 01:00 EST.
        assertThat(fires("30 2 * * *", NEW_YORK, "2026-03-08T00:00:00-05:00", 1))
                .containsExactly(at("2026-03-08T03:00:00-04:00"));
        assertThat(fires("30 1 * * *", NEW_YORK, "2026-11-01T00:00:00-04:00", 2)).containsExactly(
                at("2026-11-01T01:30:00-04:00"),
                at("2026-11-02T01:30:00-05:00"));
    }

    @Test
    void halfHourTransitionsOnLordHowe() {
        // 5 April 2026: 02:00 (+11:00) -> 01:30 (+10:30), so 01:30-01:59 happens twice.
        assertThat(fires("45 1 * * *", LORD_HOWE, "2026-04-05T00:00:00+11:00", 2)).containsExactly(
                at("2026-04-05T01:45:00+11:00"),
                at("2026-04-06T01:45:00+10:30"));
        // 4 October 2026: 02:00 (+10:30) -> 02:30 (+11:00), so 02:00-02:29 does not exist.
        assertThat(fires("15 2 * * *", LORD_HOWE, "2026-10-04T00:00:00+10:30", 2)).containsExactly(
                at("2026-10-04T02:30:00+11:00"),
                at("2026-10-05T02:15:00+11:00"));
    }

    @Test
    void zoneWithoutDstIsPlainArithmetic() {
        assertThat(fires("0 9 * * *", ZoneId.of("Asia/Tbilisi"), "2026-03-29T00:00:00+04:00", 2)).containsExactly(
                at("2026-03-29T09:00:00+04:00"),
                at("2026-03-30T09:00:00+04:00"));
    }
}
