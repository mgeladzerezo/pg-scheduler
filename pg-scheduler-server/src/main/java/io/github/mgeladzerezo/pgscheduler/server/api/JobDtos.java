package io.github.mgeladzerezo.pgscheduler.server.api;

import io.github.mgeladzerezo.pgscheduler.JobState;
import io.github.mgeladzerezo.pgscheduler.admin.SchedulerAdmin.AttemptView;
import io.github.mgeladzerezo.pgscheduler.admin.SchedulerAdmin.JobView;
import io.github.mgeladzerezo.pgscheduler.cron.Schedule;
import java.time.Instant;
import java.util.List;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/** JSON shapes of the API. Stored JSON columns are embedded as JSON, not as escaped strings. */
final class JobDtos {

    private JobDtos() {
    }

    record Job(long id, String type, String queue, JsonNode payload, int priority, JobState state, Instant runAt,
               int attempt, int maxAttempts, long timeoutMs, String uniqueKey, String lockedBy, Instant lockedUntil,
               JsonNode result, String lastError, Long scheduleId, Instant fireTime, Long parentId,
               boolean hasContinuation, Instant createdAt, Instant updatedAt, Instant startedAt, Instant finishedAt) {

        static Job of(JobView v, ObjectMapper json) {
            return new Job(v.id(), v.type(), v.queue(), parse(json, v.payloadJson()), v.priority(), v.state(),
                    v.runAt(), v.attempt(), v.maxAttempts(), v.timeoutMs(), v.uniqueKey(), v.lockedBy(),
                    v.lockedUntil(), parse(json, v.resultJson()), v.lastError(), v.scheduleId(), v.fireTime(),
                    v.parentId(), v.hasContinuation(), v.createdAt(), v.updatedAt(), v.startedAt(), v.finishedAt());
        }
    }

    record JobDetail(Job job, List<AttemptView> attempts) {
    }

    record Page(List<Job> jobs, Long nextBeforeId) {
    }

    record ScheduleDto(long id, String name, String cron, String zone, String jobType, String queue, JsonNode payload,
                       int priority, Integer maxAttempts, Long timeoutMs, String misfirePolicy, boolean paused,
                       Instant nextFireTime, Instant lastFireTime, Instant createdAt, List<FireTime> upcoming) {

        static ScheduleDto of(Schedule s, ObjectMapper json, List<FireTime> upcoming) {
            return new ScheduleDto(s.id(), s.name(), s.cron(), s.zone(), s.jobType(), s.queue(),
                    parse(json, s.payloadJson()), s.priority(), s.maxAttempts(), s.timeoutMs(),
                    s.misfirePolicy().name(), s.paused(), s.nextFireTime(), s.lastFireTime(), s.createdAt(), upcoming);
        }
    }

    /** A fire time as an instant and as the wall-clock time in the schedule's own zone. */
    record FireTime(Instant instant, String local) {
    }

    static JsonNode parse(ObjectMapper json, String text) {
        return text == null ? null : json.readTree(text);
    }
}
