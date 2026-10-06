package io.github.mgeladzerezo.pgscheduler.server.api;

import io.github.mgeladzerezo.pgscheduler.EnqueueResult;
import io.github.mgeladzerezo.pgscheduler.JobRequest;
import io.github.mgeladzerezo.pgscheduler.JobScheduler;
import io.github.mgeladzerezo.pgscheduler.JobState;
import io.github.mgeladzerezo.pgscheduler.admin.SchedulerAdmin;
import io.github.mgeladzerezo.pgscheduler.admin.SchedulerAdmin.JobQuery;
import io.github.mgeladzerezo.pgscheduler.admin.SchedulerAdmin.JobView;
import io.github.mgeladzerezo.pgscheduler.cron.CronExpression;
import io.github.mgeladzerezo.pgscheduler.cron.CronScheduler;
import io.github.mgeladzerezo.pgscheduler.cron.MisfirePolicy;
import io.github.mgeladzerezo.pgscheduler.cron.Schedule;
import io.github.mgeladzerezo.pgscheduler.cron.ScheduleDefinition;
import io.github.mgeladzerezo.pgscheduler.server.api.JobDtos.FireTime;
import io.github.mgeladzerezo.pgscheduler.server.api.JobDtos.Job;
import io.github.mgeladzerezo.pgscheduler.server.api.JobDtos.JobDetail;
import io.github.mgeladzerezo.pgscheduler.server.api.JobDtos.Page;
import io.github.mgeladzerezo.pgscheduler.server.api.JobDtos.ScheduleDto;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/** The dashboard's JSON API: reads, operator actions on jobs and queues, and cron schedule management. */
@RestController
@RequestMapping("/api")
public class DashboardController {

    private static final int PREVIEW_COUNT = 5;

    private final SchedulerAdmin admin;
    private final CronScheduler cron;
    private final JobScheduler scheduler;
    private final OverviewStream stream;
    private final ObjectMapper json;

    public DashboardController(SchedulerAdmin admin, CronScheduler cron, JobScheduler scheduler, OverviewStream stream,
                               ObjectMapper json) {
        this.admin = admin;
        this.cron = cron;
        this.scheduler = scheduler;
        this.stream = stream;
        this.json = json;
    }

    // ---- overview ---------------------------------------------------------------------------------------

    @GetMapping("/overview")
    public OverviewStream.Overview overview() {
        return stream.snapshot();
    }

    /** Server-sent events: an {@code overview} event with the same payload as {@code /api/overview} every second. */
    @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        return stream.subscribe();
    }

    // ---- jobs -------------------------------------------------------------------------------------------

    @GetMapping("/jobs")
    public Page jobs(@RequestParam(required = false) JobState state, @RequestParam(required = false) String queue,
                     @RequestParam(required = false) String type, @RequestParam(required = false) String q,
                     @RequestParam(required = false) Long before, @RequestParam(defaultValue = "50") int limit) {
        int size = Math.max(1, Math.min(limit, 200));
        List<Job> jobs = admin.search(new JobQuery(state, queue, type, q, before, size)).stream()
                .map(v -> Job.of(v, json)).toList();
        return new Page(jobs, jobs.size() == size ? jobs.getLast().id() : null);
    }

    @GetMapping("/jobs/{id}")
    public JobDetail job(@PathVariable long id) {
        JobView view = admin.job(id);
        if (view == null) {
            throw new NoSuchElementException("No job " + id);
        }
        return new JobDetail(Job.of(view, json), admin.attempts(id));
    }

    public record EnqueueRequest(String type, JsonNode payload, String queue, Integer priority, Long delaySeconds,
                                 String uniqueKey, Integer maxAttempts) {
    }

    /** Enqueues a job by hand; the payload is any JSON document. */
    @PostMapping("/jobs")
    @ResponseStatus(HttpStatus.CREATED)
    public EnqueueResult enqueue(@RequestBody EnqueueRequest request) {
        if (request.type() == null || request.type().isBlank()) {
            throw new IllegalArgumentException("type is required");
        }
        JobRequest job = JobRequest.of(request.type().trim(), request.payload());
        if (request.queue() != null && !request.queue().isBlank()) {
            job = job.queue(request.queue().trim());
        }
        if (request.priority() != null) {
            job = job.priority(request.priority());
        }
        if (request.delaySeconds() != null && request.delaySeconds() > 0) {
            job = job.delay(Duration.ofSeconds(request.delaySeconds()));
        }
        if (request.uniqueKey() != null && !request.uniqueKey().isBlank()) {
            job = job.uniqueKey(request.uniqueKey().trim());
        }
        if (request.maxAttempts() != null) {
            job = job.maxAttempts(request.maxAttempts());
        }
        return scheduler.enqueue(job);
    }

    public record RetryRequest(Integer extraAttempts) {
    }

    @PostMapping("/jobs/{id}/retry")
    public JobDetail retry(@PathVariable long id, @RequestBody(required = false) RetryRequest request) {
        admin.retry(id, request == null ? null : request.extraAttempts());
        return job(id);
    }

    @PostMapping("/jobs/{id}/cancel")
    public JobDetail cancel(@PathVariable long id) {
        admin.cancel(id);
        return job(id);
    }

    @PostMapping("/jobs/{id}/run-now")
    public JobDetail runNow(@PathVariable long id) {
        admin.runNow(id);
        return job(id);
    }

    public record RescheduleRequest(Instant runAt) {
    }

    @PutMapping("/jobs/{id}/run-at")
    public JobDetail reschedule(@PathVariable long id, @RequestBody RescheduleRequest request) {
        if (request.runAt() == null) {
            throw new IllegalArgumentException("runAt is required");
        }
        admin.reschedule(id, request.runAt());
        return job(id);
    }

    // ---- queues -----------------------------------------------------------------------------------------

    @PostMapping("/queues/{queue}/pause")
    public Map<String, Object> pause(@PathVariable String queue) {
        admin.pauseQueue(queue);
        return Map.of("queue", queue, "paused", true);
    }

    @PostMapping("/queues/{queue}/resume")
    public Map<String, Object> resume(@PathVariable String queue) {
        admin.resumeQueue(queue);
        return Map.of("queue", queue, "paused", false);
    }

    public record LimitRequest(Integer maxConcurrency) {
    }

    /** Sets or, with {@code null}, removes the cluster-wide concurrency limit of a queue. */
    @PutMapping("/queues/{queue}/concurrency")
    public Map<String, Object> limit(@PathVariable String queue, @RequestBody LimitRequest request) {
        admin.setQueueConcurrencyLimit(queue, request.maxConcurrency());
        return Map.of("queue", queue);
    }

    // ---- schedules --------------------------------------------------------------------------------------

    @GetMapping("/schedules")
    public List<ScheduleDto> schedules() {
        return cron.list().stream().map(this::toDto).toList();
    }

    public record ScheduleRequest(String name, String cron, String zone, String jobType, String queue,
                                  JsonNode payload, Integer priority, MisfirePolicy misfirePolicy) {
    }

    @PostMapping("/schedules")
    @ResponseStatus(HttpStatus.CREATED)
    public ScheduleDto createSchedule(@RequestBody ScheduleRequest request) {
        ScheduleDefinition definition = ScheduleDefinition.of(request.name(), request.cron(), request.jobType())
                .withZone(request.zone() == null || request.zone().isBlank() ? ZoneId.of("UTC") : ZoneId.of(request.zone()))
                .withPayload(request.payload());
        if (request.queue() != null && !request.queue().isBlank()) {
            definition = definition.withQueue(request.queue().trim());
        }
        if (request.priority() != null) {
            definition = definition.withPriority(request.priority());
        }
        if (request.misfirePolicy() != null) {
            definition = definition.withMisfirePolicy(request.misfirePolicy());
        }
        return toDto(cron.create(definition));
    }

    @PostMapping("/schedules/{id}/pause")
    public ScheduleDto pauseSchedule(@PathVariable long id) {
        cron.pause(id);
        return find(id);
    }

    @PostMapping("/schedules/{id}/resume")
    public ScheduleDto resumeSchedule(@PathVariable long id) {
        cron.resume(id);
        return find(id);
    }

    /** Enqueues one job for the schedule now, outside its timetable. */
    @PostMapping("/schedules/{id}/trigger")
    public Map<String, Object> trigger(@PathVariable long id) {
        return Map.of("jobId", cron.triggerNow(id));
    }

    @DeleteMapping("/schedules/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteSchedule(@PathVariable long id) {
        cron.delete(id);
    }

    /** The next fire times of an expression in a zone, for the create form; nothing is stored. */
    @GetMapping("/cron/preview")
    public List<FireTime> preview(@RequestParam String cron, @RequestParam(defaultValue = "UTC") String zone,
                                  @RequestParam(defaultValue = "5") int count) {
        return upcoming(CronExpression.parse(cron), ZoneId.of(zone), Math.max(1, Math.min(count, 20)));
    }

    private ScheduleDto find(long id) {
        return cron.list().stream().filter(s -> s.id() == id).findFirst().map(this::toDto)
                .orElseThrow(() -> new NoSuchElementException("No schedule " + id));
    }

    private ScheduleDto toDto(Schedule s) {
        List<FireTime> upcoming = s.paused() ? List.of()
                : upcoming(CronExpression.parse(s.cron()), ZoneId.of(s.zone()), PREVIEW_COUNT);
        return ScheduleDto.of(s, json, upcoming);
    }

    private static List<FireTime> upcoming(CronExpression expression, ZoneId zone, int count) {
        return expression.next(Instant.now(), zone, count).stream()
                .map(i -> new FireTime(i, DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(i.atZone(zone)) + " " + zone))
                .toList();
    }

    // ---- errors -----------------------------------------------------------------------------------------

    @ExceptionHandler(NoSuchElementException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    Map<String, String> notFound(NoSuchElementException e) {
        return Map.of("error", String.valueOf(e.getMessage()));
    }

    /** The job is in a state that does not allow the action, or another operator got there first. */
    @ExceptionHandler(IllegalStateException.class)
    @ResponseStatus(HttpStatus.CONFLICT)
    Map<String, String> conflict(IllegalStateException e) {
        return Map.of("error", String.valueOf(e.getMessage()));
    }

    @ExceptionHandler({IllegalArgumentException.class, java.time.DateTimeException.class,
            tools.jackson.core.JacksonException.class})
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    Map<String, String> badRequest(RuntimeException e) {
        return Map.of("error", String.valueOf(e.getMessage()));
    }
}
