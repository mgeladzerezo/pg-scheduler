package io.github.mgeladzerezo.pgscheduler.server.demo;

import io.github.mgeladzerezo.pgscheduler.JobContext;
import io.github.mgeladzerezo.pgscheduler.annotation.JobHandler;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Four handlers that make the dashboard worth watching. They are plain annotated methods; the retry
 * behaviour of each is configured in {@code application.yml} under {@code pgscheduler.job-types}.
 */
@Component
public class DemoHandlers {

    private static final Logger log = LoggerFactory.getLogger(DemoHandlers.class);

    public record Email(String to, String subject) {
    }

    public record Slow(Integer seconds) {
    }

    /** Pretends to call an SMTP server. Idempotence in a real handler would key on the job id. */
    @JobHandler("email.send")
    Map<String, Object> sendEmail(Email email, JobContext context) throws InterruptedException {
        Thread.sleep(ThreadLocalRandom.current().nextLong(150, 600));
        log.info("[{}] sent '{}' to {} (job {}, attempt {})", context.workerId(), email.subject(), email.to(),
                context.jobId(), context.attempt());
        return Map.of("delivered", true, "worker", context.workerId());
    }

    /** Fails half of its attempts, so retries with backoff are always visible. */
    @JobHandler("flaky.job")
    String flaky(JobContext context) {
        if (ThreadLocalRandom.current().nextBoolean()) {
            throw new IllegalStateException("simulated upstream failure on attempt " + context.attempt());
        }
        return "succeeded on attempt " + context.attempt();
    }

    /** Runs for a while in one-second steps, long enough to kill the worker it is running on. */
    @JobHandler("slow.job")
    String slow(Slow slow, JobContext context) throws InterruptedException {
        int seconds = slow == null || slow.seconds() == null ? 20 : slow.seconds();
        for (int i = 0; i < seconds; i++) {
            Thread.sleep(1000);
        }
        return "finished after " + seconds + "s on " + context.workerId();
    }

    /** Never succeeds: it ends in the dead-letter state after its last attempt. */
    @JobHandler("always.fail")
    void alwaysFail(JobContext context) {
        throw new IllegalStateException("this job always fails (attempt " + context.attempt() + " of "
                + context.maxAttempts() + ")");
    }
}
