package io.github.mgeladzerezo.pgscheduler;

/**
 * Handles jobs of one type. Register an implementation as a Spring bean and the auto-configuration picks it
 * up; the payload type {@code T} is read from the generic signature and the stored JSON is deserialised
 * into it before {@link #handle} is called.
 *
 * <pre>{@code
 * @Component
 * class SendEmail implements JobHandler<EmailPayload> {
 *     public String jobType() { return "email.send"; }
 *     public void handle(EmailPayload payload, JobContext context) { ... }
 * }
 * }</pre>
 *
 * <p>The alternative is the method-level annotation
 * {@link io.github.mgeladzerezo.pgscheduler.annotation.JobHandler @JobHandler}.
 *
 * <p><b>Handlers must be idempotent.</b> Delivery is at-least-once: if a worker dies, or loses its lease
 * while the handler is still running, the job runs again elsewhere.
 *
 * @param <T> payload type; may be generic, for example {@code List<OrderLine>}
 */
public interface JobHandler<T> {

    /** The job type this handler serves, for example {@code "email.send"}. */
    String jobType();

    /**
     * Runs one attempt. Returning normally marks the job succeeded; throwing fails the attempt and the
     * job's retry policy decides what happens next. The thread is interrupted if the job times out, the
     * lease is lost, or the worker shuts down, so blocking calls should let {@link InterruptedException}
     * propagate.
     */
    void handle(T payload, JobContext context) throws Exception;
}
