package io.github.mgeladzerezo.pgscheduler.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method of a Spring bean as the handler for one job type.
 *
 * <pre>{@code
 * @JobHandler("email.send")
 * void send(EmailPayload payload, JobContext context) { ... }
 * }</pre>
 *
 * <p>The method takes the payload as its first parameter (any type Jackson can bind, including generic
 * types such as {@code List<OrderLine>}), optionally followed by a
 * {@link io.github.mgeladzerezo.pgscheduler.JobContext}. A method with only a {@code JobContext}
 * parameter, or none, ignores the payload. A non-void return value is stored as the job's result.
 *
 * <p>The attributes override the scheduler defaults for this job type; configuration properties under
 * {@code pgscheduler.job-types.<type>} override the attributes.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface JobHandler {

    /** The job type, for example {@code "email.send"}. */
    String value();

    /** Attempts before the job is dead-lettered; 0 keeps the default. */
    int maxAttempts() default 0;

    /** Handler timeout as an ISO-8601 or Spring-style duration ({@code "30s"}, {@code "PT5M"}); empty keeps the default. */
    String timeout() default "";

    /** Exceptions (and subclasses) that send the job straight to the dead-letter state. */
    Class<? extends Throwable>[] noRetryFor() default {};

    /** Cluster-wide rate limit for this job type in jobs per second; 0 means unlimited. */
    double rateLimitPerSecond() default 0;
}
