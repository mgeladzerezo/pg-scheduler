package io.github.mgeladzerezo.pgscheduler;

import java.time.Duration;

/** Recorded as the failure of an attempt whose handler ran longer than the job's timeout. Retryable. */
public class JobTimeoutException extends RuntimeException {

    public JobTimeoutException(long jobId, Duration timeout) {
        super("Job " + jobId + " exceeded its timeout of " + timeout + " and was interrupted");
    }
}
