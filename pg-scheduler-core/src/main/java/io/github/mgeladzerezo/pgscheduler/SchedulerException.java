package io.github.mgeladzerezo.pgscheduler;

/** Unchecked wrapper for database and serialisation failures inside the scheduler. */
public class SchedulerException extends RuntimeException {

    public SchedulerException(String message) {
        super(message);
    }

    public SchedulerException(String message, Throwable cause) {
        super(message, cause);
    }
}
