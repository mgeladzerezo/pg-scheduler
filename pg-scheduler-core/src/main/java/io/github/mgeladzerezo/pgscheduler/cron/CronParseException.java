package io.github.mgeladzerezo.pgscheduler.cron;

/** Thrown when a cron expression is syntactically invalid, uses an unsupported feature or can never fire. */
public class CronParseException extends IllegalArgumentException {

    public CronParseException(String message) {
        super(message);
    }
}
