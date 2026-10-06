package io.github.mgeladzerezo.pgscheduler;

/**
 * Thrown by a handler to say that retrying cannot help (a malformed payload, a business rule violation).
 * The job goes straight to the dead-letter state regardless of the attempts it has left.
 */
public class NonRetryableJobException extends RuntimeException {

    public NonRetryableJobException(String message) {
        super(message);
    }

    public NonRetryableJobException(String message, Throwable cause) {
        super(message, cause);
    }
}
