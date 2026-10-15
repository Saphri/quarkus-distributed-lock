package io.quarkiverse.lock;

/**
 * Wraps a checked exception thrown by a task run under a lock. Unchecked exceptions and errors are rethrown unchanged.
 */
public class LockedExecutionException extends RuntimeException {

    public LockedExecutionException(Throwable cause) {
        super(cause);
    }
}
