package io.quarkiverse.lock;

import java.time.Duration;

/**
 * Thrown when a lock could not be acquired within the wait timeout.
 */
public class LockTimeoutException extends RuntimeException {

    private final String key;

    public LockTimeoutException(String key, Duration wait) {
        this(key, wait, null);
    }

    public LockTimeoutException(String key, Duration wait, Throwable lastBackendError) {
        super("Could not acquire lock '" + key + "' within " + wait
                + (lastBackendError != null ? " (last backend error: " + lastBackendError + ")" : ""), lastBackendError);
        this.key = key;
    }

    public String key() {
        return key;
    }
}
