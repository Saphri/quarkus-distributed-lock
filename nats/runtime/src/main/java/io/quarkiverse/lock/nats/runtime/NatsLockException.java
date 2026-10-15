package io.quarkiverse.lock.nats.runtime;

/**
 * A NATS error other than "held by someone else": connection problems, timeouts, unexpected API errors. The core
 * treats it as transient and retries.
 */
public class NatsLockException extends RuntimeException {

    public NatsLockException(String operation, String key, Throwable cause) {
        super("NATS lock " + operation + " of '" + key + "' failed: " + cause.getMessage(), cause);
    }

    public NatsLockException(String message, Throwable cause) {
        super(message, cause);
    }
}
