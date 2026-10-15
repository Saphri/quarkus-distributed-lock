package io.quarkiverse.lock;

/**
 * What {@link Locked} does when the lock cannot be acquired within the wait timeout.
 */
public enum OnTimeout {
    /** Throw {@link LockTimeoutException} (or fail the returned {@code Uni}/{@code CompletionStage} with it). */
    THROW,
    /**
     * Do not run the method and return normally. Only allowed on methods returning {@code void},
     * {@code Uni<Void>} or {@code CompletionStage<Void>}.
     */
    SKIP
}
