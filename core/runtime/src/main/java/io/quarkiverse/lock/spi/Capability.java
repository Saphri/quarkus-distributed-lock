package io.quarkiverse.lock.spi;

/**
 * Optional features of a {@link LockBackend}.
 */
public enum Capability {
    /** {@link Lease#fencing()} is a real, monotonic fencing token. */
    FENCING,
    /** {@link LockBackend#awaitReleaseHint} completes early when a lock is released. */
    RELEASE_NOTIFICATION,
    /** Lease expiry is enforced by the backend's clock, not the clients'. */
    SERVER_CLOCK,
    /** The blocking methods are cheap and never do I/O, so the core may call them from any thread. */
    NON_BLOCKING
}
