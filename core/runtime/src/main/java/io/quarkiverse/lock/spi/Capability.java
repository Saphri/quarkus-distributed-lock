package io.quarkiverse.lock.spi;

/**
 * Optional features of a {@link LockBackend} that change how the core drives it.
 */
public enum Capability {
    /**
     * The blocking methods are cheap and never do I/O. The core then calls them on the current thread, and runs async
     * acquisitions and releases on the subscribing thread; without it, those calls are dispatched to a worker thread.
     */
    NON_BLOCKING
}
