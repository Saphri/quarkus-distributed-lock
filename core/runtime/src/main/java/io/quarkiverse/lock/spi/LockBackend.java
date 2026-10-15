package io.quarkiverse.lock.spi;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;

import io.smallrye.mutiny.Uni;

/**
 * A backend holds at most one lease per key across all JVMs. The core puts a JVM-local reentrant lock in front of it,
 * so a backend only ever sees one acquisition attempt per key per registry at a time and never sees reentrant calls.
 * <p>
 * The blocking methods may do I/O; the core calls them on worker threads (unless {@link Capability#NON_BLOCKING}) and
 * never on an event loop. They throw on transient backend errors (connection lost, timeouts); the core retries.
 */
public interface LockBackend {

    /**
     * One non-blocking acquisition attempt.
     *
     * @return the lease, or empty if another owner holds the key
     */
    Optional<Lease> tryAcquire(String key, String ownerId);

    /**
     * Extends a lease.
     *
     * @return the renewed lease (possibly with a new fencing token), or empty if the lease was lost
     */
    Optional<Lease> renew(Lease lease);

    /**
     * Releases a lease, only if it is still the current one (compare-and-delete). Releasing a lost lease is a no-op.
     */
    void release(Lease lease);

    /**
     * Completes when the key may have become free, or after {@code max} at the latest. The default just waits.
     */
    default Uni<Void> awaitReleaseHint(String key, Duration max) {
        return Uni.createFrom().voidItem().onItem().delayIt().by(max);
    }

    /**
     * Fails fast on keys the backend cannot store.
     *
     * @throws IllegalArgumentException if the key is invalid
     */
    default void validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Lock key must not be empty");
        }
    }

    Set<Capability> capabilities();

    /**
     * Called on shutdown. Leases still held are left to expire.
     */
    default void close() {
    }
}
