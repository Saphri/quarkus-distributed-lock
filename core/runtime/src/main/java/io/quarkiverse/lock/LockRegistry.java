package io.quarkiverse.lock;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import io.smallrye.mutiny.Uni;

/**
 * Entry point for distributed locks. Every method follows the same rule: <em>enter = acquire or wait, exit (return,
 * throw or, for {@link Uni}, termination) = release</em>.
 * <p>
 * Inject the default registry with {@code @Inject LockRegistry} and a named one with
 * {@code @Inject @LockRegistryName("jobs") LockRegistry}.
 * <p>
 * Locks are reentrant per thread. The blocking methods must not be called on an event-loop thread; use
 * {@link #executeLockedAsync(String, Supplier)} there.
 */
public interface LockRegistry {

    /**
     * The registry name; {@link LockRegistryName#DEFAULT} for the default registry.
     */
    String name();

    /**
     * The wait timeout used by the methods that take no explicit wait, and by {@code @Locked} methods that declare no
     * {@code waitTimeout}. {@link Duration#ZERO} means a single attempt.
     */
    Duration defaultWait();

    /**
     * Number of keys with live local state in this registry: held, or being waited for. Zero once every lock has been
     * released and no one is waiting, which makes it useful for leak assertions.
     */
    int activeKeys();

    /**
     * Runs {@code task} while holding the lock {@code key}, waiting at most the configured default wait timeout.
     *
     * @throws LockTimeoutException if the lock could not be acquired in time
     * @throws LockedExecutionException wrapping a checked exception thrown by {@code task}
     */
    <T> T executeLocked(String key, Callable<T> task);

    /**
     * Runs {@code task} while holding the lock {@code key}, waiting at most {@code wait}. {@link Duration#ZERO} means a
     * single attempt.
     *
     * @throws LockTimeoutException if the lock could not be acquired in time
     * @throws LockedExecutionException wrapping a checked exception thrown by {@code task}
     */
    <T> T executeLocked(String key, Duration wait, Callable<T> task);

    /**
     * {@link Runnable} variant of {@link #executeLocked(String, Callable)}.
     */
    void runLocked(String key, Runnable task);

    /**
     * {@link Runnable} variant of {@link #executeLocked(String, Duration, Callable)}.
     */
    void runLocked(String key, Duration wait, Runnable task);

    /**
     * Like {@link #executeLocked(String, Duration, Callable)} but returns {@link Optional#empty()} instead of throwing
     * when the lock could not be acquired in time. A task returning {@code null} also yields an empty optional.
     */
    <T> Optional<T> tryExecuteLocked(String key, Duration wait, Callable<T> task);

    /**
     * Subscribes to the {@link Uni} returned by {@code task} while holding the lock {@code key}, waiting at most the
     * configured default wait timeout. The lock is released when that {@code Uni} terminates with an item, a failure or
     * a cancellation. Never blocks the calling thread.
     * <p>
     * Async acquisitions are not reentrant: a task that acquires the same key again waits for itself until the
     * timeout.
     */
    <T> Uni<T> executeLockedAsync(String key, Supplier<Uni<T>> task);

    /**
     * Like {@link #executeLockedAsync(String, Supplier)} with an explicit wait timeout.
     */
    <T> Uni<T> executeLockedAsync(String key, Duration wait, Supplier<Uni<T>> task);

    /**
     * Low-level escape hatch: a {@link java.util.concurrent.locks.Lock} for {@code key}. Handles are cheap and may be
     * obtained for every use; all handles for the same key in one registry share the same underlying lock.
     *
     * @throws IllegalArgumentException if the key is not valid for the registry's backend
     */
    DistributedLock obtain(String key);
}
