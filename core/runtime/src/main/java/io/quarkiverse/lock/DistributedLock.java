package io.quarkiverse.lock;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * A {@link Lock} backed by a lease in the registry's backend, reentrant per thread.
 * <p>
 * {@link #lock()} waits without a time limit, as the {@link Lock} contract requires; prefer {@link #tryLock(Duration)} or
 * the {@link LockRegistry} helpers. {@link #newCondition()} throws {@link UnsupportedOperationException}.
 */
public interface DistributedLock extends Lock {

    /**
     * The lock key.
     */
    String key();

    /**
     * Acquires the lock if it becomes available within {@code wait}. {@link Duration#ZERO} means a single attempt.
     *
     * @return {@code true} if the lock was acquired
     * @throws IllegalStateException if called on an event-loop thread
     */
    boolean tryLock(Duration wait);

    /**
     * The fencing token of the current lease, if the current thread holds the lock and the lease has not been lost.
     * Renewals produce a new, higher token.
     */
    Optional<LockToken> token();

    /**
     * {@code true} if the current thread holds the lock and its lease has not been lost.
     */
    boolean isHeldByCurrentThread();

    @Override
    default Condition newCondition() {
        throw new UnsupportedOperationException("Conditions are not supported by distributed locks");
    }
}
