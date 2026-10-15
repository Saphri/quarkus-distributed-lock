package io.quarkiverse.lock.runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import io.quarkiverse.lock.LockToken;
import io.quarkiverse.lock.spi.Lease;

/**
 * Per-key state of a registry: a JVM-local reentrant lock (owned by a thread, or by an async acquisition) in front of
 * the backend lease.
 * <p>
 * The local lock is hand-rolled rather than a {@code ReentrantLock} because async acquisitions release it from another
 * thread than the one that acquired it.
 */
final class KeyState {

    final String key;

    /** Threads and async acquisitions using this state; guarded by the registry map's per-key lock. */
    int refs;

    private final ReentrantLock monitor = new ReentrantLock();
    private final Condition freed = monitor.newCondition();
    private Object owner;
    private int holds;
    private final List<CompletableFuture<Void>> asyncWaiters = new ArrayList<>();

    /** Serializes renewal against release, so a release never races a renewal that would change the revision. */
    final ReentrantLock remote = new ReentrantLock();
    volatile Lease lease;
    volatile LockToken token;
    volatile boolean lost;
    volatile long leaseMillis;
    ScheduledFuture<?> renewal;

    KeyState(String key) {
        this.key = key;
    }

    boolean tryEnter(Object who) {
        monitor.lock();
        try {
            return enterLocked(who);
        } finally {
            monitor.unlock();
        }
    }

    /**
     * Waits for the local lock until {@code deadlineNanos} ({@link Long#MAX_VALUE} = no limit).
     */
    boolean enter(Object who, long deadlineNanos, boolean interruptible) throws InterruptedException {
        boolean interrupted = false;
        monitor.lock();
        try {
            while (!enterLocked(who)) {
                long remaining = deadlineNanos == Long.MAX_VALUE ? Long.MAX_VALUE : deadlineNanos - System.nanoTime();
                if (remaining <= 0) {
                    return false;
                }
                try {
                    if (remaining == Long.MAX_VALUE) {
                        freed.await();
                    } else {
                        freed.awaitNanos(remaining);
                    }
                } catch (InterruptedException e) {
                    if (interruptible) {
                        throw e;
                    }
                    interrupted = true;
                }
            }
            return true;
        } finally {
            monitor.unlock();
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private boolean enterLocked(Object who) {
        if (owner == null) {
            owner = who;
            holds = 1;
            return true;
        }
        if (owner == who) {
            holds++;
            return true;
        }
        return false;
    }

    /**
     * @return the remaining hold count of {@code who}
     * @throws IllegalMonitorStateException if {@code who} does not hold the lock
     */
    int exit(Object who) {
        List<CompletableFuture<Void>> wake = null;
        monitor.lock();
        try {
            if (owner != who) {
                throw new IllegalMonitorStateException("Lock '" + key + "' is not held by " + who);
            }
            if (--holds > 0) {
                return holds;
            }
            owner = null;
            freed.signal();
            if (!asyncWaiters.isEmpty()) {
                wake = new ArrayList<>(asyncWaiters);
                asyncWaiters.clear();
            }
        } finally {
            monitor.unlock();
        }
        if (wake != null) {
            wake.forEach(f -> f.complete(null));
        }
        return 0;
    }

    /**
     * A future completing when the local lock is free, or after {@code maxMillis}.
     */
    CompletableFuture<Void> whenFree(long maxMillis) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        monitor.lock();
        try {
            if (owner == null) {
                f.complete(null);
                return f;
            }
            asyncWaiters.removeIf(CompletableFuture::isDone);
            asyncWaiters.add(f);
        } finally {
            monitor.unlock();
        }
        return f.completeOnTimeout(null, maxMillis, TimeUnit.MILLISECONDS);
    }

    int holdCount(Object who) {
        monitor.lock();
        try {
            return owner == who ? holds : 0;
        } finally {
            monitor.unlock();
        }
    }

    boolean isOwner(Object who) {
        monitor.lock();
        try {
            return owner == who;
        } finally {
            monitor.unlock();
        }
    }
}
