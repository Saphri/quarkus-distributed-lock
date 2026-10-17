package io.quarkiverse.lock.runtime;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.jboss.logging.Logger;

import io.quarkiverse.lock.LockLostEvent;
import io.quarkiverse.lock.LockToken;
import io.quarkiverse.lock.spi.Lease;
import io.quarkiverse.lock.spi.LockBackend;

/**
 * Per-key state of a registry: a JVM-local reentrant lock (owned by a thread, or by an async acquisition) in front of
 * the backend lease, and that lease's lifecycle: renewal, lost-lease detection and release.
 * <p>
 * The local lock is hand-rolled rather than a {@code ReentrantLock} because async acquisitions release it from another
 * thread than the one that acquired it.
 */
final class KeyState {

    // the registry's category, so lease warnings keep the logger they have always had
    private static final Logger LOG = Logger.getLogger(LockRegistryImpl.class);

    static final long MAX_RENEW_RETRY_MILLIS = 1000;

    final String key;
    private final String registry;
    private final LockBackend backend;
    private final ScheduledExecutorService scheduler;
    private final Consumer<LockLostEvent> lostListener;
    private final Supplier<Instant> clock;

    /** Threads and async acquisitions using this state; guarded by the registry map's per-key lock. */
    int refs;

    private final ReentrantLock monitor = new ReentrantLock();
    private final Condition freed = monitor.newCondition();
    private Object owner;
    private int holds;
    private final List<CompletableFuture<Void>> asyncWaiters = new ArrayList<>();

    /** Serializes renewal against release, so a release never races a renewal that would change the revision. */
    private final ReentrantLock remote = new ReentrantLock();
    private volatile Lease lease;
    private volatile LockToken token;
    private volatile boolean lost;
    private volatile long leaseMillis;
    private ScheduledFuture<?> renewal;

    KeyState(String key, String registry, LockBackend backend, ScheduledExecutorService scheduler,
            Consumer<LockLostEvent> lostListener, Supplier<Instant> clock) {
        this.key = key;
        this.registry = registry;
        this.backend = backend;
        this.scheduler = scheduler;
        this.lostListener = lostListener;
        this.clock = clock;
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
     * Waits for the local lock until {@code deadlineNanos} on {@code nanoTime} ({@link Long#MAX_VALUE} = no limit).
     */
    boolean enter(Object who, long deadlineNanos, LongSupplier nanoTime, boolean interruptible)
            throws InterruptedException {
        boolean interrupted = false;
        monitor.lock();
        try {
            while (!enterLocked(who)) {
                long remaining = deadlineNanos == Long.MAX_VALUE ? Long.MAX_VALUE : deadlineNanos - nanoTime.getAsLong();
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

    // ------------------------------------------------------------------ lease lifecycle

    /** Takes over a freshly acquired lease and starts renewing it if it expires. */
    void onAcquired(Lease lease) {
        remote.lock();
        try {
            Instant now = clock.get();
            this.lease = lease;
            this.lost = false;
            this.token = new LockToken(key, lease.ownerId(), lease.fencing(), now);
            if (lease.expires()) {
                leaseMillis = Duration.between(now, lease.expiresAt()).toMillis();
                scheduleRenewal(lease, renewInterval(lease));
            }
        } finally {
            remote.unlock();
        }
    }

    /** The token of the current lease, {@code null} if none is held. */
    LockToken token() {
        return token;
    }

    /** Whether the current lease was lost while held. */
    boolean isLost() {
        return lost;
    }

    /** Cancels renewal and releases the lease; returns the token if the lease had been lost. */
    LockToken release() {
        Lease held;
        boolean wasLost;
        LockToken heldToken;
        remote.lock();
        try {
            if (renewal != null) {
                renewal.cancel(false);
                renewal = null;
            }
            held = lease;
            wasLost = lost;
            heldToken = token;
            lease = null;
            token = null;
            lost = false;
        } finally {
            remote.unlock();
        }
        if (wasLost) {
            LOG.warnf("Released lock '%s' in registry '%s' whose lease (fencing %d) had been lost", key, registry,
                    heldToken.fencing());
            return heldToken;
        }
        if (held != null) {
            try {
                backend.release(held);
            } catch (RuntimeException e) {
                LOG.warnf(e, "Could not release lock '%s' in registry '%s'; it will expire", key, registry);
            }
        }
        return null;
    }

    private long renewInterval(Lease lease) {
        long ttl = Duration.between(clock.get(), lease.expiresAt()).toMillis();
        return Math.max(ttl / 3, 10);
    }

    private void scheduleRenewal(Lease lease, long delayMillis) {
        if (!lease.expires()) {
            return;
        }
        renewal = scheduler.schedule(() -> renew(lease), delayMillis, TimeUnit.MILLISECONDS);
    }

    private void renew(Lease expected) {
        LockLostEvent lostEvent = null;
        remote.lock();
        try {
            if (lease != expected || lost) {
                return;
            }
            try {
                Optional<Lease> renewed = backend.renew(expected);
                if (renewed.isPresent()) {
                    Lease r = renewed.get();
                    lease = r;
                    token = new LockToken(key, r.ownerId(), r.fencing(), token.acquiredAt());
                    scheduleRenewal(r, renewInterval(r));
                } else {
                    lostEvent = markLost(null);
                }
            } catch (RuntimeException e) {
                // transient error: keep retrying until shortly before the lease would expire server-side
                long remaining = Duration.between(clock.get(), expected.expiresAt()).toMillis();
                long safety = Math.min(MAX_RENEW_RETRY_MILLIS, leaseMillis / 10);
                if (remaining <= safety) {
                    lostEvent = markLost(e);
                } else {
                    LOG.debugf(e, "Renewal of lock '%s' in registry '%s' failed, retrying", key, registry);
                    scheduleRenewal(expected, Math.max(10, Math.min(MAX_RENEW_RETRY_MILLIS, (remaining - safety) / 2)));
                }
            }
        } catch (Throwable t) {
            LOG.errorf(t, "Unexpected error renewing lock '%s' in registry '%s'", key, registry);
        } finally {
            remote.unlock();
        }
        if (lostEvent != null) {
            try {
                lostListener.accept(lostEvent);
            } catch (Throwable t) {
                LOG.errorf(t, "LockLostEvent observer failed for lock '%s'", key);
            }
        }
    }

    private LockLostEvent markLost(Throwable cause) {
        lost = true;
        LockToken lostToken = token;
        if (cause == null) {
            LOG.warnf("Lease on lock '%s' in registry '%s' (fencing %d) was lost while held: another owner took it",
                    key, registry, lostToken.fencing());
        } else {
            LOG.warnf("Lease on lock '%s' in registry '%s' (fencing %d) was lost while held: could not renew before"
                    + " expiry (%s)", key, registry, lostToken.fencing(), cause.toString());
        }
        return new LockLostEvent(registry, lostToken, cause);
    }
}
