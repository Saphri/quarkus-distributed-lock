package io.quarkiverse.lock.runtime;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;

import org.jboss.logging.Logger;

import io.quarkiverse.lock.DistributedLock;
import io.quarkiverse.lock.LockLostEvent;
import io.quarkiverse.lock.LockLostException;
import io.quarkiverse.lock.LockRegistry;
import io.quarkiverse.lock.LockTimeoutException;
import io.quarkiverse.lock.LockToken;
import io.quarkiverse.lock.LockedExecutionException;
import io.quarkiverse.lock.spi.Capability;
import io.quarkiverse.lock.spi.Lease;
import io.quarkiverse.lock.spi.LockBackend;
import io.quarkus.runtime.BlockingOperationControl;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Context;
import io.vertx.core.Vertx;

/**
 * The generic part of every registry: JVM-local reentrant lock per key, lease renewal and lost-lease handling. The
 * backend wait loop lives in {@link AcquisitionEngine}, shared by the blocking and async paths. The backend only sees
 * one owner per key per JVM.
 */
public class LockRegistryImpl implements LockRegistry {

    private static final Logger LOG = Logger.getLogger(LockRegistryImpl.class);

    static final long MAX_RENEW_RETRY_MILLIS = 1000;

    private final String name;
    private final LockBackend backend;
    private final AcquisitionEngine engine;
    private final Duration defaultWait;
    private final boolean failOnLostLease;
    private final ScheduledExecutorService scheduler;
    private final Executor worker;
    private final Consumer<LockLostEvent> lostListener;
    private final boolean nonBlocking;
    private final ConcurrentHashMap<String, KeyState> states = new ConcurrentHashMap<>();

    public LockRegistryImpl(String name, LockBackend backend, String instanceId, Duration defaultWait,
            boolean failOnLostLease, ScheduledExecutorService scheduler, Executor worker,
            Consumer<LockLostEvent> lostListener) {
        this(name, backend, instanceId, defaultWait, failOnLostLease, scheduler, worker, lostListener, System::nanoTime,
                AcquisitionEngine::randomJitter);
    }

    /** With an injected clock and jitter for the acquisition loop; see {@link AcquisitionEngine}. */
    LockRegistryImpl(String name, LockBackend backend, String instanceId, Duration defaultWait,
            boolean failOnLostLease, ScheduledExecutorService scheduler, Executor worker,
            Consumer<LockLostEvent> lostListener, LongSupplier nanoTime, LongUnaryOperator jitter) {
        this.name = name;
        this.backend = backend;
        this.engine = new AcquisitionEngine(name, backend, instanceId, nanoTime, jitter);
        this.defaultWait = defaultWait;
        this.failOnLostLease = failOnLostLease;
        this.scheduler = scheduler;
        this.worker = worker;
        this.lostListener = lostListener;
        Set<Capability> caps = backend.capabilities();
        this.nonBlocking = caps.contains(Capability.NON_BLOCKING);
    }

    @Override
    public String name() {
        return name;
    }

    public LockBackend backend() {
        return backend;
    }

    @Override
    public Duration defaultWait() {
        return defaultWait;
    }

    @Override
    public int activeKeys() {
        return states.size();
    }

    // ------------------------------------------------------------------ blocking API

    @Override
    public <T> T executeLocked(String key, Callable<T> task) {
        return executeLocked(key, defaultWait, task);
    }

    @Override
    public <T> T executeLocked(String key, Duration wait, Callable<T> task) {
        DistributedLock lock = obtain(key);
        acquireOrThrow(key, wait);
        return runAndRelease(lock, task);
    }

    @Override
    public void runLocked(String key, Runnable task) {
        runLocked(key, defaultWait, task);
    }

    @Override
    public void runLocked(String key, Duration wait, Runnable task) {
        executeLocked(key, wait, () -> {
            task.run();
            return null;
        });
    }

    @Override
    public <T> Optional<T> tryExecuteLocked(String key, Duration wait, Callable<T> task) {
        DistributedLock lock = obtain(key);
        try {
            acquireOrThrow(key, wait);
        } catch (LockTimeoutException e) {
            return Optional.empty();
        }
        return Optional.ofNullable(runAndRelease(lock, task));
    }

    private void acquireOrThrow(String key, Duration wait) {
        try {
            acquire(key, wait, true);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new LockTimeoutException(key, wait, e);
        }
    }

    private <T> T runAndRelease(DistributedLock lock, Callable<T> task) {
        T result;
        try {
            result = task.call();
        } catch (Throwable t) {
            try {
                lock.unlock();
            } catch (LockLostException lost) {
                t.addSuppressed(lost);
            }
            if (t instanceof RuntimeException re) {
                throw re;
            }
            if (t instanceof Error err) {
                throw err;
            }
            throw new LockedExecutionException(t);
        }
        lock.unlock();
        return result;
    }

    @Override
    public DistributedLock obtain(String key) {
        backend.validateKey(key);
        return new LockHandle(key);
    }

    /**
     * Acquires {@code key} for the current thread.
     *
     * @param wait {@code null} for no time limit
     * @throws LockTimeoutException if not acquired in time
     */
    void acquire(String key, Duration wait, boolean interruptible) throws InterruptedException {
        if (!BlockingOperationControl.isBlockingAllowed()) {
            throw new IllegalStateException("Blocking lock acquisition of '" + key
                    + "' is not allowed on an event-loop thread; use executeLockedAsync or a Uni-returning @Locked method");
        }
        Thread me = Thread.currentThread();
        long deadline = engine.deadline(wait);
        KeyState s = ref(key);
        boolean acquired = false;
        try {
            if (!s.enter(me, deadline, engine.clock(), interruptible)) {
                throw new LockTimeoutException(key, wait);
            }
            try {
                if (s.holdCount(me) > 1) {
                    acquired = true;
                    return;
                }
                acquireRemote(s, wait, deadline, interruptible);
                acquired = true;
            } finally {
                if (!acquired) {
                    s.exit(me);
                }
            }
        } finally {
            if (!acquired) {
                unref(s);
            }
        }
    }

    private void acquireRemote(KeyState s, Duration wait, long deadline, boolean interruptible)
            throws InterruptedException {
        AcquisitionEngine.Attempt attempt = engine.attempt(s.key, wait, deadline);
        boolean interrupted = false;
        try {
            while (true) {
                Lease lease = attempt.tryAcquire();
                if (lease != null) {
                    onAcquired(s, lease);
                    return;
                }
                try {
                    attempt.awaitHintBlocking();
                } catch (InterruptedException e) {
                    if (interruptible) {
                        throw e;
                    }
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    void release(String key, Object who) {
        KeyState s = states.get(key);
        if (s == null || !s.isOwner(who)) {
            throw new IllegalMonitorStateException("Lock '" + key + "' is not held by the current thread");
        }
        LockToken lost = null;
        if (s.holdCount(who) == 1) {
            lost = releaseRemote(s);
        }
        s.exit(who);
        unref(s);
        if (lost != null && failOnLostLease) {
            throw new LockLostException(lost);
        }
    }

    // ------------------------------------------------------------------ async API

    @Override
    public <T> Uni<T> executeLockedAsync(String key, Supplier<Uni<T>> task) {
        return executeLockedAsync(key, defaultWait, task);
    }

    @Override
    public <T> Uni<T> executeLockedAsync(String key, Duration wait, Supplier<Uni<T>> task) {
        backend.validateKey(key);
        return Uni.createFrom().deferred(() -> {
            Context context = Vertx.currentContext();
            Executor resume = context == null ? Runnable::run : r -> context.runOnContext(v -> r.run());
            return Uni.createFrom().completionStage(() -> new AsyncAcquisition(key, wait).start())
                    .emitOn(resume)
                    .onItem().transformToUni(acq -> {
                        Uni<T> work;
                        try {
                            work = task.get();
                            if (work == null) {
                                work = Uni.createFrom().failure(new NullPointerException("Locked task returned null Uni"));
                            }
                        } catch (Throwable t) {
                            work = Uni.createFrom().failure(t);
                        }
                        return work
                                .onCancellation().call(acq::releaseAsync)
                                .onItemOrFailure().transformToUni((item, failure) -> acq.releaseAsync()
                                        .onItem().transformToUni(lostToken -> {
                                            if (failure != null) {
                                                return Uni.createFrom().<T> failure(failure);
                                            }
                                            if (lostToken != null && failOnLostLease) {
                                                return Uni.createFrom().<T> failure(new LockLostException(lostToken));
                                            }
                                            return Uni.createFrom().item(item);
                                        }))
                                .emitOn(resume);
                    });
        });
    }

    /**
     * One async acquisition. Owns the local lock itself (instead of a thread), so it can be released from any thread.
     * The returned future is cancelled by Mutiny when the subscriber cancels; whoever loses the race to complete it
     * cleans up.
     */
    private final class AsyncAcquisition {

        private final String key;
        private final Duration wait;
        private final long deadline;
        private final KeyState state;
        private final AcquisitionEngine.Attempt attempt;
        private final CompletableFuture<AsyncAcquisition> result = new CompletableFuture<>();
        private boolean localHeld;
        private volatile boolean released;

        AsyncAcquisition(String key, Duration wait) {
            this.key = key;
            this.wait = wait;
            this.deadline = engine.deadline(wait);
            this.attempt = engine.attempt(key, wait, deadline);
            this.state = ref(key);
        }

        CompletableFuture<AsyncAcquisition> start() {
            tryLocal();
            return result;
        }

        private void tryLocal() {
            if (result.isDone()) {
                abandon();
                return;
            }
            if (state.tryEnter(this)) {
                localHeld = true;
                tryRemote();
                return;
            }
            long remaining = engine.remainingMillis(deadline);
            if (remaining <= 0) {
                fail(new LockTimeoutException(key, wait));
                return;
            }
            state.whenFree(Math.min(remaining, AcquisitionEngine.MAX_BACKOFF_MILLIS)).thenRun(this::tryLocal);
        }

        private void tryRemote() {
            if (nonBlocking) {
                attemptRemote();
            } else {
                try {
                    worker.execute(this::attemptRemote);
                } catch (RuntimeException e) {
                    fail(e);
                }
            }
        }

        private void attemptRemote() {
            if (result.isDone()) {
                abandon();
                return;
            }
            Lease lease;
            try {
                lease = attempt.tryAcquire();
            } catch (LockTimeoutException e) {
                fail(e);
                return;
            }
            if (lease != null) {
                try {
                    onAcquired(state, lease);
                } catch (RuntimeException e) {
                    releaseNow();
                    result.completeExceptionally(e);
                    return;
                }
                if (!result.complete(this)) {
                    // cancelled meanwhile
                    releaseNow();
                }
                return;
            }
            attempt.awaitHint().subscribe().with(v -> tryRemote(), f -> tryRemote());
        }

        private void fail(Throwable t) {
            result.completeExceptionally(t);
            abandon();
        }

        /** Gives up without a remote lease. */
        private void abandon() {
            if (released) {
                return;
            }
            released = true;
            if (localHeld) {
                state.exit(this);
            }
            unref(state);
        }

        /** Releases lease and local lock; returns the lost token if the lease was lost while held. */
        LockToken releaseNow() {
            if (released) {
                return null;
            }
            released = true;
            LockToken lost = releaseRemote(state);
            state.exit(this);
            unref(state);
            return lost;
        }

        Uni<LockToken> releaseAsync() {
            Uni<LockToken> u = Uni.createFrom().item(this::releaseNow);
            return nonBlocking ? u : u.runSubscriptionOn(worker);
        }

        @Override
        public String toString() {
            return "async acquisition of '" + key + "'";
        }
    }

    // ------------------------------------------------------------------ shared internals

    private KeyState ref(String key) {
        return states.compute(key, (k, s) -> {
            KeyState state = s == null ? new KeyState(k) : s;
            state.refs++;
            return state;
        });
    }

    private void unref(KeyState s) {
        states.computeIfPresent(s.key, (k, current) -> {
            if (current != s) {
                return current;
            }
            return --current.refs == 0 ? null : current;
        });
    }

    private void onAcquired(KeyState s, Lease lease) {
        s.remote.lock();
        try {
            s.lease = lease;
            s.lost = false;
            s.token = new LockToken(s.key, lease.ownerId(), lease.fencing(), Instant.now());
            if (lease.expires()) {
                s.leaseMillis = Duration.between(Instant.now(), lease.expiresAt()).toMillis();
                scheduleRenewal(s, lease, renewInterval(lease));
            }
        } finally {
            s.remote.unlock();
        }
    }

    private static long renewInterval(Lease lease) {
        long ttl = Duration.between(Instant.now(), lease.expiresAt()).toMillis();
        return Math.max(ttl / 3, 10);
    }

    private void scheduleRenewal(KeyState s, Lease lease, long delayMillis) {
        if (!lease.expires()) {
            return;
        }
        s.renewal = scheduler.schedule(() -> renew(s, lease), delayMillis, TimeUnit.MILLISECONDS);
    }

    private void renew(KeyState s, Lease expected) {
        LockLostEvent lostEvent = null;
        s.remote.lock();
        try {
            if (s.lease != expected || s.lost) {
                return;
            }
            try {
                Optional<Lease> renewed = backend.renew(expected);
                if (renewed.isPresent()) {
                    Lease r = renewed.get();
                    s.lease = r;
                    s.token = new LockToken(s.key, r.ownerId(), r.fencing(), s.token.acquiredAt());
                    scheduleRenewal(s, r, renewInterval(r));
                } else {
                    lostEvent = markLost(s, null);
                }
            } catch (RuntimeException e) {
                // transient error: keep retrying until shortly before the lease would expire server-side
                long remaining = expected.expiresAt().toEpochMilli() - System.currentTimeMillis();
                long safety = Math.min(MAX_RENEW_RETRY_MILLIS, s.leaseMillis / 10);
                if (remaining <= safety) {
                    lostEvent = markLost(s, e);
                } else {
                    LOG.debugf(e, "Renewal of lock '%s' in registry '%s' failed, retrying", s.key, name);
                    scheduleRenewal(s, expected, Math.max(10, Math.min(MAX_RENEW_RETRY_MILLIS, (remaining - safety) / 2)));
                }
            }
        } catch (Throwable t) {
            LOG.errorf(t, "Unexpected error renewing lock '%s' in registry '%s'", s.key, name);
        } finally {
            s.remote.unlock();
        }
        if (lostEvent != null) {
            try {
                lostListener.accept(lostEvent);
            } catch (Throwable t) {
                LOG.errorf(t, "LockLostEvent observer failed for lock '%s'", s.key);
            }
        }
    }

    private LockLostEvent markLost(KeyState s, Throwable cause) {
        s.lost = true;
        LockToken token = s.token;
        if (cause == null) {
            LOG.warnf("Lease on lock '%s' in registry '%s' (fencing %d) was lost while held: another owner took it",
                    s.key, name, token.fencing());
        } else {
            LOG.warnf("Lease on lock '%s' in registry '%s' (fencing %d) was lost while held: could not renew before"
                    + " expiry (%s)", s.key, name, token.fencing(), cause.toString());
        }
        return new LockLostEvent(name, token, cause);
    }

    /** Cancels renewal and releases the remote lease; returns the token if the lease had been lost. */
    private LockToken releaseRemote(KeyState s) {
        Lease lease;
        boolean wasLost;
        LockToken token;
        s.remote.lock();
        try {
            if (s.renewal != null) {
                s.renewal.cancel(false);
                s.renewal = null;
            }
            lease = s.lease;
            wasLost = s.lost;
            token = s.token;
            s.lease = null;
            s.token = null;
            s.lost = false;
        } finally {
            s.remote.unlock();
        }
        if (wasLost) {
            LOG.warnf("Released lock '%s' in registry '%s' whose lease (fencing %d) had been lost", s.key, name,
                    token.fencing());
            return token;
        }
        if (lease != null) {
            try {
                backend.release(lease);
            } catch (RuntimeException e) {
                LOG.warnf(e, "Could not release lock '%s' in registry '%s'; it will expire", s.key, name);
            }
        }
        return null;
    }

    // ------------------------------------------------------------------ Lock handle

    private final class LockHandle implements DistributedLock {

        private final String key;

        LockHandle(String key) {
            this.key = key;
        }

        @Override
        public String key() {
            return key;
        }

        @Override
        public void lock() {
            try {
                acquire(key, null, false);
            } catch (InterruptedException e) {
                // not interruptible
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            acquire(key, null, true);
        }

        @Override
        public boolean tryLock() {
            return tryLock(Duration.ZERO);
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            try {
                acquire(key, Duration.ofNanos(Math.max(0, unit.toNanos(time))), true);
                return true;
            } catch (LockTimeoutException e) {
                return false;
            }
        }

        @Override
        public boolean tryLock(Duration wait) {
            try {
                acquire(key, wait, false);
                return true;
            } catch (LockTimeoutException e) {
                return false;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        @Override
        public void unlock() {
            release(key, Thread.currentThread());
        }

        @Override
        public Optional<LockToken> token() {
            KeyState s = states.get(key);
            if (s == null || !s.isOwner(Thread.currentThread()) || s.lost) {
                return Optional.empty();
            }
            return Optional.ofNullable(s.token);
        }

        @Override
        public boolean isHeldByCurrentThread() {
            KeyState s = states.get(key);
            return s != null && s.isOwner(Thread.currentThread()) && !s.lost;
        }

        @Override
        public String toString() {
            return "DistributedLock[" + name + "/" + key + "]";
        }
    }
}
