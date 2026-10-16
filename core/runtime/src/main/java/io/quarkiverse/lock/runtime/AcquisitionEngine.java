package io.quarkiverse.lock.runtime;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;

import org.jboss.logging.Logger;

import io.quarkiverse.lock.LockTimeoutException;
import io.quarkiverse.lock.spi.Lease;
import io.quarkiverse.lock.spi.LockBackend;
import io.smallrye.mutiny.Uni;

/**
 * The acquisition algorithm of a registry, shared by its blocking and async paths: deadline math, the backend attempt
 * loop with backoff and jitter, retry on transient backend errors, and the bounded wait for a release hint.
 * <p>
 * The two paths only differ in how they wait between attempts: the blocking path parks the calling thread
 * ({@link Attempt#awaitHintBlocking()}), the async path chains a callback ({@link Attempt#awaitHint()}). Time and
 * jitter are injected, so the loop can be tested without real waiting.
 */
final class AcquisitionEngine {

    private static final Logger LOG = Logger.getLogger(AcquisitionEngine.class);

    static final long MIN_BACKOFF_MILLIS = 100;
    static final long MAX_BACKOFF_MILLIS = 1000;
    /** How long a release hint may overrun the pause it was asked for before the loop retries anyway. */
    static final long HINT_GRACE_MILLIS = 100;

    private final String registry;
    private final LockBackend backend;
    private final String instanceId;
    private final LongSupplier nanoTime;
    private final LongUnaryOperator jitter;

    /**
     * @param nanoTime monotonic clock, as {@link System#nanoTime()}
     * @param jitter randomizes a pause in milliseconds, see {@link #randomJitter(long)}
     */
    AcquisitionEngine(String registry, LockBackend backend, String instanceId, LongSupplier nanoTime,
            LongUnaryOperator jitter) {
        this.registry = registry;
        this.backend = backend;
        this.instanceId = instanceId;
        this.nanoTime = nanoTime;
        this.jitter = jitter;
    }

    /** Spreads a pause over 80–120% so contending owners do not retry in lockstep. */
    static long randomJitter(long millis) {
        return millis * (80 + ThreadLocalRandom.current().nextInt(41)) / 100;
    }

    LongSupplier clock() {
        return nanoTime;
    }

    /**
     * @param wait {@code null} for no time limit
     * @return the deadline on {@link #clock()}, {@link Long#MAX_VALUE} for none
     */
    long deadline(Duration wait) {
        if (wait == null) {
            return Long.MAX_VALUE;
        }
        if (wait.isNegative()) {
            throw new IllegalArgumentException("Wait timeout must not be negative: " + wait);
        }
        long now = nanoTime.getAsLong();
        long nanos;
        try {
            nanos = wait.toNanos();
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
        long d = now + nanos;
        return d < now ? Long.MAX_VALUE : d;
    }

    long remainingMillis(long deadline) {
        return deadline == Long.MAX_VALUE ? Long.MAX_VALUE
                : TimeUnit.NANOSECONDS.toMillis(deadline - nanoTime.getAsLong());
    }

    /** Starts the remote part of one acquisition, once the caller holds the local lock. */
    Attempt attempt(String key, Duration wait, long deadline) {
        return new Attempt(key, wait, deadline);
    }

    /**
     * The backend loop of one acquisition. Not thread-safe: the caller runs one step at a time (sequentially, possibly
     * on different threads).
     */
    final class Attempt {

        private final String key;
        private final Duration wait;
        private final long deadline;
        private long backoff = MIN_BACKOFF_MILLIS;
        private long pause;
        private RuntimeException lastError;

        private Attempt(String key, Duration wait, long deadline) {
            this.key = key;
            this.wait = wait;
            this.deadline = deadline;
        }

        /**
         * One backend attempt. A transient backend error counts as "not acquired" and is retried.
         *
         * @return the lease, or {@code null} if not acquired yet: wait for a hint, then call again
         * @throws LockTimeoutException if not acquired and the deadline has passed
         */
        Lease tryAcquire() {
            Optional<Lease> lease = Optional.empty();
            try {
                lease = backend.tryAcquire(key, instanceId + ":" + UUID.randomUUID());
                lastError = null;
            } catch (RuntimeException e) {
                lastError = e;
                LOG.debugf(e, "Backend error acquiring lock '%s' in registry '%s', retrying", key, registry);
            }
            if (lease.isPresent()) {
                return lease.get();
            }
            long remaining = remainingMillis(deadline);
            if (remaining <= 0) {
                throw new LockTimeoutException(key, wait, lastError);
            }
            pause = Math.min(jitter.applyAsLong(backoff), remaining);
            backoff = Math.min(backoff * 2, MAX_BACKOFF_MILLIS);
            return null;
        }

        /**
         * Completes when the backend hints the key may be free, or after the pause chosen by the last
         * {@link #tryAcquire()} plus grace. Never fails.
         */
        Uni<Void> awaitHint() {
            return backend.awaitReleaseHint(key, Duration.ofMillis(pause))
                    .ifNoItem().after(Duration.ofMillis(pause + HINT_GRACE_MILLIS)).recoverWithItem((Void) null)
                    .onFailure().recoverWithItem((Void) null);
        }

        /** Blocking form of {@link #awaitHint()}. */
        void awaitHintBlocking() throws InterruptedException {
            CompletableFuture<Void> hint = awaitHint().subscribeAsCompletionStage();
            try {
                // bounded here too, so the thread never depends on the Mutiny timer firing
                hint.get(pause + HINT_GRACE_MILLIS, TimeUnit.MILLISECONDS);
            } catch (ExecutionException | TimeoutException e) {
                // just retry
            } finally {
                hint.cancel(false);
            }
        }
    }
}
