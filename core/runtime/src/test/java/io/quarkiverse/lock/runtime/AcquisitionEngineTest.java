package io.quarkiverse.lock.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongUnaryOperator;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.quarkiverse.lock.DistributedLock;
import io.quarkiverse.lock.LockLostEvent;
import io.quarkiverse.lock.LockLostException;
import io.quarkiverse.lock.LockRegistry;
import io.quarkiverse.lock.LockTimeoutException;
import io.quarkiverse.lock.LockToken;
import io.quarkiverse.lock.spi.Capability;
import io.quarkiverse.lock.spi.Lease;
import io.quarkiverse.lock.spi.LockBackend;
import io.quarkus.runtime.BlockingOperationControl;
import io.quarkus.runtime.IOThreadDetector;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;

/**
 * The acquisition loop and the lease lifecycle through the {@link LockRegistry} interface, against a scripted backend
 * and fake clocks: no Quarkus boot, and no real waiting beyond the renewals of a short lease, which run on a real
 * scheduler.
 */
class AcquisitionEngineTest {

    private static final Duration LONG_WAIT = Duration.ofMinutes(1);

    private final AtomicLong clock = new AtomicLong();
    private final FakeBackend backend = new FakeBackend();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final Deque<Runnable> workerQueue = new ArrayDeque<>();
    /** Wall clock for lease expiry; independent of {@link #clock}, as in production. */
    private final AtomicReference<Instant> wallClock = new AtomicReference<>(Instant.EPOCH);
    private final CompletableFuture<LockLostEvent> lostEvent = new CompletableFuture<>();
    private Consumer<LockLostEvent> lostListener = lostEvent::complete;
    private final CapturedLog log = new CapturedLog();

    @BeforeAll
    static void noQuarkus() {
        // Quarkus installs its event-loop detectors on boot; without one, every thread may block
        BlockingOperationControl.setIoThreadDetector(new IOThreadDetector[0]);
    }

    @AfterEach
    void shutdown() {
        scheduler.shutdownNow();
        log.close();
    }

    // ------------------------------------------------------------------ backoff

    @Test
    void blockingBackoffDoublesUpToTheMaximum() {
        backend.taken(7);
        assertThat(registry().executeLocked("k", LONG_WAIT, () -> "done")).isEqualTo("done");
        assertThat(backend.pauses).containsExactly(100L, 200L, 400L, 800L, 1000L, 1000L, 1000L);
        assertThat(backend.attempts).isEqualTo(8);
    }

    @Test
    void asyncBackoffDoublesUpToTheMaximum() {
        backend.taken(7);
        String result = registry().executeLockedAsync("k", LONG_WAIT, () -> Uni.createFrom().item("done"))
                .await().atMost(Duration.ofSeconds(5));
        assertThat(result).isEqualTo("done");
        assertThat(backend.pauses).containsExactly(100L, 200L, 400L, 800L, 1000L, 1000L, 1000L);
    }

    @Test
    void jitterAppliesToEachPause() {
        backend.taken(3);
        registry(ms -> ms / 2).runLocked("k", LONG_WAIT, () -> {
        });
        assertThat(backend.pauses).containsExactly(50L, 100L, 200L);
    }

    @Test
    void defaultJitterStaysWithin80To120Percent() {
        for (int i = 0; i < 1000; i++) {
            assertThat(AcquisitionEngine.randomJitter(1000)).isBetween(800L, 1200L);
        }
    }

    // ------------------------------------------------------------------ deadline

    @Test
    void blockingTimesOutAtTheDeadline() {
        backend.taken(Integer.MAX_VALUE);
        LockRegistry registry = registry();
        assertThatThrownBy(() -> registry.executeLocked("k", Duration.ofSeconds(1), () -> "never"))
                .isInstanceOf(LockTimeoutException.class)
                .hasNoCause();
        // the last pause is cut to what is left of the wait
        assertThat(backend.pauses).containsExactly(100L, 200L, 400L, 300L);
        assertThat(backend.attempts).isEqualTo(5);
        assertThat(registry.activeKeys()).isZero();
    }

    @Test
    void tryExecuteLockedReturnsEmptyAtTheDeadline() {
        backend.taken(Integer.MAX_VALUE);
        assertThat(registry().tryExecuteLocked("k", Duration.ofMillis(250), () -> "never")).isEmpty();
        assertThat(backend.pauses).containsExactly(100L, 150L);
    }

    @Test
    void asyncTimesOutAtTheDeadline() {
        backend.taken(Integer.MAX_VALUE);
        LockRegistry registry = registry();
        Uni<String> locked = registry.executeLockedAsync("k", Duration.ofSeconds(1), () -> Uni.createFrom().item("never"));
        assertThatThrownBy(() -> locked.await().atMost(Duration.ofSeconds(5)))
                .isInstanceOf(LockTimeoutException.class);
        assertThat(backend.pauses).containsExactly(100L, 200L, 400L, 300L);
        assertThat(registry.activeKeys()).isZero();
    }

    @Test
    void zeroWaitMakesExactlyOneAttempt() {
        backend.taken(Integer.MAX_VALUE);
        assertThat(registry().obtain("k").tryLock()).isFalse();
        assertThat(backend.attempts).isEqualTo(1);
        assertThat(backend.pauses).isEmpty();
    }

    // ------------------------------------------------------------------ transient backend errors

    @Test
    void transientBackendErrorsAreRetried() {
        backend.failing(2);
        assertThat(registry().executeLocked("k", LONG_WAIT, () -> "done")).isEqualTo("done");
        assertThat(backend.pauses).containsExactly(100L, 200L);
        assertThat(backend.attempts).isEqualTo(3);
    }

    @Test
    void asyncTransientBackendErrorsAreRetried() {
        backend.failing(2);
        String result = registry().executeLockedAsync("k", LONG_WAIT, () -> Uni.createFrom().item("done"))
                .await().atMost(Duration.ofSeconds(5));
        assertThat(result).isEqualTo("done");
        assertThat(backend.attempts).isEqualTo(3);
    }

    @Test
    void timeoutCarriesTheLastBackendError() {
        backend.failing(Integer.MAX_VALUE);
        assertThatThrownBy(() -> registry().executeLocked("k", Duration.ofMillis(500), () -> "never"))
                .isInstanceOf(LockTimeoutException.class)
                .hasRootCauseMessage("backend unreachable");
    }

    @Test
    void anAnswerFromTheBackendClearsTheLastError() {
        backend.failing(1);
        backend.taken(Integer.MAX_VALUE);
        assertThatThrownBy(() -> registry().executeLocked("k", Duration.ofMillis(500), () -> "never"))
                .isInstanceOf(LockTimeoutException.class)
                .hasNoCause();
    }

    // ------------------------------------------------------------------ cancellation

    @Test
    void cancelWhileWaitingForAHintGivesUpWithoutALease() {
        backend.taken(1);
        CompletableFuture<Void> hint = new CompletableFuture<>();
        backend.hints.add(hint);
        // a long pause, so the hint's own timeout cannot fire during the test
        LockRegistryImpl registry = registry(ms -> ms * 1000);
        Cancellable subscription = registry.executeLockedAsync("k", LONG_WAIT, () -> Uni.createFrom().item("never"))
                .subscribe().with(item -> {
                }, failure -> {
                });
        assertThat(backend.attempts).isEqualTo(1);
        assertThat(registry.activeKeys()).isOne();

        subscription.cancel();
        hint.complete(null);

        assertThat(backend.attempts).isEqualTo(1);
        assertThat(registry.activeKeys()).isZero();
        assertThat(registry.tryExecuteLocked("k", Duration.ZERO, () -> "free")).contains("free");
    }

    @Test
    void cancelBeforeTheAttemptRunsGivesUpWithoutCallingTheBackend() {
        LockRegistryImpl registry = blockingBackendRegistry();
        Cancellable subscription = registry.executeLockedAsync("k", LONG_WAIT, () -> Uni.createFrom().item("never"))
                .subscribe().with(item -> {
                }, failure -> {
                });
        subscription.cancel();
        drainWorker();

        assertThat(backend.attempts).isZero();
        assertThat(registry.activeKeys()).isZero();
    }

    @Test
    void cancelRacingASuccessfulAttemptReleasesTheLease() {
        LockRegistryImpl registry = blockingBackendRegistry();
        AtomicReference<Cancellable> subscription = new AtomicReference<>();
        List<String> ran = new ArrayList<>();
        // the subscriber cancels while the backend is granting the lease
        backend.duringAcquire = () -> subscription.get().cancel();
        subscription.set(registry.executeLockedAsync("k", LONG_WAIT, () -> {
            ran.add("task");
            return Uni.createFrom().item("never");
        }).subscribe().with(item -> {
        }, failure -> {
        }));
        drainWorker();

        assertThat(backend.attempts).isEqualTo(1);
        assertThat(backend.released).containsExactly("k");
        assertThat(backend.held).isEmpty();
        assertThat(ran).isEmpty();
        assertThat(registry.activeKeys()).isZero();
    }

    // ------------------------------------------------------------------ lease lifecycle

    @Test
    void staleRenewalMarksTheLeaseLost() throws Exception {
        backend.leaseMillis = 300;
        backend.renewals.add(FakeBackend.stolen());
        DistributedLock lock = registry().obtain("k");
        lock.lock();
        LockToken held = lock.token().orElseThrow();

        LockLostEvent event = lostEvent.get(5, TimeUnit.SECONDS);

        assertThat(event.registry()).isEqualTo("test");
        assertThat(event.token()).isEqualTo(held);
        assertThat(event.cause()).isNull();
        assertThat(log.messages(Level.WARNING)).anyMatch(m -> m.contains("was lost while held: another owner took it"));
        assertThat(lock.isHeldByCurrentThread()).isFalse();
        assertThat(lock.token()).isEmpty();
        lock.unlock();
        // the lease is someone else's now: no release on the backend
        assertThat(backend.released).isEmpty();
        assertThat(log.messages(Level.WARNING)).anyMatch(m -> m.contains("whose lease (fencing 1) had been lost"));
    }

    @Test
    void renewalErrorsAtTheSafetyMarginMarkTheLeaseLost() throws Exception {
        backend.leaseMillis = 300;
        backend.renewals.add(FakeBackend.unreachable());
        DistributedLock lock = registry(true).obtain("k");
        lock.lock();
        LockToken held = lock.token().orElseThrow();
        // 30 ms left: exactly the safety margin of a 300 ms lease
        wallClock.set(wallClock.get().plusMillis(270));

        LockLostEvent event = lostEvent.get(5, TimeUnit.SECONDS);

        assertThat(event.token()).isEqualTo(held);
        assertThat(event.cause()).hasMessage("backend unreachable");
        assertThat(backend.renewCalls).hasValue(1);
        assertThatThrownBy(lock::unlock)
                .isInstanceOfSatisfying(LockLostException.class, e -> assertThat(e.token()).isEqualTo(held));
    }

    @Test
    void renewalErrorsBeforeTheSafetyMarginAreRetried() throws Exception {
        backend.leaseMillis = 300;
        backend.renewals.add(FakeBackend.unreachable());
        backend.renewals.add(FakeBackend.renewed());
        // renewals are serialized, so once the next one runs the retried one has updated the token
        CompletableFuture<Void> renewed = new CompletableFuture<>();
        backend.renewals.add(FakeBackend.renewedThen(renewed));
        DistributedLock lock = registry().obtain("k");
        lock.lock();
        LockToken acquired = lock.token().orElseThrow();
        // 31 ms left: just above the safety margin of a 300 ms lease
        wallClock.set(wallClock.get().plusMillis(269));

        renewed.get(5, TimeUnit.SECONDS);

        assertThat(backend.renewCalls).hasValue(3);
        assertThat(lostEvent).isNotDone();
        assertThat(lock.isHeldByCurrentThread()).isTrue();
        // a renewal bumps the fencing token but keeps the acquisition time
        LockToken current = lock.token().orElseThrow();
        assertThat(current.fencing()).isGreaterThan(acquired.fencing());
        assertThat(current.acquiredAt()).isEqualTo(acquired.acquiredAt());
        lock.unlock();
        assertThat(backend.released).containsExactly("k");
    }

    @Test
    void aFailingLostLeaseObserverIsLoggedAndContained() throws Exception {
        backend.leaseMillis = 300;
        backend.renewals.add(FakeBackend.stolen());
        lostListener = e -> {
            lostEvent.complete(e);
            throw new IllegalStateException("observer failed");
        };
        DistributedLock lock = registry().obtain("k");
        lock.lock();

        lostEvent.get(5, TimeUnit.SECONDS);

        assertThat(log.await(Level.SEVERE, "LockLostEvent observer failed for lock 'k'"))
                .hasMessage("observer failed");
        assertThat(lock.isHeldByCurrentThread()).isFalse();
        lock.unlock();
    }

    @Test
    void asyncLostLeaseFailsWhenConfigured() {
        backend.leaseMillis = 300;
        backend.renewals.add(FakeBackend.stolen());
        Uni<String> locked = registry(true).executeLockedAsync("k", LONG_WAIT,
                () -> Uni.createFrom().completionStage(lostEvent).replaceWith("done"));

        assertThatThrownBy(() -> locked.await().atMost(Duration.ofSeconds(5)))
                .isInstanceOfSatisfying(LockLostException.class,
                        e -> assertThat(e.token()).isEqualTo(lostEvent.join().token()));
        assertThat(backend.released).isEmpty();
    }

    // ------------------------------------------------------------------ fixtures

    private LockRegistryImpl registry() {
        return registry(LongUnaryOperator.identity());
    }

    private LockRegistryImpl registry(LongUnaryOperator jitter) {
        return registry(jitter, Runnable::run, false);
    }

    private LockRegistryImpl registry(boolean failOnLostLease) {
        return registry(LongUnaryOperator.identity(), Runnable::run, failOnLostLease);
    }

    /** A backend without {@link Capability#NON_BLOCKING}, whose worker runs only when the test drains it. */
    private LockRegistryImpl blockingBackendRegistry() {
        backend.nonBlocking = false;
        return registry(LongUnaryOperator.identity(), workerQueue::add, false);
    }

    private LockRegistryImpl registry(LongUnaryOperator jitter, Executor worker, boolean failOnLostLease) {
        return new LockRegistryImpl("test", backend, "jvm-1", LONG_WAIT, failOnLostLease, scheduler, worker,
                e -> lostListener.accept(e), clock::get, jitter, wallClock::get);
    }

    private void drainWorker() {
        Runnable r;
        while ((r = workerQueue.poll()) != null) {
            r.run();
        }
    }

    /**
     * Answers {@code tryAcquire} from a script ("taken" or a transient error), granting once it runs out. Leases never
     * expire unless the test sets {@code leaseMillis}; renewals then follow their own script of {@link Renewal}s,
     * renewing once it runs out. Release hints advance the fake clock by
     * their full pause and complete at once, unless the test queued its own hint.
     */
    private final class FakeBackend implements LockBackend {

        final Deque<Object> script = new ArrayDeque<>();
        final List<Long> pauses = new ArrayList<>();
        final Deque<CompletableFuture<Void>> hints = new ArrayDeque<>();
        final Set<String> held = new HashSet<>();
        final List<String> released = new ArrayList<>();
        final Deque<Renewal> renewals = new ConcurrentLinkedDeque<>();
        final AtomicInteger renewCalls = new AtomicInteger();
        volatile long leaseMillis;
        int attempts;
        final AtomicLong fencing = new AtomicLong();
        boolean nonBlocking = true;
        Runnable duringAcquire = () -> {
        };

        /** One scripted answer to {@code renew}. */
        interface Renewal {
            Optional<Lease> renew(FakeBackend backend, Lease lease);
        }

        static Renewal stolen() {
            return (b, lease) -> Optional.empty();
        }

        static Renewal unreachable() {
            return (b, lease) -> {
                throw new IllegalStateException("backend unreachable");
            };
        }

        static Renewal renewed() {
            return (b, lease) -> Optional.of(b.lease(lease.key(), lease.ownerId()));
        }

        static Renewal renewedThen(CompletableFuture<Void> done) {
            return (b, lease) -> {
                Optional<Lease> renewed = renewed().renew(b, lease);
                done.complete(null);
                return renewed;
            };
        }

        void taken(int times) {
            for (int i = 0; i < times && i < 10_000; i++) {
                script.add("taken");
            }
        }

        void failing(int times) {
            for (int i = 0; i < times && i < 10_000; i++) {
                script.add(new IllegalStateException("backend unreachable"));
            }
        }

        @Override
        public Optional<Lease> tryAcquire(String key, String ownerId) {
            attempts++;
            duringAcquire.run();
            Object next = script.poll();
            if (next instanceof RuntimeException e) {
                throw e;
            }
            if (next != null) {
                return Optional.empty();
            }
            held.add(key);
            return Optional.of(lease(key, ownerId));
        }

        @Override
        public Optional<Lease> renew(Lease lease) {
            renewCalls.incrementAndGet();
            Renewal next = renewals.poll();
            return (next == null ? renewed() : next).renew(this, lease);
        }

        private Lease lease(String key, String ownerId) {
            Instant expiresAt = leaseMillis > 0 ? wallClock.get().plusMillis(leaseMillis) : Instant.MAX;
            return new Lease(key, ownerId, fencing.incrementAndGet(), expiresAt, null);
        }

        @Override
        public void release(Lease lease) {
            held.remove(lease.key());
            released.add(lease.key());
        }

        @Override
        public Uni<Void> awaitReleaseHint(String key, Duration max) {
            pauses.add(max.toMillis());
            CompletableFuture<Void> hint = hints.poll();
            if (hint != null) {
                return Uni.createFrom().completionStage(hint);
            }
            clock.addAndGet(max.toNanos());
            return Uni.createFrom().voidItem();
        }

        @Override
        public Set<Capability> capabilities() {
            return nonBlocking ? EnumSet.of(Capability.NON_BLOCKING) : EnumSet.noneOf(Capability.class);
        }
    }

    /** Records what {@link KeyState} logs, for the duration of one test. */
    private static final class CapturedLog extends Handler {

        private final java.util.logging.Logger logger = java.util.logging.Logger.getLogger(KeyState.class.getName());
        private final List<LogRecord> records = new CopyOnWriteArrayList<>();

        CapturedLog() {
            logger.addHandler(this);
        }

        List<String> messages(Level level) {
            return records.stream().filter(r -> r.getLevel().equals(level)).map(this::message).toList();
        }

        /** The first record at {@code level} whose message starts with {@code prefix}; its throwable, for asserting. */
        Throwable await(Level level, String prefix) throws InterruptedException {
            for (int i = 0; i < 500; i++) {
                for (LogRecord r : records) {
                    if (r.getLevel().equals(level) && message(r).startsWith(prefix)) {
                        return r.getThrown();
                    }
                }
                Thread.sleep(10);
            }
            throw new AssertionError("Nothing logged at " + level + " starting with: " + prefix);
        }

        private String message(LogRecord r) {
            return new SimpleFormatter().formatMessage(r);
        }

        @Override
        public void publish(LogRecord record) {
            records.add(record);
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
            logger.removeHandler(this);
        }
    }
}
