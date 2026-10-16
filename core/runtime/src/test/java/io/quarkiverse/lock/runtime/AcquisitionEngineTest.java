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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongUnaryOperator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.quarkiverse.lock.LockRegistry;
import io.quarkiverse.lock.LockTimeoutException;
import io.quarkiverse.lock.spi.Capability;
import io.quarkiverse.lock.spi.Lease;
import io.quarkiverse.lock.spi.LockBackend;
import io.quarkus.runtime.BlockingOperationControl;
import io.quarkus.runtime.IOThreadDetector;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;

/**
 * The acquisition loop through the {@link LockRegistry} interface, against a scripted backend and a fake clock: no
 * Quarkus boot and no real waiting.
 */
class AcquisitionEngineTest {

    private static final Duration LONG_WAIT = Duration.ofMinutes(1);

    private final AtomicLong clock = new AtomicLong();
    private final FakeBackend backend = new FakeBackend();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final Deque<Runnable> workerQueue = new ArrayDeque<>();

    @BeforeAll
    static void noQuarkus() {
        // Quarkus installs its event-loop detectors on boot; without one, every thread may block
        BlockingOperationControl.setIoThreadDetector(new IOThreadDetector[0]);
    }

    @AfterEach
    void shutdown() {
        scheduler.shutdownNow();
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

    // ------------------------------------------------------------------ fixtures

    private LockRegistryImpl registry() {
        return registry(LongUnaryOperator.identity());
    }

    private LockRegistryImpl registry(LongUnaryOperator jitter) {
        return registry(jitter, Runnable::run);
    }

    /** A backend without {@link Capability#NON_BLOCKING}, whose worker runs only when the test drains it. */
    private LockRegistryImpl blockingBackendRegistry() {
        backend.nonBlocking = false;
        return registry(LongUnaryOperator.identity(), workerQueue::add);
    }

    private LockRegistryImpl registry(LongUnaryOperator jitter, Executor worker) {
        return new LockRegistryImpl("test", backend, "jvm-1", LONG_WAIT, false, scheduler, worker, e -> {
        }, clock::get, jitter);
    }

    private void drainWorker() {
        Runnable r;
        while ((r = workerQueue.poll()) != null) {
            r.run();
        }
    }

    /**
     * Answers {@code tryAcquire} from a script ("taken" or a transient error), granting once it runs out. Release hints
     * advance the fake clock by their full pause and complete at once, unless the test queued its own hint.
     */
    private final class FakeBackend implements LockBackend {

        final Deque<Object> script = new ArrayDeque<>();
        final List<Long> pauses = new ArrayList<>();
        final Deque<CompletableFuture<Void>> hints = new ArrayDeque<>();
        final Set<String> held = new HashSet<>();
        final List<String> released = new ArrayList<>();
        int attempts;
        long fencing;
        boolean nonBlocking = true;
        Runnable duringAcquire = () -> {
        };

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
            return Optional.of(new Lease(key, ownerId, ++fencing, Instant.MAX, null));
        }

        @Override
        public Optional<Lease> renew(Lease lease) {
            return Optional.of(lease);
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
}
