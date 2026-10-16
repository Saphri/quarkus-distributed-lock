package io.quarkiverse.lock.deployment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.inject.Inject;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.lock.LockRegistry;
import io.quarkiverse.lock.LockTimeoutException;
import io.quarkus.test.QuarkusExtensionTest;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import io.vertx.core.Vertx;

class AsyncLockTest {

    @RegisterExtension
    static final QuarkusExtensionTest TEST = new QuarkusExtensionTest();

    @Inject
    LockRegistry locks;

    @Inject
    Vertx vertx;

    @Test
    void asyncMutualExclusion() {
        AtomicInteger inside = new AtomicInteger();
        AtomicInteger max = new AtomicInteger();
        List<CompletableFuture<Integer>> all = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            int n = i;
            all.add(locks.executeLockedAsync("async", Duration.ofSeconds(20), () -> {
                max.accumulateAndGet(inside.incrementAndGet(), Math::max);
                return Uni.createFrom().item(n).onItem().delayIt().by(Duration.ofMillis(10))
                        .invoke(() -> inside.decrementAndGet());
            }).subscribeAsCompletionStage());
        }
        CompletableFuture.allOf(all.toArray(CompletableFuture[]::new)).join();
        assertThat(max).hasValue(1);
        assertThat(locks.activeKeys()).isZero();
    }

    @Test
    void asyncAndBlockingExcludeEachOther() throws Exception {
        CompletableFuture<Void> release = new CompletableFuture<>();
        CompletableFuture<String> holder = locks.executeLockedAsync("mixed",
                () -> Uni.createFrom().completionStage(release).replaceWith("held")).subscribeAsCompletionStage();
        Thread.sleep(100);
        assertThat(locks.tryExecuteLocked("mixed", Duration.ZERO, () -> "x")).isEmpty();
        release.complete(null);
        assertThat(holder.get(5, TimeUnit.SECONDS)).isEqualTo("held");
        assertThat(locks.tryExecuteLocked("mixed", Duration.ZERO, () -> "x")).contains("x");
    }

    @Test
    void asyncTimeoutAndFailureRelease() throws Exception {
        CompletableFuture<Void> release = new CompletableFuture<>();
        locks.executeLockedAsync("t", () -> Uni.createFrom().completionStage(release)).subscribe().with(v -> {
        });
        Thread.sleep(50);
        assertThatThrownBy(() -> locks.executeLockedAsync("t", Duration.ofMillis(200), () -> Uni.createFrom().item(1))
                .await().atMost(Duration.ofSeconds(5)))
                .isInstanceOf(LockTimeoutException.class);
        release.complete(null);

        RuntimeException boom = new RuntimeException("boom");
        assertThatThrownBy(() -> locks.executeLockedAsync("t", Duration.ofSeconds(2), () -> Uni.createFrom().failure(boom))
                .await().atMost(Duration.ofSeconds(5))).isSameAs(boom);
        assertThatThrownBy(() -> locks.executeLockedAsync("t", Duration.ofSeconds(2), () -> {
            throw boom;
        }).await().atMost(Duration.ofSeconds(5))).isSameAs(boom);
        assertThat(locks.tryExecuteLocked("t", Duration.ofSeconds(1), () -> "free")).contains("free");
        assertThat(locks.activeKeys()).isZero();
    }

    @Test
    void cancellationReleases() throws Exception {
        Cancellable c = locks.executeLockedAsync("cancel", () -> Uni.createFrom().nothing()).subscribe().with(x -> {
        });
        Thread.sleep(100);
        assertThat(locks.tryExecuteLocked("cancel", Duration.ZERO, () -> "x")).isEmpty();
        c.cancel();
        assertThat(locks.tryExecuteLocked("cancel", Duration.ofSeconds(2), () -> "x")).contains("x");

        // cancel while still waiting for the lock
        CompletableFuture<Void> release = new CompletableFuture<>();
        locks.executeLockedAsync("cancel", () -> Uni.createFrom().completionStage(release)).subscribe().with(x -> {
        });
        Thread.sleep(50);
        Cancellable waiting = locks.executeLockedAsync("cancel", Duration.ofSeconds(30), () -> Uni.createFrom().item(1))
                .subscribe().with(x -> {
                });
        Thread.sleep(50);
        waiting.cancel();
        release.complete(null);
        assertThat(locks.tryExecuteLocked("cancel", Duration.ofSeconds(3), () -> "x")).contains("x");
        Thread.sleep(1200);
        assertThat(locks.activeKeys()).isZero();
    }

    @Test
    void blockingOnEventLoopIsRejectedAndAsyncResumesOnContext() throws Exception {
        CompletableFuture<Throwable> blocking = new CompletableFuture<>();
        AtomicReference<String> threadOfTask = new AtomicReference<>();
        CompletableFuture<String> asyncThread = new CompletableFuture<>();
        vertx.runOnContext(v -> {
            try {
                locks.executeLocked("el", () -> "x");
                blocking.complete(null);
            } catch (Throwable t) {
                blocking.complete(t);
            }
            locks.executeLockedAsync("el", () -> {
                threadOfTask.set(Thread.currentThread().getName());
                return Uni.createFrom().item("ok");
            }).subscribe().with(i -> asyncThread.complete(Thread.currentThread().getName()), asyncThread::completeExceptionally);
        });
        assertThat(blocking.get(5, TimeUnit.SECONDS)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("event-loop");
        assertThat(asyncThread.get(5, TimeUnit.SECONDS)).contains("eventloop");
        assertThat(threadOfTask.get()).contains("eventloop");
    }
}
