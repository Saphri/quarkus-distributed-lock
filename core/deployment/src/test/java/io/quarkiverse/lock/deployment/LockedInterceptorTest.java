package io.quarkiverse.lock.deployment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.lock.LockKey;
import io.quarkiverse.lock.LockRegistry;
import io.quarkiverse.lock.LockRegistryName;
import io.quarkiverse.lock.LockTimeoutException;
import io.quarkiverse.lock.Locked;
import io.quarkiverse.lock.OnTimeout;
import io.quarkus.test.QuarkusExtensionTest;
import io.smallrye.mutiny.Uni;

class LockedInterceptorTest {

    @RegisterExtension
    static final QuarkusExtensionTest TEST = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(Service.class, ClassLevel.class))
            .overrideConfigKey("quarkus.lock.wait-timeout", "5s");

    @Inject
    Service service;

    @Inject
    ClassLevel classLevel;

    @Inject
    LockRegistry locks;

    @Inject
    @LockRegistryName("jobs")
    LockRegistry jobs;

    @ApplicationScoped
    public static class Service {

        final List<String> calls = new CopyOnWriteArrayList<>();
        final AtomicInteger inside = new AtomicInteger();
        final AtomicInteger maxInside = new AtomicInteger();

        @Locked("simple")
        public String simple() {
            max();
            sleep(5);
            inside.decrementAndGet();
            return "ok";
        }

        @Locked(value = "invoice-{id}", waitTimeout = "100ms")
        public void bill(@LockKey("id") long id, String note) {
            calls.add("bill " + id + " " + note);
        }

        @Locked(value = "outer")
        public String outer() {
            return inner();
        }

        @Locked(value = "outer")
        public String inner() {
            return "inner";
        }

        @Locked(value = "job", waitTimeout = "0s", onTimeout = OnTimeout.SKIP, registry = "jobs")
        public void job() {
            calls.add("job");
        }

        @Locked(value = "fail", waitTimeout = "0s")
        public String failFast() {
            return "ran";
        }

        @Locked(value = "uni-{k}")
        public Uni<String> uni(@LockKey("k") String k) {
            return Uni.createFrom().item("uni " + k).onItem().delayIt().by(Duration.ofMillis(50));
        }

        @Locked(value = "uni-skip", waitTimeout = "0s", onTimeout = OnTimeout.SKIP)
        public Uni<Void> uniSkip() {
            calls.add("uniSkip");
            return Uni.createFrom().voidItem();
        }

        @Locked(value = "stage")
        public CompletionStage<String> stage() {
            return CompletableFuture.supplyAsync(() -> "stage");
        }

        public List<String> calls() {
            return calls;
        }

        public int maxInside() {
            return maxInside.get();
        }

        private void max() {
            maxInside.accumulateAndGet(inside.incrementAndGet(), Math::max);
        }

        private static void sleep(long ms) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @ApplicationScoped
    @Locked(value = "class-level", waitTimeout = "0s")
    public static class ClassLevel {

        public String a() {
            return "a";
        }
    }

    @Test
    void exclusion() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            Future<?>[] fs = new Future<?>[4];
            for (int i = 0; i < 4; i++) {
                fs[i] = pool.submit(() -> {
                    for (int j = 0; j < 10; j++) {
                        service.simple();
                    }
                });
            }
            for (Future<?> f : fs) {
                f.get(30, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
        assertThat(service.maxInside()).isEqualTo(1);
    }

    @Test
    void placeholdersAndTimeout() throws Exception {
        service.bill(42, "first");
        assertThat(service.calls()).contains("bill 42 first");
        CountDownLatch held = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(1);
        Thread t = new Thread(() -> locks.runLocked("invoice-7", () -> {
            held.countDown();
            try {
                done.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
        t.start();
        held.await();
        try {
            assertThatThrownBy(() -> service.bill(7, "blocked")).isInstanceOf(LockTimeoutException.class)
                    .hasMessageContaining("invoice-7");
            service.bill(8, "other key");
        } finally {
            done.countDown();
            t.join();
        }
        assertThat(service.calls()).contains("bill 8 other key").doesNotContain("bill 7 blocked");
    }

    @Test
    void reentrantAcrossMethods() {
        assertThat(service.outer()).isEqualTo("inner");
    }

    @Test
    void skipOnTimeoutAndNamedRegistry() throws Exception {
        CountDownLatch held = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(1);
        Thread t = new Thread(() -> jobs.runLocked("job", () -> {
            held.countDown();
            try {
                done.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
        t.start();
        held.await();
        try {
            service.job();
            assertThat(service.calls()).doesNotContain("job");
            // the default registry is a separate lock space
            assertThat(locks.tryExecuteLocked("job", Duration.ZERO, () -> "free")).contains("free");
        } finally {
            done.countDown();
            t.join();
        }
        service.job();
        assertThat(service.calls()).contains("job");
    }

    @Test
    void asyncReturnTypes() throws Exception {
        assertThat(service.uni("a").await().atMost(Duration.ofSeconds(5))).isEqualTo("uni a");
        assertThat(service.stage().toCompletableFuture().get(5, TimeUnit.SECONDS)).isEqualTo("stage");

        // lock is held until the Uni completes, not until the method returns
        CompletableFuture<String> pending = service.uni("b").subscribeAsCompletionStage();
        Thread.sleep(10);
        assertThat(locks.tryExecuteLocked("uni-b", Duration.ZERO, () -> "x")).isEmpty();
        assertThat(pending.get(5, TimeUnit.SECONDS)).isEqualTo("uni b");
        assertThat(locks.tryExecuteLocked("uni-b", Duration.ZERO, () -> "x")).contains("x");

        CompletableFuture<Void> release = new CompletableFuture<>();
        locks.executeLockedAsync("uni-skip", () -> Uni.createFrom().completionStage(release)).subscribe().with(v -> {
        });
        Thread.sleep(50);
        assertThat(service.uniSkip().await().atMost(Duration.ofSeconds(5))).isNull();
        assertThat(service.calls()).doesNotContain("uniSkip");
        release.complete(null);
    }

    @Test
    void classLevelBinding() throws Exception {
        assertThat(classLevel.a()).isEqualTo("a");
        CountDownLatch held = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(1);
        Thread t = new Thread(() -> locks.runLocked("class-level", () -> {
            held.countDown();
            try {
                done.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
        t.start();
        held.await();
        try {
            assertThatThrownBy(classLevel::a).isInstanceOf(LockTimeoutException.class);
        } finally {
            done.countDown();
            t.join();
        }
    }
}
