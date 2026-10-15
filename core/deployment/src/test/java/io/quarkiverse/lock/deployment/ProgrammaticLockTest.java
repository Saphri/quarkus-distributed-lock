package io.quarkiverse.lock.deployment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.inject.Inject;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.lock.DistributedLock;
import io.quarkiverse.lock.LockRegistry;
import io.quarkiverse.lock.LockRegistryName;
import io.quarkiverse.lock.LockTimeoutException;
import io.quarkiverse.lock.LockToken;
import io.quarkiverse.lock.LockedExecutionException;
import io.quarkiverse.lock.runtime.LockRegistryImpl;
import io.quarkus.test.QuarkusExtensionTest;

class ProgrammaticLockTest {

    @RegisterExtension
    static final QuarkusExtensionTest TEST = new QuarkusExtensionTest()
            .overrideConfigKey("quarkus.lock.wait-timeout", "2s")
            .overrideConfigKey("quarkus.lock.jobs.wait-timeout", "0s");

    @Inject
    LockRegistry locks;

    @Inject
    @LockRegistryName("jobs")
    LockRegistry jobs;

    ExecutorService pool;

    @BeforeEach
    void setup() {
        pool = Executors.newFixedThreadPool(8);
    }

    @AfterEach
    void cleanup() {
        pool.shutdownNow();
    }

    @Test
    void registriesAreConfiguredWithFallback() {
        assertThat(locks.name()).isEqualTo(LockRegistryName.DEFAULT);
        assertThat(((LockRegistryImpl) locks).defaultWait()).isEqualTo(Duration.ofSeconds(2));
        assertThat(jobs.name()).isEqualTo("jobs");
        assertThat(((LockRegistryImpl) jobs).defaultWait()).isEqualTo(Duration.ZERO);
    }

    @Test
    void mutualExclusion() throws Exception {
        AtomicInteger inside = new AtomicInteger();
        AtomicInteger maxInside = new AtomicInteger();
        AtomicInteger count = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            futures.add(pool.submit(() -> {
                for (int j = 0; j < 50; j++) {
                    locks.runLocked("counter", Duration.ofSeconds(10), () -> {
                        int now = inside.incrementAndGet();
                        maxInside.accumulateAndGet(now, Math::max);
                        count.incrementAndGet();
                        inside.decrementAndGet();
                    });
                }
            }));
        }
        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        assertThat(count).hasValue(400);
        assertThat(maxInside).hasValue(1);
        assertThat(((LockRegistryImpl) locks).activeKeys()).isZero();
    }

    @Test
    void reentrant() {
        String r = locks.executeLocked("re", () -> locks.executeLocked("re", () -> "inner"));
        assertThat(r).isEqualTo("inner");
        DistributedLock lock = locks.obtain("re");
        lock.lock();
        lock.lock();
        lock.unlock();
        assertThat(lock.isHeldByCurrentThread()).isTrue();
        lock.unlock();
        assertThat(lock.isHeldByCurrentThread()).isFalse();
    }

    @Test
    void contention() throws Exception {
        CountDownLatch held = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(1);
        Future<?> holder = pool.submit(() -> locks.runLocked("busy", () -> {
            held.countDown();
            try {
                done.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
        assertThat(held.await(5, TimeUnit.SECONDS)).isTrue();

        assertThat(locks.tryExecuteLocked("busy", Duration.ZERO, () -> "x")).isEmpty();
        long t0 = System.nanoTime();
        assertThatThrownBy(() -> locks.executeLocked("busy", Duration.ofMillis(300), () -> "x"))
                .isInstanceOf(LockTimeoutException.class)
                .hasMessageContaining("busy");
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0)).isBetween(250L, 2000L);
        // named registry: default wait 0s, separate lock space
        assertThat(jobs.tryExecuteLocked("busy", Duration.ZERO, () -> "free")).contains("free");

        done.countDown();
        holder.get(5, TimeUnit.SECONDS);
        assertThat(locks.tryExecuteLocked("busy", Duration.ZERO, () -> "free")).contains("free");
    }

    @Test
    void waiterAcquiresAfterRelease() throws Exception {
        CountDownLatch held = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Future<?> holder = pool.submit(() -> locks.runLocked("handover", () -> {
            held.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
        held.await();
        Future<String> waiter = pool.submit(() -> locks.executeLocked("handover", Duration.ofSeconds(5), () -> "got it"));
        Thread.sleep(100);
        release.countDown();
        assertThat(waiter.get(5, TimeUnit.SECONDS)).isEqualTo("got it");
        holder.get();
    }

    @Test
    void exceptionsAndRelease() {
        IllegalStateException boom = new IllegalStateException("boom");
        assertThatThrownBy(() -> locks.executeLocked("ex", () -> {
            throw boom;
        })).isSameAs(boom);
        assertThatThrownBy(() -> locks.executeLocked("ex", () -> {
            throw new IOException("io");
        })).isInstanceOf(LockedExecutionException.class).hasCauseInstanceOf(IOException.class);
        // released both times
        assertThat(locks.tryExecuteLocked("ex", Duration.ZERO, () -> 1)).contains(1);
        assertThat(((LockRegistryImpl) locks).activeKeys()).isZero();
    }

    @Test
    void fencingTokens() {
        DistributedLock lock = locks.obtain("fence");
        assertThat(lock.token()).isEmpty();
        lock.lock();
        LockToken first = lock.token().orElseThrow();
        lock.unlock();
        lock.lock();
        LockToken second = lock.token().orElseThrow();
        lock.unlock();
        assertThat(second.fencing()).isGreaterThan(first.fencing());
        assertThat(first.key()).isEqualTo("fence");
        assertThat(first.owner()).contains(":");
        assertThat(first.owner()).isNotEqualTo(second.owner());
    }

    @Test
    void unlockFromOtherThreadFails() throws Exception {
        DistributedLock lock = locks.obtain("owner");
        lock.lock();
        try {
            Future<?> f = pool.submit(lock::unlock);
            assertThatThrownBy(f::get).hasCauseInstanceOf(IllegalMonitorStateException.class);
            assertThatThrownBy(() -> locks.obtain("never-locked").unlock())
                    .isInstanceOf(IllegalMonitorStateException.class);
        } finally {
            lock.unlock();
        }
    }

    @Test
    void lockApi() throws Exception {
        DistributedLock lock = locks.obtain("api");
        assertThat(lock.tryLock()).isTrue();
        Future<Boolean> other = pool.submit(() -> lock.tryLock(50, TimeUnit.MILLISECONDS));
        assertThat(other.get()).isFalse();
        lock.unlock();
        assertThat(pool.submit(() -> {
            boolean ok = lock.tryLock(Duration.ofSeconds(1));
            lock.unlock();
            return ok;
        }).get()).isTrue();
        assertThatThrownBy(lock::newCondition).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> locks.obtain("")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void lockInterruptibly() throws Exception {
        DistributedLock lock = locks.obtain("intr");
        lock.lock();
        try {
            CountDownLatch started = new CountDownLatch(1);
            Thread t = new Thread(() -> {
                started.countDown();
                try {
                    lock.lockInterruptibly();
                    lock.unlock();
                } catch (InterruptedException e) {
                    // expected
                }
            });
            t.start();
            started.await();
            Thread.sleep(100);
            t.interrupt();
            t.join(2000);
            assertThat(t.isAlive()).isFalse();
        } finally {
            lock.unlock();
        }
        assertThat(((LockRegistryImpl) locks).activeKeys()).isZero();
    }

    @Test
    void tryExecuteLockedWithNullResult() {
        Optional<Object> r = locks.tryExecuteLocked("null", Duration.ZERO, () -> null);
        assertThat(r).isEmpty();
    }
}
