package io.quarkiverse.lock.nats.deployment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.nats.client.Connection;
import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.KeyValueStatus;
import io.quarkiverse.lock.DistributedLock;
import io.quarkiverse.lock.LockKey;
import io.quarkiverse.lock.LockLostEvent;
import io.quarkiverse.lock.LockRegistry;
import io.quarkiverse.lock.LockRegistryName;
import io.quarkiverse.lock.LockToken;
import io.quarkiverse.lock.Locked;
import io.quarkiverse.lock.nats.runtime.KeyCodec;
import io.quarkiverse.lock.runtime.LockRegistries;
import io.quarkiverse.reactive.messaging.nats.jetstream.client.Client;
import io.quarkus.test.QuarkusExtensionTest;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;

class NatsLockTest {

    @RegisterExtension
    static final QuarkusExtensionTest TEST = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(Billing.class, LostObserver.class))
            .overrideConfigKey("quarkus.messaging.nats.devservices.image-name", System.getProperty("nats.image", "nats:2.12-alpine"))
            .overrideConfigKey("quarkus.lock.type", "nats")
            .overrideConfigKey("quarkus.lock.wait-timeout", "10s")
            .overrideConfigKey("quarkus.lock.nats.lease", "2s")
            .overrideConfigKey("quarkus.lock.nats.storage", "memory")
            .overrideConfigKey("quarkus.lock.jobs.nats.lease", "3s");

    static final Duration LEASE = Duration.ofSeconds(2);

    @ApplicationScoped
    public static class Billing {
        final AtomicInteger inside = new AtomicInteger();
        final AtomicInteger max = new AtomicInteger();

        @Locked("invoice-{id}")
        public void bill(@LockKey("id") String id) {
            max.accumulateAndGet(inside.incrementAndGet(), Math::max);
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            inside.decrementAndGet();
        }

        public int max() {
            return max.get();
        }

        @Locked("reactive-{id}")
        public Uni<String> reactive(@LockKey("id") String id) {
            return Uni.createFrom().item("r" + id).onItem().delayIt().by(Duration.ofMillis(20));
        }
    }

    @ApplicationScoped
    public static class LostObserver {
        final List<LockLostEvent> events = new CopyOnWriteArrayList<>();

        void onLost(@Observes LockLostEvent e) {
            events.add(e);
        }

        List<LockLostEvent> events() {
            return events;
        }
    }

    @Inject
    LockRegistry locks;

    @Inject
    @LockRegistryName("jobs")
    LockRegistry jobs;

    @Inject
    LockRegistries registries;

    @Inject
    Billing billing;

    @Inject
    LostObserver lost;

    @Inject
    @Identifier("default")
    Client client;

    KeyValue kv;

    @BeforeEach
    void kv() throws Exception {
        kv = client.nativeConnection().keyValue("locks");
    }

    @Test
    void bucketsProvisioned() throws Exception {
        Connection c = client.nativeConnection();
        KeyValueStatus status = c.keyValueManagement().getStatus("locks");
        assertThat(status.getTtl()).isEqualTo(LEASE);
        assertThat(status.getMaxHistoryPerKey()).isEqualTo(1);
        // limit markers on: waiters see lease age-outs instead of polling
        assertThat(status.getLimitMarkerTtl()).isEqualTo(Duration.ofSeconds(1));
        assertThat(c.keyValueManagement().getStatus("locks_jobs").getTtl()).isEqualTo(Duration.ofSeconds(3));
        // the unnamed quarkus.lock.nats.* group is not mistaken for a registry called "nats"
        assertThat(registries.created()).containsOnlyKeys(LockRegistryName.DEFAULT, "jobs");
    }

    @Test
    void lockLivesInBucketWithFencing() throws Exception {
        DistributedLock lock = locks.obtain("orders");
        lock.lock();
        LockToken t1;
        try {
            t1 = lock.token().orElseThrow();
            KeyValueEntry e = kv.get("orders");
            assertThat(e).isNotNull();
            assertThat(e.getRevision()).isEqualTo(t1.fencing());
            assertThat(e.getValueAsString()).contains("\"owner\":\"" + t1.owner() + "\"");
            // another JVM cannot create it
            assertThat(tryForeignCreate("orders")).isFalse();
        } finally {
            lock.unlock();
        }
        assertThat(kv.get("orders")).isNull();
        LockToken t2 = locks.executeLocked("orders", () -> locks.obtain("orders").token().orElseThrow());
        assertThat(t2.fencing()).isGreaterThan(t1.fencing());
    }

    @Test
    void renewalKeepsTheLockBeyondTheLease() throws Exception {
        DistributedLock lock = locks.obtain("long-task");
        lock.lock();
        try {
            long first = lock.token().orElseThrow().fencing();
            Thread.sleep(LEASE.toMillis() * 2 + 500);
            assertThat(lock.isHeldByCurrentThread()).isTrue();
            assertThat(lock.token().orElseThrow().fencing()).isGreaterThan(first);
            assertThat(tryForeignCreate("long-task")).isFalse();
        } finally {
            lock.unlock();
        }
        assertThat(lost.events()).isEmpty();
    }

    @Test
    void waiterWakesOnForeignRelease() throws Exception {
        long rev = kv.create("handover", "{\"owner\":\"other-jvm\"}".getBytes());
        CompletableFuture<Long> acquiredAfter = CompletableFuture.supplyAsync(() -> {
            long t0 = System.nanoTime();
            locks.runLocked("handover", () -> {
            });
            return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        });
        // let the waiter go through a couple of backoff rounds so it is parked on the watcher
        Thread.sleep(1200);
        long releasedAt = System.nanoTime();
        kv.delete("handover", rev);
        long waited = acquiredAfter.get(10, TimeUnit.SECONDS);
        long sinceRelease = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - releasedAt);
        assertThat(waited).isGreaterThanOrEqualTo(1200);
        // woken by the watch event, not by the 1s poll
        assertThat(sinceRelease).isLessThan(500);
    }

    @Test
    void crashedHolderAgesOut() throws Exception {
        kv.create("crashed", "{\"owner\":\"dead-jvm\"}".getBytes());
        long t0 = System.nanoTime();
        locks.runLocked("crashed", () -> {
        });
        long waited = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        assertThat(waited).isBetween(LEASE.toMillis() - 100, LEASE.toMillis() + 1000);
    }

    @Test
    void lostLeaseIsDetected() throws Exception {
        DistributedLock lock = locks.obtain("stolen");
        lock.lock();
        try {
            // an operator purges the key and another node takes it
            kv.purge("stolen");
            kv.create("stolen", "{\"owner\":\"thief\"}".getBytes());
            await().pollInSameThread().atMost(Duration.ofSeconds(3)).until(() -> !lock.isHeldByCurrentThread());
            assertThat(lost.events()).anySatisfy(e -> assertThat(e.token().key()).isEqualTo("stolen"));
        } finally {
            lock.unlock();
        }
        // our release did not delete the thief's lock
        assertThat(kv.get("stolen").getValueAsString()).contains("thief");
        kv.purge("stolen");
    }

    @Test
    void interceptorAndAsync() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            CountDownLatch go = new CountDownLatch(1);
            Future<?>[] fs = new Future<?>[4];
            for (int i = 0; i < 4; i++) {
                fs[i] = pool.submit(() -> {
                    go.await();
                    for (int j = 0; j < 5; j++) {
                        billing.bill("42");
                    }
                    return null;
                });
            }
            go.countDown();
            for (Future<?> f : fs) {
                f.get(30, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
        assertThat(billing.max()).isEqualTo(1);
        assertThat(billing.reactive("7").await().atMost(Duration.ofSeconds(5))).isEqualTo("r7");
        assertThat(kv.get("reactive-7")).isNull();
    }

    @Test
    void namedRegistryHasOwnBucket() throws Exception {
        String r = jobs.executeLocked("nightly", () -> {
            KeyValue jobsKv = client.nativeConnection().keyValue("locks_jobs");
            return jobsKv.get("nightly") != null ? "in jobs bucket" : "missing";
        });
        assertThat(r).isEqualTo("in jobs bucket");
        assertThat(kv.get("nightly")).isNull();
    }

    @Test
    void keysAreEncoded() throws Exception {
        String key = "user:jane@example.com/cart 1";
        locks.runLocked(key, () -> {
            try {
                assertThat(kv.get(KeyCodec.encode(key))).isNotNull();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        assertThat(KeyCodec.encode(key)).isEqualTo("user=3Ajane=40example.com/cart=201");
    }

    private boolean tryForeignCreate(String key) throws Exception {
        try {
            kv.create(key, "{\"owner\":\"other-jvm\"}".getBytes());
            return true;
        } catch (io.nats.client.JetStreamApiException e) {
            return false;
        }
    }
}
