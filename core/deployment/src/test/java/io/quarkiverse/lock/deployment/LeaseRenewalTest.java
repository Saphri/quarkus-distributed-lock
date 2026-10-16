package io.quarkiverse.lock.deployment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.lock.DistributedLock;
import io.quarkiverse.lock.LockLostEvent;
import io.quarkiverse.lock.LockLostException;
import io.quarkiverse.lock.LockRegistry;
import io.quarkiverse.lock.LockRegistryName;
import io.quarkiverse.lock.LockToken;
import io.quarkiverse.lock.spi.Capability;
import io.quarkiverse.lock.spi.Lease;
import io.quarkiverse.lock.spi.LockBackend;
import io.quarkiverse.lock.spi.LockBackendFactory;
import io.quarkus.test.QuarkusExtensionTest;

/**
 * Renewal and lost-lease handling in the core, against a fake expiring backend.
 */
class LeaseRenewalTest {

    @RegisterExtension
    static final QuarkusExtensionTest TEST = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(FakeFactory.class, FakeBackend.class, LostObserver.class))
            .overrideConfigKey("quarkus.lock.type", "fake")
            .overrideConfigKey("quarkus.lock.strict.fail-on-lost-lease", "true");

    static final Duration TTL = Duration.ofMillis(600);

    /** A single-JVM stand-in for a lease store with expiry, driven by flags. */
    public static class FakeBackend implements LockBackend {
        final ConcurrentHashMap<String, Lease> leases = new ConcurrentHashMap<>();
        final AtomicLong seq = new AtomicLong();
        final AtomicInteger renewals = new AtomicInteger();
        final AtomicInteger releases = new AtomicInteger();
        volatile boolean failRenewals;

        @Override
        public Optional<Lease> tryAcquire(String key, String ownerId) {
            Lease l = new Lease(key, ownerId, seq.incrementAndGet(), Instant.now().plus(TTL), null);
            Lease prev = leases.compute(key, (k, cur) -> cur == null || cur.expiresAt().isBefore(Instant.now()) ? l : cur);
            return prev == l ? Optional.of(l) : Optional.empty();
        }

        @Override
        public Optional<Lease> renew(Lease lease) {
            if (failRenewals) {
                throw new IllegalStateException("backend unreachable");
            }
            Lease next = new Lease(lease.key(), lease.ownerId(), seq.incrementAndGet(), Instant.now().plus(TTL), null);
            Lease r = leases.computeIfPresent(lease.key(), (k, cur) -> cur.fencing() == lease.fencing() ? next : cur);
            if (r == next) {
                renewals.incrementAndGet();
                return Optional.of(next);
            }
            return Optional.empty();
        }

        @Override
        public void release(Lease lease) {
            releases.incrementAndGet();
            leases.computeIfPresent(lease.key(), (k, cur) -> cur.fencing() == lease.fencing() ? null : cur);
        }

        /** Simulates another owner taking the key after our lease expired. */
        void steal(String key) {
            leases.put(key, new Lease(key, "thief", seq.incrementAndGet(), Instant.now().plus(Duration.ofMinutes(1)), null));
        }

        @Override
        public Set<Capability> capabilities() {
            return EnumSet.noneOf(Capability.class);
        }
    }

    @ApplicationScoped
    public static class FakeFactory implements LockBackendFactory {
        final ConcurrentHashMap<String, FakeBackend> backends = new ConcurrentHashMap<>();

        @Override
        public String type() {
            return "fake";
        }

        @Override
        public LockBackend create(String registryName) {
            return backends.computeIfAbsent(registryName, n -> new FakeBackend());
        }

        FakeBackend backend(String registry) {
            return backends.get(registry);
        }
    }

    @ApplicationScoped
    public static class LostObserver {
        final List<LockLostEvent> events = new CopyOnWriteArrayList<>();
        volatile Instant lostAt;

        void onLost(@Observes LockLostEvent e) {
            lostAt = Instant.now();
            events.add(e);
        }

        Instant lostAt() {
            return lostAt;
        }

        List<LockLostEvent> events() {
            return events;
        }

        void clear() {
            events.clear();
        }
    }

    @Inject
    LockRegistry locks;

    @Inject
    @LockRegistryName("strict")
    LockRegistry strict;

    @Inject
    FakeFactory factory;

    @Inject
    LostObserver observer;

    @BeforeEach
    void reset() {
        observer.clear();
    }

    @Test
    void renewsWhileHeldAndReleases() throws Exception {
        DistributedLock lock = locks.obtain("renew");
        lock.lock();
        LockToken first = lock.token().orElseThrow();
        FakeBackend backend = factory.backend(LockRegistryName.DEFAULT);
        int before = backend.renewals.get();
        Thread.sleep(TTL.toMillis() * 3);
        assertThat(backend.renewals.get() - before).isGreaterThanOrEqualTo(5);
        assertThat(lock.isHeldByCurrentThread()).isTrue();
        assertThat(lock.token().orElseThrow().fencing()).isGreaterThan(first.fencing());
        assertThat(lock.token().orElseThrow().acquiredAt()).isEqualTo(first.acquiredAt());
        lock.unlock();
        assertThat(backend.leases).doesNotContainKey("renew");
        assertThat(observer.events()).isEmpty();
    }

    @Test
    void stolenLeaseIsDetected() {
        DistributedLock lock = locks.obtain("stolen");
        lock.lock();
        long fencing = lock.token().orElseThrow().fencing();
        factory.backend(LockRegistryName.DEFAULT).steal("stolen");
        await().pollInSameThread().atMost(Duration.ofSeconds(3)).until(() -> !lock.isHeldByCurrentThread());
        assertThat(lock.token()).isEmpty();
        assertThat(observer.events()).singleElement().satisfies(e -> {
            assertThat(e.registry()).isEqualTo(LockRegistryName.DEFAULT);
            assertThat(e.token().fencing()).isEqualTo(fencing);
            assertThat(e.cause()).isNull();
        });
        int releases = factory.backend(LockRegistryName.DEFAULT).releases.get();
        lock.unlock(); // warns, no exception by default, no remote release of a lost lease
        assertThat(factory.backend(LockRegistryName.DEFAULT).releases.get()).isEqualTo(releases);
        assertThat(factory.backend(LockRegistryName.DEFAULT).leases.get("stolen").ownerId()).isEqualTo("thief");
    }

    @Test
    void unreachableBackendLosesLeaseBeforeExpiry() {
        FakeBackend backend = factory.backend(LockRegistryName.DEFAULT);
        DistributedLock lock = locks.obtain("partition");
        lock.lock();
        try {
            backend.failRenewals = true;
            await().pollInSameThread().atMost(Duration.ofSeconds(3)).until(() -> !lock.isHeldByCurrentThread());
            // declared lost before the backend-side lease (the last successful renewal) expired
            assertThat(observer.lostAt()).isBefore(backend.leases.get("partition").expiresAt());
            assertThat(observer.events()).singleElement().satisfies(e -> assertThat(e.cause()).hasMessage("backend unreachable"));
        } finally {
            backend.failRenewals = false;
            lock.unlock();
        }
    }

    @Test
    void failOnLostLease() {
        strict.obtain("warmup"); // creates the registry and its backend
        assertThatThrownBy(() -> strict.runLocked("strict", () -> {
            factory.backend("strict").steal("strict");
            await().atMost(Duration.ofSeconds(3)).until(() -> !observer.events().isEmpty());
        })).isInstanceOf(LockLostException.class).hasMessageContaining("strict");

        RuntimeException boom = new RuntimeException("task failed");
        assertThatThrownBy(() -> strict.runLocked("strict2", () -> {
            factory.backend("strict").steal("strict2");
            await().atMost(Duration.ofSeconds(3)).until(() -> observer.events().size() == 2);
            throw boom;
        })).isSameAs(boom).satisfies(t -> assertThat(t.getSuppressed()).singleElement().isInstanceOf(LockLostException.class));
    }

    @Test
    void waitsForExpiryOfAnotherOwner() {
        factory.backend(LockRegistryName.DEFAULT).leases.put("foreign",
                new Lease("foreign", "other-jvm", 0, Instant.now().plus(TTL), null));
        long t0 = System.nanoTime();
        assertThat(locks.executeLocked("foreign", Duration.ofSeconds(5), () -> "mine")).isEqualTo("mine");
        assertThat(Duration.ofNanos(System.nanoTime() - t0)).isGreaterThan(TTL.minusMillis(150));
    }
}
