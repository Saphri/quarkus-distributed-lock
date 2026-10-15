package io.quarkiverse.lock.spike;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.KeyValueManagement;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.KeyValueOperation;
import io.nats.client.api.KeyValueWatchOption;
import io.nats.client.api.KeyValueWatcher;
import io.nats.client.api.StorageType;

/**
 * Spike for design §9 step 1: the "bucket max_age = lease" protocol of §5.2 on plain jnats.
 * <p>
 * Every test prints its measurements with a {@code [spike]} prefix; the findings are written up in
 * {@code docs/spike-nats.md}. Run with {@code mvn -f spike/pom.xml test [-Dnats.image=nats:2.10-alpine] [-Dspike.lease=PT10S]}.
 */
class NatsKvLockSpikeTest {

    static final String IMAGE = System.getProperty("nats.image", "nats:2.11-alpine");
    static final Duration LEASE = Duration.parse(System.getProperty("spike.lease", "PT2S"));

    static GenericContainer<?> nats;
    static Connection conn;
    static KeyValueManagement kvm;
    static boolean markersSupported;
    static final AtomicInteger BUCKETS = new AtomicInteger();

    @BeforeAll
    static void start() throws Exception {
        nats = new GenericContainer<>(IMAGE).withCommand("-js").withExposedPorts(4222)
                .waitingFor(Wait.forLogMessage(".*Server is ready.*", 1));
        nats.start();
        conn = Nats.connect(Options.builder().server("nats://" + nats.getHost() + ":" + nats.getMappedPort(4222)).build());
        kvm = conn.keyValueManagement();
        String version = conn.getServerInfo().getVersion();
        markersSupported = conn.getServerInfo().isSameOrNewerThanVersion("2.11.0");
        log("server %s (image %s), limit markers supported: %s", version, IMAGE, markersSupported);
    }

    @AfterAll
    static void stop() throws Exception {
        if (conn != null) {
            conn.close();
        }
        if (nats != null) {
            nats.stop();
        }
    }

    static void log(String fmt, Object... args) {
        System.out.println("[spike] " + String.format(fmt, args));
    }

    static KeyValue bucket(boolean markers) throws Exception {
        return bucket(markers, LEASE);
    }

    static KeyValue bucket(boolean markers, Duration lease) throws Exception {
        String name = "locks_spike_" + BUCKETS.incrementAndGet();
        KeyValueConfiguration.Builder b = KeyValueConfiguration.builder().name(name)
                .maxHistoryPerKey(1).ttl(lease).storageType(StorageType.Memory);
        if (markers) {
            b.limitMarker(Duration.ofSeconds(1));
        }
        kvm.create(b.build());
        return conn.keyValue(name);
    }

    static byte[] value(String owner) {
        return ("{\"owner\":\"" + owner + "\",\"acq\":" + System.currentTimeMillis() + "}").getBytes(StandardCharsets.UTF_8);
    }

    /** Polls {@code create} until it wins; returns elapsed millis since {@code since}. */
    static long createWhenFree(KeyValue kv, String key, long sinceNanos, Duration max) throws Exception {
        long deadline = System.nanoTime() + max.toNanos();
        while (System.nanoTime() < deadline) {
            try {
                kv.create(key, value("poller"));
                return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - sinceNanos);
            } catch (JetStreamApiException e) {
                Thread.sleep(10);
            }
        }
        return -1;
    }

    // ---------------------------------------------------------------- primitives

    @Test
    void acquireConflictRenewRelease() throws Exception {
        KeyValue kv = bucket(markersSupported);

        long rev1 = kv.create("orders", value("A"));
        JetStreamApiException conflict = assertThrows(JetStreamApiException.class, () -> kv.create("orders", value("B")));
        log("create on held key -> JetStreamApiException code=%d apiErrorCode=%d desc=%s",
                conflict.getErrorCode(), conflict.getApiErrorCode(), conflict.getErrorDescription());

        long rev2 = kv.update("orders", value("A"), rev1);
        assertTrue(rev2 > rev1);
        JetStreamApiException staleRenew = assertThrows(JetStreamApiException.class, () -> kv.update("orders", value("A"), rev1));
        log("update with stale revision -> code=%d apiErrorCode=%d desc=%s",
                staleRenew.getErrorCode(), staleRenew.getApiErrorCode(), staleRenew.getErrorDescription());

        JetStreamApiException staleRelease = assertThrows(JetStreamApiException.class, () -> kv.delete("orders", rev1));
        log("delete with stale revision -> code=%d apiErrorCode=%d", staleRelease.getErrorCode(),
                staleRelease.getApiErrorCode());
        kv.delete("orders", rev2);
        assertNull(kv.get("orders"), "get() after delete returns null");

        long rev3 = kv.create("orders", value("B"));
        assertTrue(rev3 > rev2, "create after delete marker succeeds and the revision keeps growing");
        log("fencing revisions: create=%d renew=%d (delete marker) re-create=%d", rev1, rev2, rev3);

        // revisions are bucket-wide stream sequences, so they are monotonic across keys too
        long other = kv.create("invoices", value("C"));
        assertTrue(other > rev3);
    }

    @Test
    void renewalKeepsLeaseAliveBeyondMaxAge() throws Exception {
        KeyValue kv = bucket(markersSupported);
        long rev = kv.create("k", value("A"));
        long end = System.nanoTime() + LEASE.multipliedBy(4).toNanos();
        int renewals = 0;
        while (System.nanoTime() < end) {
            Thread.sleep(LEASE.toMillis() / 3);
            rev = kv.update("k", value("A"), rev);
            renewals++;
        }
        KeyValueEntry e = kv.get("k");
        assertNotNull(e);
        assertEquals(rev, e.getRevision());
        log("held %dms (lease %dms) via %d renewals at lease/3; still owned at rev %d",
                LEASE.multipliedBy(4).toMillis(), LEASE.toMillis(), renewals, rev);
    }

    // ---------------------------------------------------------------- crash / age-out

    @Test
    void ageOutTimingWithoutMarkers() throws Exception {
        measureAgeOut(false);
    }

    @Test
    void ageOutTimingWithMarkers() throws Exception {
        if (!markersSupported) {
            log("limit markers not supported by server, skipping");
            return;
        }
        measureAgeOut(true);
    }

    void measureAgeOut(boolean markers) throws Exception {
        KeyValue kv = bucket(markers);
        List<Long> samples = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            String key = "crash" + i;
            kv.create(key, value("crashed-holder"));
            long t0 = System.nanoTime();
            long free = createWhenFree(kv, key, t0, LEASE.multipliedBy(5));
            assertTrue(free > 0, "key must age out");
            samples.add(free);
        }
        Collections.sort(samples);
        log("age-out (markers=%s, lease=%dms): re-acquirable after %s ms (min %d, max %d, overshoot max %d ms)",
                markers, LEASE.toMillis(), samples, samples.get(0), samples.get(samples.size() - 1),
                samples.get(samples.size() - 1) - LEASE.toMillis());
    }

    @Test
    void ageOutUnderLoad() throws Exception {
        KeyValue kv = bucket(markersSupported);
        int n = 500;
        long[] created = new long[n];
        for (int i = 0; i < n; i++) {
            kv.create("load" + i, value("crashed-holder"));
            created[i] = System.nanoTime();
        }
        long writeSpanMs = TimeUnit.NANOSECONDS.toMillis(created[n - 1] - created[0]);
        // poll the first, a middle and the last key concurrently-ish
        int[] probes = { 0, n / 2, n - 1 };
        List<Long> res = new ArrayList<>();
        for (int p : probes) {
            long free = createWhenFree(kv, "load" + p, created[p], LEASE.multipliedBy(5));
            res.add(free);
        }
        log("age-out with %d live keys written over %dms (markers=%s): first/middle/last re-acquirable after %s ms",
                n, writeSpanMs, markersSupported, res);
        long remaining = 0;
        Thread.sleep(LEASE.toMillis());
        for (int i = 0; i < n; i++) {
            KeyValueEntry e = kv.get("load" + i);
            if (e != null && e.getOperation() == KeyValueOperation.PUT && e.getValueAsString().contains("crashed")) {
                remaining++;
            }
        }
        assertEquals(0, remaining, "all crashed holders aged out");
    }

    @Test
    void renewAfterAgeOutFails() throws Exception {
        for (boolean markers : markersSupported ? new boolean[] { false, true } : new boolean[] { false }) {
            KeyValue kv = bucket(markers);
            long rev = kv.create("k", value("A"));
            Thread.sleep(LEASE.toMillis() + 1500);
            JetStreamApiException e = assertThrows(JetStreamApiException.class, () -> kv.update("k", value("A"), rev));
            log("renew after age-out (markers=%s, nobody re-acquired) -> code=%d apiErrorCode=%d",
                    markers, e.getErrorCode(), e.getApiErrorCode());
            // and release after age-out must not delete a newer holder
            long revB = kv.create("k", value("B"));
            assertThrows(JetStreamApiException.class, () -> kv.delete("k", rev));
            assertEquals(revB, kv.get("k").getRevision());
        }
    }

    // ---------------------------------------------------------------- watchers

    static final class Recorder implements KeyValueWatcher {
        final BlockingQueue<KeyValueEntry> events = new LinkedBlockingQueue<>();
        final CountDownLatch endOfData = new CountDownLatch(1);
        final ConcurrentHashMap<Long, Long> receivedAt = new ConcurrentHashMap<>();

        @Override
        public void watch(KeyValueEntry e) {
            receivedAt.put(e.getRevision(), System.nanoTime());
            events.add(e);
        }

        @Override
        public void endOfData() {
            endOfData.countDown();
        }
    }

    @Test
    void watcherSeesExplicitRelease() throws Exception {
        KeyValue kv = bucket(markersSupported);
        Recorder r = new Recorder();
        try (var sub = kv.watch("k", r, KeyValueWatchOption.UPDATES_ONLY)) {
            long rev = kv.create("k", value("A"));
            KeyValueEntry put = r.events.poll(2, TimeUnit.SECONDS);
            assertNotNull(put);
            assertEquals(KeyValueOperation.PUT, put.getOperation());
            long t0 = System.nanoTime();
            kv.delete("k", rev);
            KeyValueEntry del = r.events.poll(2, TimeUnit.SECONDS);
            assertNotNull(del);
            log("explicit release -> watcher op=%s after %dms", del.getOperation(),
                    TimeUnit.NANOSECONDS.toMillis(r.receivedAt.get(del.getRevision()) - t0));
        }
    }

    @Test
    void watcherOnAgeOutWithoutMarkers() throws Exception {
        KeyValue kv = bucket(false);
        Recorder r = new Recorder();
        try (var sub = kv.watch("k", r, KeyValueWatchOption.UPDATES_ONLY)) {
            kv.create("k", value("A"));
            assertNotNull(r.events.poll(2, TimeUnit.SECONDS));
            KeyValueEntry next = r.events.poll(LEASE.toMillis() * 3, TimeUnit.MILLISECONDS);
            log("age-out without markers -> watcher event: %s", next == null ? "none (silent)" : next.getOperation());
            assertNull(next, "silent age-out without limit markers");
        }
    }

    @Test
    void watcherOnAgeOutWithMarkers() throws Exception {
        if (!markersSupported) {
            log("limit markers not supported by server, skipping");
            return;
        }
        KeyValue kv = bucket(true);
        Recorder r = new Recorder();
        try (var sub = kv.watch("k", r, KeyValueWatchOption.UPDATES_ONLY)) {
            kv.create("k", value("A"));
            KeyValueEntry put = r.events.poll(2, TimeUnit.SECONDS);
            assertNotNull(put);
            long t0 = System.nanoTime();
            KeyValueEntry marker = r.events.poll(LEASE.toMillis() * 3, TimeUnit.MILLISECONDS);
            assertNotNull(marker, "limit marker produces a watch event");
            long seen = TimeUnit.NANOSECONDS.toMillis(r.receivedAt.get(marker.getRevision()) - t0);
            // a waiter reacting to the event can acquire right away
            long acquired = kv.create("k", value("B"));
            log("age-out with markers -> watcher op=%s rev=%d %dms after create (lease %dms); create on marker -> rev %d",
                    marker.getOperation(), marker.getRevision(), seen, LEASE.toMillis(), acquired);
            // the marker itself expires after limitMarker TTL; make sure the new holder isn't affected
            Thread.sleep(1500);
            KeyValueEntry e = kv.get("k");
            assertNotNull(e);
            assertEquals(acquired, e.getRevision());
        }
    }

    @Test
    void oneWatcherForManyWaitersIsEnoughAndCheap() throws Exception {
        // One watch subscription on a wildcard sees releases on any key: a registry can multiplex waiters on it.
        KeyValue kv = bucket(markersSupported);
        Recorder r = new Recorder();
        try (var sub = kv.watch(">", r, KeyValueWatchOption.UPDATES_ONLY)) {
            long a = kv.create("a", value("A"));
            long b = kv.create("b", value("A"));
            kv.delete("a", a);
            kv.delete("b", b);
            List<String> ops = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                KeyValueEntry e = r.events.poll(2, TimeUnit.SECONDS);
                assertNotNull(e);
                ops.add(e.getKey() + ":" + e.getOperation());
            }
            log("wildcard watcher events: %s", ops);
        }
    }

    @Test
    void existingBucketWithDifferentMaxAgeIsDetectable() throws Exception {
        KeyValue kv = bucket(false, Duration.ofSeconds(30));
        var status = kvm.getStatus(kv.getBucketName());
        log("bucket status ttl=%s history=%d limitMarkerTtl=%s", status.getTtl(), status.getMaxHistoryPerKey(),
                status.getLimitMarkerTtl());
        assertEquals(Duration.ofSeconds(30), status.getTtl());
    }
}
