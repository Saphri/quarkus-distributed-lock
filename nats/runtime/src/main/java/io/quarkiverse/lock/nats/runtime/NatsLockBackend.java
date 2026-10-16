package io.quarkiverse.lock.nats.runtime;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.jboss.logging.Logger;

import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.KeyValueOperation;
import io.nats.client.api.KeyValueWatchOption;
import io.nats.client.api.KeyValueWatcher;
import io.nats.client.impl.NatsKeyValueWatchSubscription;
import io.quarkiverse.lock.spi.Capability;
import io.quarkiverse.lock.spi.Lease;
import io.quarkiverse.lock.spi.LockBackend;
import io.smallrye.mutiny.Uni;

/**
 * Locks as keys in a NATS JetStream KV bucket whose max age is the lease (design §5.2).
 * <ul>
 * <li>acquire = {@code create(key)}; the revision is the fencing token</li>
 * <li>renew = {@code update(key, rev)}, which resets the server-side age</li>
 * <li>release = {@code delete(key, rev)}</li>
 * <li>crash = no renewals, the server ages the key out; no client clocks involved</li>
 * </ul>
 * One wildcard watcher per bucket, started on first contention, wakes waiters on deletes and (NATS 2.11+, limit
 * markers on) on age-outs.
 */
public class NatsLockBackend implements LockBackend {

    private static final Logger LOG = Logger.getLogger(NatsLockBackend.class);

    /** JetStream "wrong last sequence": the key is held by someone else, or our revision is stale. */
    static final int WRONG_LAST_SEQUENCE = 10071;

    private final String registry;
    private final KeyValue kv;
    private final Duration lease;
    private final boolean markers;
    private final String host;
    private final ConcurrentHashMap<String, List<CompletableFuture<Void>>> waiters = new ConcurrentHashMap<>();
    private volatile NatsKeyValueWatchSubscription watch;
    private volatile boolean watchFailed;

    public NatsLockBackend(String registry, KeyValue kv, Duration lease, boolean markers) {
        this.registry = registry;
        this.kv = kv;
        this.lease = lease;
        this.markers = markers;
        this.host = Optional.ofNullable(System.getenv("HOSTNAME")).orElse("");
    }

    public String bucket() {
        return kv.getBucketName();
    }

    public Duration lease() {
        return lease;
    }

    public boolean limitMarkers() {
        return markers;
    }

    @Override
    public void validateKey(String key) {
        KeyCodec.encode(key);
    }

    @Override
    public Optional<Lease> tryAcquire(String key, String ownerId) {
        String k = KeyCodec.encode(key);
        Instant start = Instant.now();
        try {
            long rev = kv.create(k, value(ownerId));
            return Optional.of(new Lease(key, ownerId, rev, start.plus(lease), rev));
        } catch (JetStreamApiException e) {
            if (e.getApiErrorCode() == WRONG_LAST_SEQUENCE) {
                return Optional.empty();
            }
            throw new NatsLockException("acquire", key, e);
        } catch (IOException e) {
            throw new NatsLockException("acquire", key, e);
        }
    }

    @Override
    public Optional<Lease> renew(Lease held) {
        String k = KeyCodec.encode(held.key());
        Instant start = Instant.now();
        try {
            long rev = kv.update(k, value(held.ownerId()), (Long) held.backendState());
            return Optional.of(new Lease(held.key(), held.ownerId(), rev, start.plus(lease), rev));
        } catch (JetStreamApiException e) {
            if (e.getApiErrorCode() == WRONG_LAST_SEQUENCE) {
                return Optional.empty();
            }
            throw new NatsLockException("renew", held.key(), e);
        } catch (IOException e) {
            throw new NatsLockException("renew", held.key(), e);
        }
    }

    @Override
    public void release(Lease held) {
        try {
            kv.delete(KeyCodec.encode(held.key()), (Long) held.backendState());
        } catch (JetStreamApiException e) {
            if (e.getApiErrorCode() == WRONG_LAST_SEQUENCE) {
                LOG.debugf("Lock '%s' in registry '%s' was no longer ours at release (revision %s)", held.key(),
                        registry, held.backendState());
                return;
            }
            throw new NatsLockException("release", held.key(), e);
        } catch (IOException e) {
            throw new NatsLockException("release", held.key(), e);
        }
    }

    @Override
    public Uni<Void> awaitReleaseHint(String key, Duration max) {
        if (!ensureWatching()) {
            return LockBackend.super.awaitReleaseHint(key, max);
        }
        String k = KeyCodec.encode(key);
        CompletableFuture<Void> hint = new CompletableFuture<>();
        waiters.computeIfAbsent(k, x -> new CopyOnWriteArrayList<>()).add(hint);
        hint.whenComplete((v, t) -> waiters.computeIfPresent(k, (x, list) -> {
            list.remove(hint);
            return list.isEmpty() ? null : list;
        }));
        return Uni.createFrom().completionStage(hint)
                .ifNoItem().after(max).recoverWithItem((Void) null)
                .onCancellation().invoke(() -> hint.cancel(false))
                .onTermination().invoke(() -> hint.complete(null));
    }

    private boolean ensureWatching() {
        if (watch != null) {
            return true;
        }
        if (watchFailed) {
            return false;
        }
        synchronized (this) {
            if (watch != null) {
                return true;
            }
            try {
                watch = kv.watchAll(new KeyValueWatcher() {
                    @Override
                    public void watch(KeyValueEntry entry) {
                        if (entry.getOperation() == KeyValueOperation.DELETE
                                || entry.getOperation() == KeyValueOperation.PURGE) {
                            List<CompletableFuture<Void>> list = waiters.get(entry.getKey());
                            if (list != null) {
                                list.forEach(f -> f.complete(null));
                            }
                        }
                    }

                    @Override
                    public void endOfData() {
                    }
                }, KeyValueWatchOption.UPDATES_ONLY, KeyValueWatchOption.META_ONLY);
                return true;
            } catch (IOException | JetStreamApiException | RuntimeException e) {
                watchFailed = true;
                LOG.warnf(e, "Could not watch lock bucket '%s'; waiters of registry '%s' will poll", bucket(), registry);
                return false;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }

    @Override
    public Set<Capability> capabilities() {
        return EnumSet.noneOf(Capability.class);
    }

    @Override
    public void close() {
        NatsKeyValueWatchSubscription w = watch;
        if (w != null) {
            try {
                w.unsubscribe();
            } catch (RuntimeException e) {
                LOG.debugf(e, "Error closing watcher of lock bucket '%s'", bucket());
            }
        }
        waiters.values().forEach(l -> l.forEach(f -> f.complete(null)));
    }

    private byte[] value(String ownerId) {
        return ("{\"owner\":\"" + json(ownerId) + "\",\"acq\":" + System.currentTimeMillis() + ",\"host\":\"" + json(host)
                + "\"}").getBytes(StandardCharsets.UTF_8);
    }

    private static String json(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        for (char c : s.toCharArray()) {
            if (c == '"' || c == '\\') {
                sb.append('\\').append(c);
            } else if (c < 0x20) {
                sb.append(String.format("\\u%04x", (int) c));
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
