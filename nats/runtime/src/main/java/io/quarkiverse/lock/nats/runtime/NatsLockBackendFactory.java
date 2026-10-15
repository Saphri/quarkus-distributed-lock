package io.quarkiverse.lock.nats.runtime;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.jboss.logging.Logger;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValueManagement;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.KeyValueStatus;
import io.nats.client.api.StorageType;
import io.quarkiverse.lock.spi.LockBackend;
import io.quarkiverse.lock.spi.LockBackendFactory;
import io.quarkiverse.reactive.messaging.nats.jetstream.client.Client;
import io.smallrye.common.annotation.Identifier;

@ApplicationScoped
public class NatsLockBackendFactory implements LockBackendFactory {

    private static final Logger LOG = Logger.getLogger(NatsLockBackendFactory.class);

    public static final String TYPE = "nats";

    /** JetStream "stream not found". */
    static final int STREAM_NOT_FOUND = 10059;
    /** JetStream "stream name already in use with a different configuration". */
    static final int STREAM_NAME_IN_USE = 10058;

    static final Duration MIN_LEASE = Duration.ofSeconds(1);
    static final Duration RECOMMENDED_MIN_LEASE = Duration.ofSeconds(5);

    @Inject
    NatsLockConfig config;

    @Inject
    @Any
    Instance<Client> clients;

    /** bucket -> lease, so two registries sharing a bucket must agree on the lease. */
    private final Map<String, Duration> bucketLeases = new ConcurrentHashMap<>();

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Set<String> configuredRegistries() {
        return config.registries().keySet();
    }

    @Override
    public LockBackend create(String registry) {
        String bucket = config.bucket(registry);
        Duration lease = config.lease(registry);
        if (lease.compareTo(MIN_LEASE) < 0 || lease.toNanos() % 1_000_000_000L != 0) {
            throw new IllegalArgumentException("Lock registry '" + registry + "': lease must be whole seconds and at least "
                    + MIN_LEASE.toSeconds() + "s, got " + lease);
        }
        if (lease.compareTo(RECOMMENDED_MIN_LEASE) < 0) {
            LOG.warnf("Lock registry '%s' uses a %ds lease; NATS releases crashed holders up to ~0.3s late, so leases"
                    + " under %ds leave little margin", registry, lease.toSeconds(), RECOMMENDED_MIN_LEASE.toSeconds());
        }
        Duration previous = bucketLeases.putIfAbsent(bucket, lease);
        if (previous != null && !previous.equals(lease)) {
            throw new IllegalStateException("Lock registry '" + registry + "' uses bucket '" + bucket + "' with lease "
                    + lease + ", but another registry uses it with lease " + previous
                    + "; the lease is the bucket's max age, so use one bucket per lease");
        }
        String datasource = config.datasource(registry);
        Instance<Client> client = clients.select(Identifier.Literal.of(datasource));
        if (!client.isResolvable()) {
            throw new IllegalStateException("Lock registry '" + registry + "': no NATS datasource '" + datasource
                    + "' (quarkus-messaging-nats-jetstream)");
        }
        Connection connection = client.get().nativeConnection();
        boolean markers = provision(connection, registry, bucket, lease);
        try {
            return new NatsLockBackend(registry, connection.keyValue(bucket), lease, markers);
        } catch (IOException e) {
            throw new NatsLockException("Could not open lock bucket '" + bucket + "'", e);
        }
    }

    /**
     * Adds the bucket if absent and checks that an existing one has the expected lease.
     *
     * @return whether limit markers are enabled (waiters see age-outs)
     */
    boolean provision(Connection connection, String registry, String bucket, Duration lease) {
        try {
            KeyValueManagement kvm = connection.keyValueManagement();
            KeyValueStatus status = status(kvm, bucket);
            if (status == null) {
                boolean markersSupported = connection.getServerInfo().isSameOrNewerThanVersion("2.11.0");
                KeyValueConfiguration.Builder b = KeyValueConfiguration.builder()
                        .name(bucket)
                        .description("Distributed locks of registry '" + registry + "' (lease " + lease.toSeconds() + "s)")
                        .maxHistoryPerKey(1)
                        .ttl(lease)
                        .replicas(config.replicas(registry))
                        .storageType("memory".equalsIgnoreCase(config.storage(registry)) ? StorageType.Memory
                                : StorageType.File);
                if (markersSupported) {
                    b.limitMarker(Duration.ofSeconds(1));
                } else {
                    LOG.infof("NATS server %s does not support limit markers (2.11+); waiters on lock bucket '%s' poll"
                            + " for expired leases", connection.getServerInfo().getVersion(), bucket);
                }
                try {
                    kvm.create(b.build());
                    LOG.infof("Created lock bucket '%s' (lease %ds) for registry '%s'", bucket, lease.toSeconds(), registry);
                } catch (JetStreamApiException e) {
                    if (e.getApiErrorCode() != STREAM_NAME_IN_USE) {
                        throw e;
                    }
                    // created concurrently by another instance
                }
                status = status(kvm, bucket);
                if (status == null) {
                    throw new IllegalStateException("Lock bucket '" + bucket + "' disappeared right after creation");
                }
            }
            if (!lease.equals(status.getTtl())) {
                throw new IllegalStateException("Lock bucket '" + bucket + "' exists with max age " + status.getTtl()
                        + " but registry '" + registry + "' is configured with lease " + lease
                        + "; the bucket's max age is the lease. Change the configuration or recreate the bucket.");
            }
            if (status.getMaxHistoryPerKey() != 1) {
                LOG.warnf("Lock bucket '%s' keeps %d history entries per key; 1 is enough for locks", bucket,
                        status.getMaxHistoryPerKey());
            }
            return status.getLimitMarkerTtl() != null && !status.getLimitMarkerTtl().isZero();
        } catch (IOException | JetStreamApiException e) {
            throw new NatsLockException("Could not provision lock bucket '" + bucket + "': " + e.getMessage(), e);
        }
    }

    private static KeyValueStatus status(KeyValueManagement kvm, String bucket) throws IOException, JetStreamApiException {
        try {
            return kvm.getStatus(bucket);
        } catch (JetStreamApiException e) {
            if (e.getApiErrorCode() == STREAM_NOT_FOUND) {
                return null;
            }
            throw e;
        }
    }
}
