package io.quarkiverse.lock.nats.runtime;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import io.quarkiverse.lock.LockRegistryName;
import io.quarkus.runtime.annotations.ConfigDocMapKey;
import io.quarkus.runtime.annotations.ConfigDocSection;
import io.quarkus.runtime.annotations.ConfigGroup;
import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefaults;
import io.smallrye.config.WithParentName;
import io.smallrye.config.WithUnnamedKey;

@ConfigMapping(prefix = "quarkus.lock")
@ConfigRoot(phase = ConfigPhase.RUN_TIME)
public interface NatsLockConfig {

    Duration DEFAULT_LEASE = Duration.ofSeconds(30);
    String DEFAULT_BUCKET = "locks";
    String DEFAULT_DATASOURCE = "default";

    /**
     * NATS settings per lock registry: {@code quarkus.lock.nats.*} for the default registry (and defaults for named
     * ones), {@code quarkus.lock."name".nats.*} for a named registry.
     */
    @ConfigDocMapKey("registry-name")
    @ConfigDocSection
    @WithParentName
    @WithDefaults
    @WithUnnamedKey(LockRegistryName.DEFAULT)
    Map<String, RegistryConfig> registries();

    @ConfigGroup
    interface RegistryConfig {

        /**
         * NATS settings of the registry.
         */
        NatsRegistryConfig nats();
    }

    @ConfigGroup
    interface NatsRegistryConfig {

        /**
         * The KV bucket holding the locks. Defaults to {@code locks} for the default registry and
         * {@code locks_<registry-name>} for named registries. One bucket per lease duration: two registries may share a
         * bucket only with the same lease.
         */
        Optional<String> bucket();

        /**
         * Lease duration, set as the bucket's max age. A holder that stops renewing (crash, partition) loses the lock
         * after this long; renewals run every third of it. Whole seconds, at least 1s; 5s or more is recommended.
         * Named registries inherit the default registry's value; the built-in default is 30s.
         */
        Optional<Duration> lease();

        /**
         * Replicas of the lock bucket when it is created. Use 3 in production clusters. Named registries inherit the
         * default registry's value; the built-in default is 1.
         */
        Optional<Integer> replicas();

        /**
         * Storage of the lock bucket when it is created: {@code file} or {@code memory}. Named registries inherit the
         * default registry's value; the built-in default is {@code file}.
         */
        Optional<String> storage();

        /**
         * The {@code quarkus-nats-jetstream-client} datasource whose connection is used. Named registries inherit
         * the default registry's value; the built-in default is the default datasource.
         */
        Optional<String> datasource();
    }

    default NatsRegistryConfig own(String registry) {
        return registries().get(registry).nats();
    }

    default NatsRegistryConfig defaults() {
        return registries().get(LockRegistryName.DEFAULT).nats();
    }

    default String bucket(String registry) {
        return own(registry).bucket().orElseGet(() -> LockRegistryName.DEFAULT.equals(registry) ? DEFAULT_BUCKET
                : DEFAULT_BUCKET + "_" + registry.replaceAll("[^A-Za-z0-9_-]", "_"));
    }

    default Duration lease(String registry) {
        return own(registry).lease().or(() -> defaults().lease()).orElse(DEFAULT_LEASE);
    }

    default int replicas(String registry) {
        return own(registry).replicas().or(() -> defaults().replicas()).orElse(1);
    }

    default String storage(String registry) {
        return own(registry).storage().or(() -> defaults().storage()).orElse("file");
    }

    default String datasource(String registry) {
        return own(registry).datasource().or(() -> defaults().datasource()).orElse(DEFAULT_DATASOURCE);
    }
}
