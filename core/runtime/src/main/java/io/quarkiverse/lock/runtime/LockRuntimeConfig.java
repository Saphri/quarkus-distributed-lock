package io.quarkiverse.lock.runtime;

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
public interface LockRuntimeConfig {

    Duration DEFAULT_WAIT_TIMEOUT = Duration.ofSeconds(30);

    /**
     * Identifies this JVM in lock owner ids ({@code <instance-id>:<uuid>}), which backends such as NATS store next to the
     * lock for diagnostics. Defaults to the {@code HOSTNAME} environment variable plus the process id.
     */
    Optional<String> instanceId();

    /**
     * Lock registries. The unnamed entry ({@code quarkus.lock.*}) configures the default registry and holds the
     * defaults for named ones ({@code quarkus.lock."name".*}).
     */
    @ConfigDocMapKey("registry-name")
    @ConfigDocSection
    @WithParentName
    @WithDefaults
    @WithUnnamedKey(LockRegistryName.DEFAULT)
    Map<String, RegistryRuntimeConfig> registries();

    default Duration waitTimeout(String registry) {
        return registries().get(registry).waitTimeout()
                .or(() -> registries().get(LockRegistryName.DEFAULT).waitTimeout())
                .orElse(DEFAULT_WAIT_TIMEOUT);
    }

    default boolean failOnLostLease(String registry) {
        return registries().get(registry).failOnLostLease()
                .or(() -> registries().get(LockRegistryName.DEFAULT).failOnLostLease())
                .orElse(false);
    }

    @ConfigGroup
    interface RegistryRuntimeConfig {

        /**
         * Default wait for {@code executeLocked}, {@code runLocked} and {@code @Locked} without an explicit timeout.
         * {@code 0s} means a single attempt. There is no "wait forever" value; use a large duration. Named registries
         * inherit the default registry's value; the built-in default is 30 seconds.
         */
        Optional<Duration> waitTimeout();

        /**
         * Whether releasing a lock whose lease was lost while it was held throws {@code LockLostException} (after the
         * task has run). When disabled, only a warning is logged. Named registries inherit the default registry's
         * value; the built-in default is {@code false}.
         */
        Optional<Boolean> failOnLostLease();
    }
}
