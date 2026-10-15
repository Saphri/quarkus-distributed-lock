package io.quarkiverse.lock.runtime;

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
@ConfigRoot(phase = ConfigPhase.BUILD_AND_RUN_TIME_FIXED)
public interface LockBuildTimeConfig {

    /**
     * Backend type of the in-JVM default.
     */
    String IN_MEMORY = "in-memory";

    /**
     * Lock registries. The unnamed entry ({@code quarkus.lock.*}) configures the default registry and holds the
     * defaults for named ones ({@code quarkus.lock."name".*}).
     */
    @ConfigDocMapKey("registry-name")
    @ConfigDocSection
    @WithParentName
    @WithDefaults
    @WithUnnamedKey(LockRegistryName.DEFAULT)
    Map<String, RegistryBuildTimeConfig> registries();

    default String type(String registry) {
        Optional<String> own = registries().get(registry).type();
        if (own.isPresent()) {
            return own.get();
        }
        return registries().get(LockRegistryName.DEFAULT).type().orElse(IN_MEMORY);
    }

    @ConfigGroup
    interface RegistryBuildTimeConfig {

        /**
         * The lock backend, for example {@code nats}. The backend's extension must be on the classpath. Named registries
         * inherit the default registry's type; the default is {@code in-memory} (locks are local to the JVM).
         */
        Optional<String> type();
    }
}
