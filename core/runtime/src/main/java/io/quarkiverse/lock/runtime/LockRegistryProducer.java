package io.quarkiverse.lock.runtime;

import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.spi.InjectionPoint;
import jakarta.inject.Singleton;

import io.quarkiverse.lock.LockRegistry;
import io.quarkiverse.lock.LockRegistryName;

@Singleton
public class LockRegistryProducer {

    private final LockRegistries registries;

    public LockRegistryProducer(LockRegistries registries) {
        this.registries = registries;
    }

    @Produces
    @Default
    @Singleton
    LockRegistry defaultRegistry() {
        return registries.getDefault();
    }

    @Produces
    @LockRegistryName("")
    @Dependent
    LockRegistry namedRegistry(InjectionPoint ip) {
        for (var q : ip.getQualifiers()) {
            if (q instanceof LockRegistryName n) {
                return registries.get(n.value());
            }
        }
        return registries.getDefault();
    }
}
