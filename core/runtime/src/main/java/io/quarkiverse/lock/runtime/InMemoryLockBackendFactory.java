package io.quarkiverse.lock.runtime;

import jakarta.enterprise.context.ApplicationScoped;

import io.quarkiverse.lock.spi.LockBackend;
import io.quarkiverse.lock.spi.LockBackendFactory;

@ApplicationScoped
public class InMemoryLockBackendFactory implements LockBackendFactory {

    @Override
    public String type() {
        return LockBuildTimeConfig.IN_MEMORY;
    }

    @Override
    public LockBackend create(String registryName) {
        return new InMemoryLockBackend();
    }
}
