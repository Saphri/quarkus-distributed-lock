package io.quarkiverse.lock.runtime;

import java.time.Instant;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import io.quarkiverse.lock.spi.Capability;
import io.quarkiverse.lock.spi.Lease;
import io.quarkiverse.lock.spi.LockBackend;

/**
 * The in-JVM default: always grants the lease, so the registry's local lock is the whole lock. Leases never expire;
 * fencing tokens come from a per-registry counter.
 */
public class InMemoryLockBackend implements LockBackend {

    private final AtomicLong fencing = new AtomicLong();

    @Override
    public Optional<Lease> tryAcquire(String key, String ownerId) {
        return Optional.of(new Lease(key, ownerId, fencing.incrementAndGet(), Instant.MAX, null));
    }

    @Override
    public Optional<Lease> renew(Lease lease) {
        return Optional.of(lease);
    }

    @Override
    public void release(Lease lease) {
    }

    @Override
    public Set<Capability> capabilities() {
        return EnumSet.of(Capability.NON_BLOCKING);
    }
}
