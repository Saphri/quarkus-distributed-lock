package io.quarkiverse.lock.spi;

import java.time.Instant;

/**
 * One lease held in a backend.
 *
 * @param key the lock key
 * @param ownerId the owner id the lease was acquired with
 * @param fencing the fencing token: strictly increasing across leases and renewals of a key
 * @param expiresAt when the backend will consider the lease expired unless renewed, by the local clock;
 *        {@link Instant#MAX} for leases that never expire (no renewal is scheduled)
 * @param backendState backend-specific data (for example a revision), opaque to the core
 */
public record Lease(String key, String ownerId, long fencing, Instant expiresAt, Object backendState) {

    public boolean expires() {
        return !Instant.MAX.equals(expiresAt);
    }
}
