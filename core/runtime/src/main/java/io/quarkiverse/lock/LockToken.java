package io.quarkiverse.lock;

import java.time.Instant;

/**
 * Identifies one lease on a lock.
 *
 * @param key the lock key
 * @param owner the owner id written to the backend ({@code <instanceId>:<uuid>})
 * @param fencing a token that is strictly greater than the token of any earlier lease on the same key in the same
 *        registry. Pass it to the protected resource and have it reject writes carrying a token lower than the highest it
 *        has seen.
 * @param acquiredAt when the lease was acquired, by the local clock
 */
public record LockToken(String key, String owner, long fencing, Instant acquiredAt) {
}
