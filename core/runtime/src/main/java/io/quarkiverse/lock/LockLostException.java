package io.quarkiverse.lock;

/**
 * Thrown on release when the lease was lost while the lock was held and {@code quarkus.lock.fail-on-lost-lease} is
 * enabled. The protected task has already run to completion when this is thrown.
 */
public class LockLostException extends RuntimeException {

    private final LockToken token;

    public LockLostException(LockToken token) {
        super("Lease on lock '" + token.key() + "' (fencing " + token.fencing() + ") was lost while the lock was held");
        this.token = token;
    }

    public LockToken token() {
        return token;
    }
}
