package io.quarkiverse.lock;

/**
 * CDI event fired when a held lease is lost (renewal rejected by the backend, or not confirmed before the lease
 * expired). Code still inside the critical section keeps running; only fencing makes that safe. Observe it with
 * {@code void onLost(@Observes LockLostEvent e)}.
 *
 * @param registry the registry name
 * @param token the lease that was lost
 * @param cause the renewal error, or {@code null} if the backend reported the lease as taken
 */
public record LockLostEvent(String registry, LockToken token, Throwable cause) {
}
