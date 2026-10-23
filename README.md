# Quarkus Distributed Lock

Distributed locks for Quarkus with pluggable backends. It ships an in-JVM default and a NATS JetStream KV backend. The design is in [docs/design.md](docs/design.md), and the NATS spike behind the protocol is in [docs/spike-nats.md](docs/spike-nats.md).

Status: experimental.

## Modules

| Artifact | What it is |
|---|---|
| `io.quarkiverse.lock:quarkus-distributed-lock` | `LockRegistry` API, `@Locked` interceptor, SPI, in-JVM backend (`in-memory`) |
| `io.quarkiverse.lock:quarkus-distributed-lock-nats` | NATS JetStream KV backend (`nats`), on top of `quarkus-nats-jetstream-client` |

## Usage

```java
@Inject LockRegistry locks;

String r = locks.executeLocked("someLockKey", () -> someExclusiveResourceCall());   // default wait from config
locks.runLocked("someLockKey", Duration.ofSeconds(5), () -> doSomething());
Optional<String> maybe = locks.tryExecuteLocked("someLockKey", Duration.ZERO, () -> "ran"); // skip if busy
Uni<String> u = locks.executeLockedAsync("someLockKey", () -> client.callAsync());   // released when the Uni terminates
```

```java
@Locked(value = "invoice-{id}", waitTimeout = "10s")
public void bill(@LockKey("id") long id) { ... }

@Locked(value = "nightly-report", waitTimeout = "0s", onTimeout = OnTimeout.SKIP, registry = "jobs")
@Scheduled(cron = "0 0 2 * * ?")
void report() { ... }
```

Every call follows one rule: entering acquires the lock or waits for it, and exiting (return, throw, or `Uni` termination) releases it.

- On timeout the call throws `LockTimeoutException`. `tryExecuteLocked` returns `Optional.empty()` instead, and `OnTimeout.SKIP` skips the method.
- The task's exceptions are rethrown unchanged. Checked exceptions are wrapped in `LockedExecutionException`.
- Locks are reentrant per thread. Async acquisitions are not reentrant.
- Blocking calls throw on an event-loop thread; use `executeLockedAsync` or a `Uni`-returning `@Locked` method there.
- `@Locked` keys and their `{placeholders}`, `waitTimeout` and `onTimeout` are validated at build time.
- The low-level API is `locks.obtain(key)`, which returns a `java.util.concurrent.locks.Lock` with `tryLock(Duration)`, `token()` and `isHeldByCurrentThread()`.

### Leases and fencing

A backend lock is a lease that the extension renews in the background every third of the lease. If a holder stops renewing, for example because it crashed, the lock frees up after the lease. If a renewal fails, the lease is marked lost: `isHeldByCurrentThread()` turns false and a `LockLostEvent` CDI event fires.

Code already inside the critical section keeps running, so a lease alone does not make a resource safe. Pass `lock.token().get().fencing()` to the protected resource and have it reject writes that carry a lower token than the highest it has seen. Set `quarkus.lock.fail-on-lost-lease=true` to make the release throw `LockLostException`.

## Configuration

```properties
quarkus.lock.type=nats                  # backend of the default registry and default for named ones; in-memory if absent
quarkus.lock.wait-timeout=30s           # default wait (no "forever"; 0s = single attempt)
quarkus.lock.fail-on-lost-lease=false
quarkus.lock.jobs.type=nats             # named registry "jobs"; inherits from quarkus.lock.* when not set
quarkus.lock.jobs.wait-timeout=0s

# NATS backend: quarkus.lock.nats.* for the default registry (and defaults), quarkus.lock."name".nats.* per registry
quarkus.lock.nats.lease=30s             # = bucket max age; whole seconds
quarkus.lock.nats.replicas=3            # when the bucket is created
quarkus.lock.nats.storage=file          # file | memory
quarkus.lock.nats.datasource=default    # quarkus-nats-jetstream-client datasource
quarkus.lock.jobs.nats.bucket=locks_jobs  # defaults: "locks" / "locks_<registry>"
quarkus.lock.jobs.nats.lease=10s
```

Inject a named registry with `@Inject @LockRegistryName("jobs") LockRegistry jobs;`.

## NATS backend

There is one KV bucket per registry, with `history=1` and `max_age` equal to the lease. Limit markers are turned on when the server is NATS 2.11 or newer.

| Operation | KV call |
|---|---|
| Acquire | `create(key)`. The revision is the fencing token. |
| Renew | `update(key, rev)`. This resets the age on the server. |
| Release | `delete(key, rev)` |
| Crash | No renewals, so the server ages the key out. No client clocks are involved. |

Waiters are woken by a bucket watcher on deletes and, with limit markers, on age-outs. They also poll with a 100ms to 1s backoff.

At startup the extension creates the bucket if it is missing. Startup fails if an existing bucket has a different max age than the configured lease.

## Building

```shell
./mvnw install                          # unit and dev-services tests (needs Docker)
./mvnw verify -pl integration-tests -Dnative   # native integration test (needs GraalVM)
./mvnw -f spike/pom.xml test            # the NATS spike
```
