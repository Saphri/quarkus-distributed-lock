# Quarkus distributed locks with pluggable backends (NATS KV first)

Status: design draft, 2026-10-15. Starting point for implementation; v1 = in-JVM default + NATS JetStream KV provider.

## 1. Is there already something like this?

Short answer: no Quarkus/Quarkiverse extension offers a general-purpose, backend-pluggable `Lock` API. What exists:

| Prior art | What it is | Gap vs. what you want |
|---|---|---|
| **Quarkiverse `quarkus-shedlock`** (dcdh/quarkus-shedlock) | ShedLock port: `@SchedulerLock`-style interceptor, providers as separate extensions (JDBC, Mongo, in-memory). | Lease-only "skip if someone else holds it" semantics aimed at scheduled jobs. No blocking `lock()`, no `tryLock(timeout)`, no reentrancy, no NATS provider. |
| **quarkus-redis-klock** (kekingcn) | `@Klock` method annotation + `@KlockKey` parameters, Redis only (Redisson-flavoured). | Single backend, annotation-only, README documents no TTL/renewal/reentrancy semantics. Not in Quarkiverse. |
| **`io.quarkus.arc.Lock`** | ArC container-managed concurrency (`@Lock(READ/WRITE)`) on a bean. | In-JVM only. Name clash to keep in mind. |
| **Vert.x `SharedData.getLock*`** | Cluster-wide lock when Vert.x is clustered (Hazelcast/Infinispan). | Requires a clustered Vert.x; not a Quarkus-level API, not pluggable. |
| **Spring Integration `LockRegistry`** | `Lock obtain(Object key)`; impls `DefaultLockRegistry` (in-JVM striped `ReentrantLock`), `RedisLockRegistry`, `JdbcLockRegistry`, `ZookeeperLockRegistry`, Hazelcast. Sub-interfaces `ExpirableLockRegistry` (expire unused), `RenewableLockRegistry` (`renewLock(key)`). | The model you want, but Spring. Good API shape to copy. |
| **ShedLock `shedlock-provider-jetstream`** (`NatsJetStreamLockProvider`) | Official ShedLock provider on a NATS KV bucket (`shedlock-locks`, memory storage). Value = 8-byte `lockAtMostUntil` epoch ms. Lock = `create`, or `get` + compare with client clock + `update(revision)` if expired. Unlock = `update` to `lockAtLeastUntil` or plain `delete`. | Same CAS primitives we need, but: expiry by **client clocks**, no `extend`/renewal, no blocking wait, no reentrancy, no fencing exposed, and unlock's final `delete` is not revision-checked (can remove a lock another node just took). Reusable as a reference, not as the backend. |
| **NATS ecosystem** | Go: `entcache/natscache` (stampede lock via KV + LimitMarkerTTL), `addonx/natslock` (leader election, warns of get-then-put race). Python: `3tears-nats` (create-if-absent + heartbeat + TTL). | No Java/Quarkus library found. |

Conclusion: a "Quarkus LockRegistry" with an in-JVM default and a NATS KV provider is a real gap. The NATS acquire primitive (create / CAS update on a KV key) is the same one ShedLock's JetStream provider uses; what this library adds is blocking waits, reentrancy, renewal, server-side expiry and fencing.

## 2. Goals / non-goals

v1 ships two backends: in-JVM (default) and NATS JetStream KV. Other backends are out of scope for v1 but the SPI is designed for them.

Goals
- One API (`LockRegistry` → `java.util.concurrent.locks.Lock`) with an in-JVM default that works with zero config, like Spring.
- Backend chosen by adding an extension + config (`quarkus.lock.type=nats`), mirroring how `quarkus.cache.type` works in your cache extension.
- Imperative and Mutiny (`Uni`) APIs, plus an `@Locked` interceptor for the 80% case.
- Leases with automatic renewal, so a crashed holder never blocks forever.
- Fencing tokens exposed, because leases alone are not safe (§5.4).
- Native-image friendly from day one (you already have the pattern in the cache repo).

Non-goals (v1)
- Fair/FIFO ordering, read/write locks, semaphores, `Condition` support (`newCondition()` throws `UnsupportedOperationException`, as Spring's distributed impls do).
- Strong guarantees beyond what the backend gives (no Redlock-style multi-server quorum).

## 3. Public API (core extension)

Two ways to use a lock, with identical semantics: **enter = acquire or wait, exit (return or throw) = release**.

### 3.1 Programmatic

```java
@Inject LockRegistry locks;

// uses the configured default wait timeout
String r = locks.executeLocked("someLockKey", () -> someExclusiveResourceCall());
locks.runLocked("someLockKey", () -> doSomething());                         // Runnable variant

// explicit wait timeout
locks.executeLocked("someLockKey", Duration.ofSeconds(5), () -> ...);

// "do it only if free": no exception on contention
Optional<String> maybe = locks.tryExecuteLocked("someLockKey", Duration.ZERO, () -> ...);

// reactive (Mutiny): released when the Uni terminates (item, failure or cancellation)
Uni<String> u = locks.executeLockedAsync("someLockKey", () -> client.callAsync());
```

```java
public interface LockRegistry {
    String name();                                                       // LockRegistryName.DEFAULT for the default
    Duration defaultWait();                                              // configured wait-timeout
    int activeKeys();                                                    // keys held or waited for locally
    <T> T executeLocked(String key, Callable<T> task);                  // default wait from config
    <T> T executeLocked(String key, Duration wait, Callable<T> task);
    void runLocked(String key, Runnable task);
    void runLocked(String key, Duration wait, Runnable task);
    <T> Optional<T> tryExecuteLocked(String key, Duration wait, Callable<T> task);
    <T> Uni<T> executeLockedAsync(String key, Supplier<Uni<T>> task);
    <T> Uni<T> executeLockedAsync(String key, Duration wait, Supplier<Uni<T>> task);
    DistributedLock obtain(String key);                                  // low-level escape hatch
}

public interface DistributedLock extends java.util.concurrent.locks.Lock {
    boolean tryLock(Duration wait);
    Optional<LockToken> token();                        // fencing token while held by this thread
    boolean isHeldByCurrentThread();
}

public record LockToken(String key, String owner, long fencing, Instant acquiredAt) {}
```

Rules:
- Timeout while waiting → `LockTimeoutException` (unchecked), except `tryExecuteLocked`, which returns `Optional.empty()`.
- Release happens in `finally`, so it runs on normal return and on any exception; the task's exception is rethrown unchanged (checked exceptions from `Callable` wrapped in `LockedExecutionException`, cause preserved).
- Reentrant per thread: a locked method calling another method locked on the same key does not deadlock.
- If the lease is lost while the task runs (§5.4), the task is not interrupted; the release logs a warning and `LockLostException` is optional via config (`quarkus.lock.fail-on-lost-lease`, default `false`).

### 3.2 Annotation

A CDI interceptor that is literally `registry.executeLocked(key, wait, () -> invocationContext.proceed())`.

```java
@Locked("someLockKey")                                   // name/key required; default wait from config
public Result someExclusiveResourceCall() { ... }

@Locked(value = "invoice-{id}", waitTimeout = "10s")     // per-annotation timeout overrides config
public void bill(@LockKey("id") long id) { ... }

@Locked(value = "nightly-report", waitTimeout = "0s", onTimeout = OnTimeout.SKIP, registry = "jobs")
@Scheduled(cron = "0 0 2 * * ?")
void report() { ... }                                    // ShedLock-like: skip if another node runs it
```

```java
@InterceptorBinding @Target({METHOD, TYPE}) @Retention(RUNTIME)
public @interface Locked {
    @Nonbinding String value();                          // lock key, may contain {param} placeholders
    @Nonbinding String waitTimeout() default "";         // "" = use config; Quarkus duration syntax ("5s", "PT5S")
    @Nonbinding String registry() default "";            // "" = default registry
    @Nonbinding OnTimeout onTimeout() default OnTimeout.THROW;   // THROW | SKIP (SKIP only on void / Uni<Void> methods)
}
```

- Key is required (no implicit `Class#method` default, so keys stay stable across refactors). Placeholders `{name}` are filled from parameters annotated `@LockKey("name")`; validation of placeholders vs. parameters happens **at build time** in the deployment module, so a typo fails the build, not the first call.
- `waitTimeout` is parsed at build time too (invalid duration → build error).
- Return type `Uni`/`CompletionStage` → the interceptor uses `executeLockedAsync`; the lock is released when the async result completes, not when the method returns.
- Usual CDI interceptor caveats apply: self-invocation from the same bean is not intercepted, and private methods aren't intercepted. Document it.

### 3.3 Configuration

```properties
quarkus.lock.type=nats                         # default backend for all registries; in-memory if absent
quarkus.lock.wait-timeout=30s                  # default wait for executeLocked / @Locked without waitTimeout
quarkus.lock.jobs.type=nats                    # named registry override
quarkus.lock.jobs.wait-timeout=0s              # per-registry default wait
quarkus.lock.nats.jobs.bucket=locks_jobs
quarkus.lock.nats.jobs.lease=30s
```

Precedence for the wait timeout: annotation / method argument → `quarkus.lock.<registry>.wait-timeout` → `quarkus.lock.wait-timeout` → built-in default (30s). `Duration.ZERO` means a single attempt. There is deliberately no "wait forever" default; it can be set explicitly with a very large value.

Injection: `@Inject LockRegistry locks;` (default registry) and `@Inject @LockRegistryName("jobs") LockRegistry jobs;`.

## 4. Provider SPI

Split the problem the way Spring's `RedisLockRegistry` does internally: a **JVM-local `ReentrantLock` per key** in front of a **remote lease**. The local lock gives reentrancy and keeps threads in the same JVM from hammering the backend; the provider only deals with "one owner per JVM".

```java
public interface LockBackend {                        // implemented by each provider extension
    Optional<Lease> tryAcquire(String key, String ownerId, Duration lease);   // one attempt, non-blocking
    Optional<Lease> renew(Lease lease);                                        // empty = lost
    void release(Lease lease);                                                 // CAS on lease identity
    Uni<Void> awaitReleaseHint(String key, Duration max);   // optional: wake waiters early (watch/pubsub); default = sleep
    Set<Capability> capabilities();                         // e.g. NON_BLOCKING
}

public record Lease(String key, String ownerId, long fencing, Instant expiresAt, Object backendState) {}
```

The core owns everything generic: local lock map (with idle eviction, like `ExpirableLockRegistry`), wait loop with backoff + jitter, renewal scheduler, lost-lease handling, metrics, the interceptor. A provider is ~200 lines.

Provider selection works like the cache extension: a build step collects `LockBackendFactory` beans by `type()`, and the recorder wires registries from config. No provider on the classpath → `in-memory` (a `LockBackend` that always succeeds, so the local `ReentrantLock` is the whole lock).

**v1 scope: `in-memory` (core) and `nats` only.** The SPI stays open, ShedLock-style, so more providers can be added as separate extensions later, e.g. `redis` (Quarkus Redis client, `SET NX PX` + Lua compare-and-delete/extend) and `jdbc` (row with `SELECT ... FOR UPDATE` or upsert with expiry).

## 5. NATS KV provider

### 5.1 Primitives available (jnats `KeyValue`)

Verified against `nats.java` main:
- `long create(key, value)` and `long create(key, value, MessageTtl)`: succeed only if the key has no live value (absent, or last entry is a delete/purge marker). Returns the new revision.
- `long update(key, value, expectedRevision)`: compare-and-set on revision.
- `void delete(key, expectedRevision)`, `purge(key, expectedRevision[, MessageTtl])`: CAS deletes.
- `watch(key, watcher, ...)`: push notifications of puts/deletes.

Server facts that shape the design (docs.nats.io "TTL and limits", ADR-43/48, server 2.11+):
- Per-key TTL is **set on create only**. A later `put`/`update` writes a new message without the TTL header, so the key stops expiring. That rules out "create with TTL, renew with update" as a lease mechanism.
- Bucket-level `max_age` (the `ttl` your cache extension already sets on buckets) ages each message from its own write time. With `history=1`, an `update` replaces the message, so **an update naturally resets the clock**.
- With limit markers enabled (`LimitMarkerTTL`, 2.11+), age-out and TTL expiry leave a delete marker that watchers see. Without markers, an expired key just disappears silently.

Note on the cache repo: `main` uses bucket `ttl` plus a logical `exp` in the value envelope, but branch `claude/project-thread-48dia5` moves `expiresAfter` to real per-message TTLs, and the lock provider can reuse what it established:
- quarkiverse nats-jetstream **3.40.6+** keeps `limitMarker` when provisioning a bucket (that is what enables `allow_msg_ttl`), and **3.40.7** exposes KeyValue `create`/`purge` with a TTL, so the lock provider can stay on the quarkiverse client like the cache does.
- Limit marker TTL can be the server minimum (1s); TTLs are whole seconds, so lease and wait granularity is 1s.
- Writes are create-only there; the same create-wins rule is the lock's acquire step.

### 5.2 Chosen lock protocol: bucket `max_age` = lease

One bucket per lock registry; the registry's lease duration is the bucket's `max_age`.

```
bucket "locks_orders": history=1, max_age=lease (30s), limit markers on (if server >= 2.11)
key   = encoded lock key (reuse KeyCodec rules: deterministic, <= 255 bytes, valid KV token chars)
value = {"owner":"<instanceId>:<uuid>","acq":<epochMs>,"host":"..."}   (diagnostics only)
```

- **Acquire**: `rev = create(key, value)`. Success → lease with `fencing = rev`. `KeyExistsException`/wrong-last-sequence → held by someone else.
- **Renew** (scheduler at `lease/3`): `rev' = update(key, value, rev)`. Success → new revision, clock reset server-side. Failure → lease lost (someone took it after expiry, or it was deleted): mark lost, fire listener, do not retry blindly.
- **Release**: `delete(key, rev)` (CAS). A failure means we already lost it; log at debug, nothing else.
- **Crash**: no renewals → server ages the message out after `max_age`. No client clocks involved anywhere. This is the main reason to prefer this over a "lease expiry timestamp in the value" design, which needs takeover logic that compares client clocks.
- **Waiting**: `watch(key)` and retry `create` on DELETE/PURGE/marker events; fall back to backoff polling (100ms → 1s, jitter) when markers aren't available, since silent age-out produces no event. Watch once per locally-contended key, not per thread.

Trade-off: lease length is per bucket, not per call. That matches Spring's `RedisLockRegistry(connectionFactory, registryKey, expireAfter)` model, and different leases simply mean different named registries. If per-call leases are needed later, add an opt-in mode: one-shot locks via `create(key, value, MessageTtl.seconds(n))` with no renewal (fine for `executeLocked` with a known upper bound, ShedLock-style `lockAtMostFor`).

Open point to verify in a spike: exact `max_age` granularity/timing of age-out under load (expiry is enforced by the server's periodic check, so actual release is `lease + small delta`). Size `lease` with that in mind and keep a minimum (e.g. 5s).

### 5.3 Fencing

The KV revision is the stream sequence, strictly increasing across the bucket, so every successful `create`/`update` yields a token greater than any earlier holder's. Expose it as `LockToken.fencing()` and document the pattern: pass it to the protected resource, which rejects writes carrying a token lower than the highest it has seen. Note that renewals bump the token, so resources should compare against the token they last saw from *any* holder (monotonic either way).

### 5.4 Failure modes

| Scenario | Behaviour |
|---|---|
| Holder JVM crashes | Key ages out after ≤ lease (+ server check delta). Waiters wake via marker event or poll. |
| Long GC pause / partition longer than lease | Another node acquires. On resume, the renewal CAS fails → lease marked lost, `LockLostListener` fires, `isHeldByCurrentThread()` turns false. Code already inside the critical section keeps running: **only fencing makes this safe**. Document loudly (Kleppmann's argument applies to every lease lock, Redis included). |
| NATS connection drops while held | Renewals fail with I/O errors (not CAS errors). Keep retrying until `expiresAt - safetyMargin`, then declare lost. Don't release locally while the server might still hold it. |
| Server/cluster failover (R3 bucket) | KV writes are RAFT-replicated; a create acknowledged by the leader survives. Recommend `replicas=3` for lock buckets in prod; R1 loses locks on server restart (which only makes them free early). |
| Clock skew between clients | No effect: expiry is server-side only. |
| `unlock()` from wrong thread | `IllegalMonitorStateException`, as `ReentrantLock`. |
| `unlock()` after lease lost | Warn + no-op on the remote side; still release the local lock. |
| Key too long / invalid chars | Fail fast at `obtain()` (same philosophy as the cache's KeyCodec). |
| Server < 2.11 | Works (max_age + CAS exist since 2.x); waiters just poll instead of watching expiry. Detect at startup and log once. |

### 5.5 Bucket provisioning

Same pattern as `NatsCacheInfo.keyValueConfiguration()`: add-if-absent at startup with `history=1`, `ttl=lease`, `replicas` from config, limit markers when the server supports them. If the bucket exists with a different `max_age`, fail startup (silent mismatch would change lease semantics). Reuse `BucketCollisionValidator`-style build-time check so two registries can't share a bucket with different leases.

## 6. Interaction with the in-JVM layer

```
lock():
  localLock.lockInterruptibly()             // reentrancy + intra-JVM queueing
  if holdCount > 1: return                  // reentrant: remote lease already held
  loop until deadline:
     lease = backend.tryAcquire(...)
     if lease: schedule renew; return
     await backend.awaitReleaseHint(key, backoff)
  localLock.unlock(); throw / return false
unlock():
  if holdCount == 1: cancel renew; backend.release(lease)
  localLock.unlock()
```

Renewal runs on a small dedicated scheduler (or Vert.x timers), never on the caller's thread. Blocking `lock()` is fine on worker and virtual threads; on the event loop only the `Uni` API is allowed (detect with `Context.isOnEventLoopThread()` and throw, as Quarkus does elsewhere).

## 7. Observability

- Micrometer (optional): `lock.acquire.time`, `lock.held.time`, `lock.acquire.failures{reason}`, `lock.lost` per registry.
- Health check: backend reachable, bucket present.
- Debug log of owner id on contention (the value's `owner`/`host` fields make `nats kv get` useful for ops).

## 8. Module layout and naming

Suggested repo `quarkus-distributed-lock` (Quarkiverse-style, `io.quarkiverse.lock`):

```
core/{runtime,deployment}        API, in-memory backend, @Locked interceptor, config, SPI
nats/{runtime,deployment}        NATS KV backend (depends on the same quarkiverse nats-jetstream client as the cache)
integration-tests/               dev-services based, JVM + native
```

Dependency note: there was no standalone NATS client extension for Quarkus when v1 was written; the NATS provider (like the cache) then depended on **quarkus-reactive-messaging-nats-jetstream**, which pulls in SmallRye Reactive Messaging. v1 accepted that. The slimmer **quarkus-nats-jetstream-client** extension has since appeared (4.0.0.Beta1), so the `nats` module now depends on that instead: same `Client` CDI bean, connection config, TLS and dev services, without the reactive-messaging overhead. Keep the provider code against jnats' `KeyValue` interface (obtained through the quarkiverse client) so that moving to a slimmer client extension later, if one appears or is split out, touches one class. Only the `nats` module carries this dependency; `core` stays dependency-free.

Avoid the name `@Lock` (clashes with `io.quarkus.arc.Lock`). If this goes to Quarkiverse, propose it on the Quarkiverse discussions first; a maintainer might suggest folding the NATS bit into the Reactive Messaging NATS JetStream extension instead, so it's worth asking before you name things.

ShedLock already ships a JetStream provider (see §1), so a NATS ShedLock provider is not a gap. Optional extra instead: an adapter exposing any `LockRegistry` as a ShedLock `LockProvider`, so quarkus-shedlock users get the server-side expiry and fencing of this design.

## 9. Suggested next steps

1. Spike (one class, plain jnats + Testcontainers NATS 2.11): acquire/renew/release, crash age-out timing, watcher behaviour with and without limit markers.
2. Lock the API shape (§3) and SPI (§4) with the in-memory backend + interceptor.
3. NATS provider + dev-services tests, then native IT.
4. Decide Quarkiverse vs. standalone, then create the repo.

## Sources

- quarkus-redis-klock: https://github.com/kekingcn/quarkus-redis-klock
- Quarkiverse ShedLock: https://docs.quarkiverse.io/quarkus-shedlock/dev/index.html
- jnats `KeyValue` interface: https://github.com/nats-io/nats.java/blob/main/src/main/java/io/nats/client/KeyValue.java
- NATS KV TTL and limits: https://docs.nats.io/learn/key-value/ttl-and-limits
- NATS 2.11 per-message TTL: https://www.synadia.com/blog/per-message-ttl-nats-2-11
- Go prior art: https://pkg.go.dev/github.com/incroy/entcache@v0.5.0/natscache, https://beta.pkg.go.dev/github.com/metal-toolbox/addonx/natslock
- Python prior art: https://pypi.org/project/3tears-nats/
- Spring Integration LockRegistry: https://docs.spring.io/spring-integration/reference/distributed-locks.html

## 10. Implementation notes (v1)

Where the v1 code differs from the sketches above, and why:

- **NATS config is registry-first**: `quarkus.lock.nats.lease` / `quarkus.lock."jobs".nats.lease` instead of `quarkus.lock.nats.jobs.lease`. It follows the `quarkus.datasource."name".jdbc.*` convention, so `nats` can never be mistaken for a registry name, and every backend nests its settings the same way.
- **`LockBackend.tryAcquire(key, ownerId)` has no lease argument**: the lease is backend configuration (for NATS it is the bucket's max age, see §5.2), and the backend reports it through `Lease.expiresAt()`. `LockBackend` also gained `validateKey(key)` (fail fast at `obtain()`) and `close()`; `Capability` adds `NON_BLOCKING` so the in-memory backend skips the worker-thread hop.
- **Lost leases are a CDI event** (`@Observes LockLostEvent`) rather than a listener interface.
- **Async acquisitions are not reentrant** (no thread identity to key on); a `Uni` task that locks the same key again waits for itself until the timeout. Documented on `executeLockedAsync`.
- **Registries are created lazily**, except those referenced at build time (`@LockRegistryName`, `@Locked(registry)`, `quarkus.lock."name".type`) or carrying backend config (`quarkus.lock."name".nats.*`), which are created at startup so bucket/lease mismatches fail fast.
- **App-defined backends**: if the application itself declares a `LockBackendFactory`, an unknown `type` is no longer a build error (the factory's type is only known at runtime); the runtime check still applies.
- Not in v1 yet: Micrometer metrics and the health check of §7.
