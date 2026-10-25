# quarkus-distributed-lock

A Quarkus extension providing mutual exclusion across JVMs through named lock registries with pluggable, lease-based backends. The vocabulary below is canonical: code, issues, tests, and docs should use these terms.

## Language

### Registries and keys

**Lock Registry**:
A named unit binding lock keys to one backend and one set of lease/wait settings. Every lock lives in exactly one registry; the Default Registry serves unqualified use.
_Avoid_: lock manager, namespace, scope, group

**Default Registry**:
The registry unqualified uses resolve to; its name is `default`. Different leases or backends mean different registries, never per-call overrides.
_Avoid_: global lock, implicit registry

**Lock Key**:
A stable string naming one mutually-exclusive region. Identity is declared (config or annotation), never derived from code identity such as method or class names.
_Avoid_: lock name, lock id, method name, class name

**Lock Key Placeholder**:
A `{name}` slot in a Lock Key template, filled from the parameter annotated `@LockKey("name")`. Mismatch between placeholders and parameters is a build failure, not a runtime one.
_Avoid_: format argument, interpolation, implicit key

**Locked Method**:
A method whose invocations enter a Lock Registry lock before the body runs and leave it on every exit path (return, throw, or async termination).
_Avoid_: protected method, annotated method, guarded call

### Holding and releasing

**Lease**:
A backend-held record that an Owner holds a Lock Key, valid for the registry's lease duration and expiring in the backend unless renewed. A lease, not the client, decides when a lock becomes free.
_Avoid_: lock (for the backend record), hold, TTL

**Owner**:
One JVM instance holding a lease, identified in the backend by an Owner ID.
_Avoid_: holder, host, process, thread

**Renewal**:
Extending a live lease before it expires; produces a new, higher fencing token and resets expiry server-side. A renewal failure means Lease Loss.
_Avoid_: extend, refresh, keep-alive

**Lease Loss**:
The state of a lease that expired or was taken by another Owner before its Owner released it. The in-task code keeps running; only fencing makes that safe. Signalled as an observable event (`LockLostEvent`).
_Avoid_: lock expiry, eviction, takeover

**Fencing Token**:
A number per key, strictly increasing across every acquisition and renewal in a registry, passed to the protected resource so it can reject writes from stale owners.
_Avoid_: revision, sequence, counter

**Lock Token**:
The application-facing identification of the lease the current thread holds — its key, Owner, and Fencing Token. The SPI-level word for the backend record itself is Lease.
_Avoid_: using "lease" for what app code sees

**Reentrancy**:
A thread re-acquiring a Lock Key it already holds, without waiting or deadlocking. Blocking acquisitions are reentrant per thread; async acquisitions are not.
_Avoid_: recursion, nesting, self-lock

### Backends

**Lock Backend**:
A provider that keeps at most one lease per Lock Key across all JVMs, chosen per registry by configuration. v1: `in-memory` (core default) and `nats`. The SPI stays open for future providers.
_Avoid_: provider, driver, service, implementation

### Contention

**Wait Timeout**:
How long an acquirer waits for a free Lock Key before losing. A duration of zero means a single attempt ("do it only if free"); there is no wait-forever default.
_Avoid_: TTL (reserved for leases), deadlock timeout, expiry

**Skip**:
The `@Locked` contention outcome in which the method body is not run and the call returns normally; legal only on void-shaped methods. The outcome is a build-time choice per annotation.
_Avoid_: silent failure, optional execution, best-effort

## Resolved synonyms

These words appear loosely in earlier docs; the canonical term is in bold:

- "lock" is overloaded — use **Lock Key** for identity, **Lease** for the backend record, **Locked Method** for the mechanism.
- "holder" / "host" → **Owner** (Owner ID in the backend).
- "extend" / "refresh" → **Renewal**.
- "revision" (NATS-specific) → **Fencing Token** at the domain level; the backend's internal counter stays backend-specific.
