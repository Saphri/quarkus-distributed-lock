# ADR-0001: Redis lease protocol — server-side expiry + Lua CAS, Redis-server fencing counters

Status: accepted (2026-10-26)
Context: change `add-redis-lock-backend` (openspec/changes/add-redis-lock-backend/design.md, Decisions D1–D3)

## Context

The `redis` Lock Backend must keep at most one Lease per Lock Key across JVMs with server-side expiry, Renewal, Lease Loss, and strictly increasing Fencing Tokens — the same semantics the NATS backend gets from JetStream KV (`create`/`update`/`delete` + bucket `max_age` + revision). Redis offers different primitives: per-key `EXPIRE`, `SET`-if-absent, and atomic Lua scripts. docs/design.md §4 sketched the backend as `SET NX` + Lua compare-and-delete/extend, and §2 ruled Redlock-style multi-server quorums out of scope; this ADR settles the concrete protocol without re-litigating those.

## Decision

Per `redis` registry (namespace prefix `locks` / `locks_<registry>`, mirroring NATS buckets):

- **Lease key** `<prefix><lock key>` holds `{owner, fencing, host, acq}` with a Redis `EXPIRE` at lease duration from the last acquire/renew; expiry is decided by the Redis server, never by client clocks.
- **Acquire / Renew / Release** are one-shot atomic Lua scripts: acquire sets-if-absent and bumps the fence; renew CAS-compares the stored fencing token (the `Lease.backendState()`) then extends expiry and bumps the fence; release CAS-deletes only if the fence still matches ours, otherwise a no-op.
- **Fencing Token** comes from a per-registry Redis hash `<prefix>fences` (Lock Key → counter) bumped with `HINCRBY` inside the same scripts, so tokens stay strictly increasing across expiry and purge.
- **Wait hints**: no push; the backend keeps the core default `awaitReleaseHint` (timed sleep + AcquisitionEngine backoff). RESP3 out-of-band events are per-connection (useless across JVMs), and pub/sub would miss crashed-holder expiry anyway.

Rejected alternatives: client-side expiry timestamps in the value (ShedLock-KV style — clock-skew takeover logic contradicts "a lease, not the client, decides when a lock becomes free"); a Redlock/Spring-style lock library (bypasses the SPI, violates the §2 non-goal); epoch-millis fencing tokens (not strictly increasing); one fence counter key per Lock Key (key sprawl).

## Consequences

+ Crash semantics and clock-skew behavior match the NATS backend and the design doc without client clocks; takeover-after-expiry needs no explicit logic (expired keys are simply absent).
+ Redis expiry is per key, so unlike NATS's bucket `max_age`, registries with different leases may share one Redis server — no "one bucket per lease" constraint.
− Waiters poll instead of watching: release-to-wakeup latency is backoff granularity, and contention adds Redis QPS.
− Scripts must declare every Redis key they touch; the fence hash is a fixed script key, and purging the lock namespace destroys fences (operational guidance: never `FLUSH*` lock namespaces; a bad flush makes tokens younger than expected, not overlapping within a registry).
− Single Redis server per registry; multi-server setups get no quorum (consistent with §2).
