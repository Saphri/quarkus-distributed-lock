# AGENTS.md — quarkus-distributed-lock

Multi-module Maven project (Quarkus extension) providing distributed locks with pluggable backends. Use the Maven wrapper from the repo root; do not use a system Maven install.

## Modules

| Path | Artifact | Role |
|---|---|---|
| `core/runtime` | `quarkus-distributed-lock` | Public API (`LockRegistry`, `DistributedLock`), SPI (`LockBackendFactory`), in-JVM backend |
| `core/deployment` | `quarkus-distributed-lock-deployment` | Build-time validation (`LockProcessor`), CDI wiring, `@Locked` interceptor registration |
| `nats/runtime` | `quarkus-distributed-lock-nats` | NATS JetStream KV backend: leases, renewal, release notifications, fencing via revision |
| `nats/deployment` | `quarkus-distributed-lock-nats-deployment` | Extension wiring for the NATS backend |
| `integration-tests` | `quarkus-distributed-lock-integration-tests` | End-to-end tests (added to the reactor by default via the `it` profile; skipped when `performRelease=true`) |
| `spike/` | standalone, not in the reactor | NATS KV spike (plain jnats + Testcontainers) |

## Commands

- Full build: `./mvnw -B clean install -Dno-format` (CI uses this; needs Docker for dev-services tests)
- Native build (CI): `./mvnw -B install -Dnative -Dquarkus.native.container-build -Dnative.surefire.skip` (needs GraalVM + container build)
- Single test, deployment module: `./mvnw -q -pl core/deployment -am -Dtest=LockedInterceptorTest -Dsurefire.failIfNoSpecifiedTests=false test`
- Single NATS backend test: `./mvnw -q -pl nats/deployment -am -Dtest=NatsLockTest -Dsurefire.failIfNoSpecifiedTests=false test` (needs Docker; dev services spin up a NATS container, image overridable via `-Dnats.image=...`)
- Integration tests: `./mvnw -q -pl integration-tests -am test` (dev services, needs Docker). Native IT: `./mvnw verify -pl integration-tests -Dnative` (needs GraalVM; failsafe runs `LockResourceIT` under the native profile)
- NATS spike: `./mvnw -f spike/pom.xml test` (standalone, needs Docker)

There is no repo-level lint target; validation is the Maven build above.

## Architecture notes

- Public entry point is `LockRegistry` in `core/runtime`: blocking and async execution plus a low-level `obtain(key)` escape hatch returning a `java.util.concurrent.locks.Lock` with `tryLock(Duration)`, `token()` (fencing token while held) and `isHeldByCurrentThread()`.
- Contract: entering acquires or waits until timeout; exiting always releases in `finally`-style cleanup. Blocking acquisitions are reentrant per thread; async acquisitions are not. On timeout, blocking calls throw `LockTimeoutException`; `tryExecuteLocked` returns `Optional.empty()`; `@Locked(onTimeout = SKIP)` skips the method body (void-shaped methods only).
- Blocking calls on an event-loop thread throw — use `executeLockedAsync` or a `Uni`-returning `@Locked` method there.
- Most correctness checks happen at build time in `core/deployment` via `LockProcessor`: registry names (config, `@LockRegistryName`, `@Locked(registry = ...)`), backend type selection, `@Locked` key placeholders vs parameters, return types, and `waitTimeout` duration parsing. Invalid values fail the build, not runtime.
- Backend SPI: extensions implement `LockBackendFactory`; keep lease/renewal/release/wait-notification behavior consistent with `docs/design.md` rather than ad hoc per backend. Do not bypass registry logic in a backend.
- NATS backend (JetStream KV): one bucket per registry, `history=1`, `max_age` = lease; acquire = `create(key)` (revision is the fencing token), renew = `update(key, rev)`, release = `delete(key, rev)`. Server-side expiry only — no client clocks. Waiters wake via bucket watcher plus a 100ms–1s poll backoff. Startup creates the bucket if missing and fails if an existing bucket's max age differs from the configured lease.
- `@Locked` keys are explicit (config or annotation), never derived from method/class names; placeholders must match `@LockKey("name")` parameters at build time. Keep lock keys stable.
- Leases alone do not make a resource safe: pass the fencing token to the protected resource and reject lower tokens. Set `quarkus.lock.fail-on-lost-lease=true` to throw on release after lease loss.

## Conventions

- Prefer explicit registry names (`@LockRegistryName("jobs")`, named config prefixes like `quarkus.lock.jobs.*`) and explicit lock keys over defaults.
- Treat duration parsing and `@Locked` configuration as build-time concerns; do not add runtime fallbacks for invalid values.
- Feature work spans at least two layers: runtime logic + deployment validation/tests. If you change `@Locked` semantics, check `core/deployment` tests (e.g. `InvalidLockedUsageTest`) and the relevant backend module tests too.
- Use JUnit + Quarkus extension test patterns; do not introduce ad hoc harnesses. NATS deployment tests use dev services with an overridable image (`nats:2.12-alpine` default, `-Dnats.image=...`).
- Keep code and docs aligned with the glossary in `GLOSSARY.md` (Lock Registry / Lock Key / Lease / Owner / Renewal / Fencing Token / Wait Timeout / Skip — avoid synonyms like "holder", "extend", "revision" at the domain level).
- ADRs under `docs/adr/` describe planned work (e.g. Redis backend, `providers/<backend>/{runtime,deployment}` layout) that is **not yet applied** — the tree still has `nats/` at root and no Redis provider. Do not follow them as current state; check the actual tree first.
- Domain docs: `GLOSSARY.md` (repo root) and ADRs under `docs/adr/`. Agent workflow docs live in `docs/agents/` (issue tracker via `gh`, triage labels, domain-doc conventions) — see those files before creating issues or applying labels.
