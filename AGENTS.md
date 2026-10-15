# Copilot instructions for quarkus-distributed-lock

## Repository overview

This is a multi-module Maven project for a Quarkus extension that provides distributed locks with pluggable backends.

- `core/runtime`: public API and runtime implementation for lock registry behavior, SPI, and the default in-JVM backend.
- `core/deployment`: build-time validation, extension registration, CDI wiring, and `@Locked` annotation checks.
- `nats/runtime`: NATS JetStream KV backend that implements leasing, renewal, release notifications, and fencing semantics.
- `nats/deployment`: Quarkus extension wiring for the NATS backend.
- `integration-tests`: end-to-end tests for the extension in an application context.
- `docs/`: design notes and the NATS spike/protocol write-up.
- `spike/`: exploratory work for backend experiments.

This project is intentionally built around a backend SPI rather than a single hardcoded implementation. Most behavior is defined in the runtime API, while backend-specific validation and registration live in the deployment modules.

## Build, test, and validation commands

Use the Maven wrapper from the repository root.

- Full CI-style build:
  `./mvnw -B clean install -Dno-format`
- Native build (used in CI):
  `./mvnw -B install -Dnative -Dquarkus.native.container-build -Dnative.surefire.skip`
- Single test in the deployment module (works in this multi-module reactor):
  `./mvnw -q -pl core/deployment -am -Dtest=LockedInterceptorTest -Dsurefire.failIfNoSpecifiedTests=false test`
- Single NATS backend test:
  `./mvnw -q -pl nats/deployment -am -Dtest=NatsLockTest -Dsurefire.failIfNoSpecifiedTests=false test`
- Integration tests:
  `./mvnw -q -pl integration-tests -am test`
- NATS spike:
  `./mvnw -f spike/pom.xml test`

There is no dedicated repo-level lint target configured in the Maven build. The normal validation path is the Maven test/build commands above.

## Architecture and extension model

### Lock registry API

The main public entry point is `LockRegistry` in `core/runtime`. It exposes both blocking and async lock execution patterns and an underlying `DistributedLock` / `java.util.concurrent.locks.Lock` escape hatch.

Key semantics to preserve:

- Entering a lock acquires it or waits until timeout.
- Exiting a lock always releases it in `finally`-style cleanup.
- Lock methods are reentrant per thread.
- Async acquisitions are not reentrant.
- `tryExecuteLocked` returns `Optional.empty()` instead of throwing on timeout.
- Blocking calls must not be used on event-loop threads; use `executeLockedAsync` or a `Uni`-returning `@Locked` method.

### Build-time validation and CDI

Most correctness checks happen in `core/deployment` via `LockProcessor` rather than at runtime.

This module validates:

- registry names discovered from config, `@LockRegistryName`, and `@Locked(registry = ...)`
- backend type selection (`in-memory` vs. provider-specific types)
- `@Locked` key placeholders and parameter matching
- return types and invalid usage patterns
- duration parsing for `waitTimeout`

The `@Locked` interceptor is registered as a CDI bean and is responsible for applying the configured lock around method invocations.

### Backend SPI

Extensions provide lock backends through the `LockBackendFactory` and backend SPI. The default in-memory backend is implemented in the core runtime, while the NATS provider adds its own backend implementation.

When adding or changing a backend:

- implement the backend contract used by `LockRegistry` integration
- preserve lease semantics and timeout behavior
- keep `validateKey`, acquisition, renewal, release, and wait-notification behavior consistent with the design
- avoid bypassing the runtime registry logic with backend-specific ad hoc flow

### NATS backend behavior

The NATS provider is not a special-case bolt-on; it follows the same runtime contract as the in-JVM backend. It uses JetStream KV keys as lock entries, with:

- `create(key)` for acquire
- `update(key, revision)` for renewal
- `delete(key, revision)` for release
- server-side expiry/lease semantics rather than client-side clocks
- fencing token tracking via revision metadata

This is the central architectural concept for the project: lock lifetime is managed by a backend-specific lease implementation, while the public API and annotation behavior remain backend-agnostic.

## Key conventions specific to this repo

- Prefer explicit registry names and explicit lock keys. The project intentionally validates placeholders and key naming at build time.
- Keep lock keys stable; do not rely on method names or class names for identity when a value is available from config or annotation metadata.
- Treat `Duration` parsing and `@Locked` configuration as build-time concerns. Invalid values are expected to fail the build rather than fail later in a runtime call.
- Preserve the “acquire or wait, then release in all exit paths” contract when adding new code. This project is designed around consistent `try/finally` semantics.
- For async code, prefer `Uni`-returning APIs or `executeLockedAsync` instead of blocking on an event-loop thread.
- When making backend changes, reason in terms of the backend SPI and the lease/fencing design in `docs/design.md`, not just the concrete class you are touching.
- Keep code and tests aligned with the extension model: configuration classes, recorder code, and deployment validation are all part of the runtime contract.

## Practical contributor guidance

- Most feature work spans at least two layers: runtime logic and deployment validation. Do not assume a single-file fix is sufficient.
- If a change affects `@Locked` semantics, also check the deployment validation and test coverage in `core/deployment` and the relevant backend module tests.
- For backend-specific work, review `docs/design.md` before changing lease or contention semantics; the design doc captures the intended API and protocol behavior.
- The project already uses JUnit and Quarkus extension test patterns; follow those conventions when adding tests rather than introducing ad hoc test harnesses.
- Use the Maven wrapper (`./mvnw`) rather than a system Maven installation so the project stays on the pinned Quarkus version and wrapper configuration.
