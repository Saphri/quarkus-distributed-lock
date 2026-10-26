# ADR-0002: backend extension modules live under providers/<backend>/{runtime,deployment}

Status: accepted (2026-10-26)
Context: change `add-redis-lock-backend` (design.md, Decision D6)

## Context

The repo kept `nats/{runtime,deployment}` at the root beside `core/`. Each future Lock Backend provider (redis, jdbc, …) would add more root-level module directories, and there was no established home for the next one. The glossary's Lock Backend entry reads "v1: `in-memory` (core) and `nats`. The SPI stays open for future **providers**" — the extensions are providers *of* backends, so `providers/` names the directory without renaming the domain concept.

## Decision

Backend provider extensions move under `providers/<backend>/{runtime,deployment}`: `nats/…` → `providers/nats/…`, and the Redis provider is added as `providers/redis/{runtime,deployment}`. Maven module *paths* change; Maven artifact IDs (`quarkus-distributed-lock-nats[-deployment]`) and Maven `<parent>` references do not, so published-extension compatibility is untouched. `core/` remains at the root: it is not a provider. Tooling docs that reference module paths (AGENTS.md `-pl` examples, README layout, CI) are updated to match; `./mvnw -pl providers/nats/deployment -am …` replaces the old form.

Rejected: `backends/` — the user asked for providers, and provider-of-backend is exactly what these extensions are; renaming the directory to a glossary-avoided term would fight the glossary's own "future providers" phrasing. Keeping root-level per-backend dirs — rejected: grows root clutter with every provider.

## Consequences

+ Adding a third backend (jdbc, …) is a copy/paste of an existing `providers/<backend>` pair — no layout decision each time.
+ Artifact IDs stable → zero impact for applications consuming the published extensions.
− `-pl <path>` invocations, worktree/greps, and docs referring to `nats/…` paths need updating with the move.
− Child poms sit one directory deeper; their `<relativePath>` is `../../../pom.xml`.
