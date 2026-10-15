# NATS KV lock spike: results

Design §9 step 1. Code: [`spike/`](../spike/src/test/java/io/quarkiverse/lock/spike/NatsKvLockSpikeTest.java), plain jnats 2.26.4 + Testcontainers, memory-storage buckets with `history=1`, `max_age = lease`.

Run: `mvn -f spike/pom.xml test [-Dnats.image=nats:2.10-alpine] [-Dspike.lease=PT10S]`

Servers tested: 2.10.29, 2.11.17, 2.12.15 (all 11 checks pass on each; marker checks skip on 2.10).

## Verdict

The §5.2 protocol works as designed on every server version tested. No design change needed; two details are refined below (marker op type, expiry overshoot).

## Findings

| Question | Result |
|---|---|
| Acquire = `create(key, value)` | Works. On a held key: `JetStreamApiException`, HTTP code 400, **apiErrorCode 10071** ("wrong last sequence"). That code is the "held by someone else" signal for create, update and delete alike. |
| Renew = `update(key, value, rev)` | Resets the age-out clock server-side. Held a 2s lease for 8s with 12 renewals at `lease/3`. A stale revision fails with 10071. |
| Release = `delete(key, rev)` | CAS works; a stale revision fails with 10071 and leaves the new holder's entry untouched. `get()` returns `null` after a delete, and `create` succeeds on top of the delete marker. |
| Renew/release after age-out | Fails with 10071 with and without markers, whether or not someone re-acquired in the meantime. So "renew failed with 10071" = lease lost, unambiguously. |
| Fencing | Revisions are bucket-wide stream sequences: create=1, renew=2, delete marker=3, re-create=4, and keys share the sequence. Strictly monotonic as §5.3 assumes. |
| Crash age-out timing | 2.10: lease + 0–13ms. 2.11/2.12: lease + 0–260ms, with or without markers, the same at 2s and 10s leases. The newer servers check expiry on a ~250ms tick. |
| Age-out under load | 500 live keys written within ~150ms all aged out; probed keys re-acquirable at lease + 2–186ms. No backlog effect at this scale. |
| Watcher, explicit release | `DELETE` event, delivered immediately. |
| Watcher, age-out without markers | Silent: no event at all (as the design expected), so waiters must poll. |
| Watcher, age-out with markers (2.11+) | One event with operation **`PURGE`** (jnats maps the `MaxAge` marker reason to PURGE), delivered at the age-out. A `create` right after the event wins. The marker itself expires after the marker TTL (1s) without touching the new holder's entry. |
| Wildcard watch | One `watch(">")` subscription sees PUT/DELETE for every key in the bucket, so a registry can multiplex all waiters on a single watcher instead of one per key. |
| Bucket config check | `KeyValueStatus.getTtl()` returns the bucket `max_age`, so the "existing bucket has a different lease" startup check in §5.5 is straightforward. `getLimitMarkerTtl()` tells whether markers are on. |

## Consequences for the implementation

- Treat `apiErrorCode == 10071` as contention (acquire) or lost lease (renew/release); everything else (I/O, timeouts, other API errors) is a transient backend error and follows the §5.4 "connection drops" row.
- Waiters wake on `DELETE` and `PURGE` events. With markers on, they still poll as a safety net, but at a slow interval; without markers, poll with the 100ms → 1s backoff.
- Size leases knowing actual release on crash is `lease + ≤ 0.3s` on 2.11+. A 5s minimum lease stays sensible; renew at `lease/3`.
- One wildcard watcher per registry, created lazily on first contention, is enough.
