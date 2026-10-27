# Design Notes

## Overview
'Hi this is Adarsh'

The library is a single `http.Handler` wrapper. It intercepts POST requests carrying an
`Idempotency-Key` header, runs the real handler at most once per key, and replays the
captured response to any subsequent caller presenting the same key.

State machine per key: 

```
(absent) --[first request]--> in-flight --[commit]--> done --[TTL expires]--> (absent)
                                        --[panic ]-> (absent, waiters get 503)
```

---

## Decision 1: retry arrives while first execution is still running

**Choice: block until the original completes, then replay.**

The middleware stores a `chan struct{}` per entry. Subsequent callers with the same key
and fingerprint wait on that channel (respecting their request context deadline). When the
original handler returns, the channel is closed and all waiters unblock to receive the same
captured response.

**Rejected: return 409 Conflict immediately.**
A 409 is a coherent response, but it pushes retry-after logic onto every caller. In
practice, callers retry on 409 just as eagerly as on network errors, adding round-trips
without benefit. Blocking inside the middleware is bounded by the caller's own context
timeout, so goroutine accumulation is naturally capped.

---

## Decision 2: retry has the same key but a different body or route

**Choice: return 422 Unprocessable Entity.**

The fingerprint is `sha256(method || 0x00 || path || 0x00 || body)`. A mismatch with a
stored key is an unambiguous client error — the caller is reusing a key across logically
different operations. The request is rejected immediately; the stored entry is untouched.

Null-byte separators prevent cross-field collisions where two differently-split inputs
produce the same concatenation.

**Rejected: silently ignore the mismatch and replay the stored response.**
Ignoring the mismatch would mask client bugs. A caller that reuses keys incorrectly should
be told immediately so it can fix the error, not receive a quietly wrong answer.

**Rejected: return 409 Conflict.**
409 implies a transient state. 422 is semantically correct: the entity (the re-used key)
cannot be processed because the request is malformed relative to the existing record.

---

## Decision 3: the first execution failed

**Choice: cache and replay all terminal responses, including 5xx.**

Once `ServeHTTP` returns — at any status code — the response is committed and stored.
Retries receive the exact same status and body.

Rationale: the handler ran. We cannot know what side effects occurred before the failure.
Allowing re-execution on 5xx risks double execution (double charge, double insert). The
caller receives a truthful error response and can escalate through out-of-band recovery
rather than by retrying blindly.

The one exception is a handler `panic`. A panic propagates before any response is written,
meaning there is nothing to replay. The entry is abandoned so the next request re-executes.
Concurrent waiters receive a 503 rather than blocking forever.

**Rejected: delete the entry on 5xx and allow re-execution.**
Tempting for "transient" errors, but the library cannot distinguish a transient failure
(DB timeout) from a partial write (charge written, email failed). Erring on the side of
caching preserves safety over convenience.

---

## Decision 4: memory must be bounded

**Choice: fixed entry cap with oldest-committed-first eviction, plus a background TTL sweep.**

`NewStore(max, ttl)` limits the map to `max` entries. When the cap is reached,
`evictLocked` removes an expired-and-done entry first; failing that, the oldest committed
entry. In-flight entries are never evicted — evicting them would strand waiting goroutines
on a channel that never closes.

A background goroutine sweeps every `ttl/2`, deleting expired committed entries to
reclaim memory between natural evictions.

**Rejected: TTL only, no size cap.**
A TTL alone does not bound peak memory. Under load, the map can grow without limit until
the sweep fires. A size cap provides a hard upper bound regardless of sweep timing.

**Rejected: LRU via doubly-linked list.**
An LRU list adds complexity and per-operation overhead. The eviction policy only matters
at capacity, which should be rare in a correctly-sized deployment. Oldest-first is
O(n) only at eviction time, which is acceptable given the alternative complexity cost.

---

## Decision 5: requests without an Idempotency-Key header

**Choice: pass through to the handler unchanged.**

The middleware is opt-in per request. A missing key means the caller has not requested
idempotency guarantees; the request executes normally every time.

**Rejected: return 400 Bad Request.**
Mandating the header would prevent the middleware from wrapping endpoints that serve both
idempotent and non-idempotent clients. The caller is in the best position to decide
whether it needs the guarantee.

---

## What changes for persistence and multi-node

The current implementation is in-memory, single-process. Extending it requires:

1. **Persistent store** (Redis, Postgres): replace `getOrCreate`, `commit`, `abandon` with
   database-backed equivalents. The critical property — atomic "get or insert in-flight" —
   maps to a `SET NX` in Redis or an `INSERT ... ON CONFLICT DO NOTHING` with a
   subsequent `SELECT` in Postgres.

2. **Distributed lock for in-flight**: the channel-based blocking model only works within
   one process. Across nodes, a retry landing on a different node cannot wait on another
   node's goroutine. The practical replacement is: return 409 with a `Retry-After` header
   while the key is in-flight, and let the caller retry. This reintroduces the round-trip
   cost that blocking avoids, but it is unavoidable across a process boundary.

3. **Clock skew for TTL**: a distributed store must use wall-clock TTL (e.g. Redis `EXPIRE`)
   rather than in-process `time.Now()` comparisons, and all nodes must agree on expiry
   times within acceptable skew.
