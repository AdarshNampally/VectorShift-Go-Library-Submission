# idempotency

A Go middleware library that makes HTTP POST endpoints safe to retry. Wrap a mutating
handler with `New` and any request carrying an `Idempotency-Key` header is guaranteed to
execute the handler at most once, regardless of how many retries arrive.

## Requirements

- Go 1.22 or later
- No external dependencies (stdlib only)

## Installation

```
go get github.com/AdarshNampally/idempotency
```

## Usage

```go
store := idempotency.NewStore(10_000, 24*time.Hour)

mux := http.NewServeMux()
mux.Handle("POST /charge", idempotency.New(store, http.HandlerFunc(chargeHandler)))

http.ListenAndServe(":8080", mux)
```

Callers include an `Idempotency-Key` header with a unique value per logical operation:

```
POST /charge HTTP/1.1
Idempotency-Key: pay-uuid-001
Content-Type: application/json

{"amount": 100}
```

Retrying with the same key and the same request body returns the original response without
re-executing the handler.

## Behaviour

| Scenario | Response |
|---|---|
| No `Idempotency-Key` header | Handler executes every time (pass-through) |
| First request with a key | Handler executes; response is stored |
| Retry with same key and same body | Stored response replayed; handler not called |
| Retry while original is still running | Blocked until original completes, then replayed |
| Retry with same key but different body | `422 Unprocessable Entity` |
| Handler returns 5xx | Error response stored and replayed on retry |
| Handler panics | Entry abandoned; next request re-executes; concurrent waiters receive `503` |
| Entry TTL exceeded | Treated as a new key; handler re-executes |

## Store configuration

```go
// NewStore(maxEntries int, ttl time.Duration)
store := idempotency.NewStore(10_000, 24*time.Hour)
```

`maxEntries` caps the number of records held in memory. When the cap is reached, the
oldest committed entry is evicted. In-flight entries are never evicted.

`ttl` controls how long a committed response is retained. After expiry, the key is treated
as unseen and the next request re-executes the handler.

## Running the tests

```
go test -race ./...
```

## Running the example

```
go run ./example
```

Then in another terminal:

```sh
# Both requests return the same charge_id — handler ran once.
curl -s -X POST -H "Idempotency-Key: pay-001" -d '100' http://localhost:8080/charge
curl -s -X POST -H "Idempotency-Key: pay-001" -d '100' http://localhost:8080/charge

# Different body with same key returns 422.
curl -s -X POST -H "Idempotency-Key: pay-001" -d '999' http://localhost:8080/charge
```

## Design

See [DESIGN.md](DESIGN.md) for the rationale behind each behavioural decision and the
alternatives that were considered and rejected.
