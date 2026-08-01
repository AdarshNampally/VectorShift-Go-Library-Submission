package idempotency_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AdarshNampally/idempotency"
)

func handler(calls *int64, status int, body string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls != nil {
			atomic.AddInt64(calls, 1)
		}
		w.WriteHeader(status)
		w.Write([]byte(body))
	})
}

func request(mid http.Handler, key, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/charge", strings.NewReader(body))
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	w := httptest.NewRecorder()
	mid.ServeHTTP(w, r)
	return w
}

func TestNoKey(t *testing.T) {
	var calls int64
	mid := idempotency.New(idempotency.NewStore(10, time.Minute), handler(&calls, http.StatusOK, "ok"))

	request(mid, "", "body")
	request(mid, "", "body")

	if calls != 2 {
		t.Fatalf("want 2 handler calls, got %d", calls)
	}
}

func TestFirstRequest(t *testing.T) {
	var calls int64
	mid := idempotency.New(idempotency.NewStore(10, time.Minute), handler(&calls, http.StatusCreated, "created"))

	w := request(mid, "k1", "body")

	if calls != 1 {
		t.Fatalf("want 1 handler call, got %d", calls)
	}
	if w.Code != http.StatusCreated {
		t.Fatalf("want %d, got %d", http.StatusCreated, w.Code)
	}
	if w.Body.String() != "created" {
		t.Fatalf("want %q, got %q", "created", w.Body.String())
	}
}

func TestReplay(t *testing.T) {
	var calls int64
	mid := idempotency.New(idempotency.NewStore(10, time.Minute), handler(&calls, http.StatusCreated, "created"))

	w1 := request(mid, "k1", "body")
	w2 := request(mid, "k1", "body")

	if calls != 1 {
		t.Fatalf("want 1 handler call, got %d", calls)
	}
	if w1.Code != w2.Code || w1.Body.String() != w2.Body.String() {
		t.Fatalf("replayed response does not match original: %d/%q vs %d/%q",
			w1.Code, w1.Body.String(), w2.Code, w2.Body.String())
	}
}

func TestFingerprintMismatch(t *testing.T) {
	mid := idempotency.New(idempotency.NewStore(10, time.Minute), handler(nil, http.StatusOK, "ok"))

	request(mid, "k1", "body-a")
	w := request(mid, "k1", "body-b")

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("want %d, got %d", http.StatusUnprocessableEntity, w.Code)
	}
}

func TestInFlight(t *testing.T) {
	var calls int64
	started := make(chan struct{})
	unblock := make(chan struct{})

	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&calls, 1)
		close(started)
		<-unblock
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("result"))
	})

	mid := idempotency.New(idempotency.NewStore(10, time.Minute), h)

	makeReq := func() *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/charge", strings.NewReader("body"))
		r.Header.Set("Idempotency-Key", "k1")
		w := httptest.NewRecorder()
		mid.ServeHTTP(w, r)
		return w
	}

	results := make(chan *httptest.ResponseRecorder, 2)
	go func() { results <- makeReq() }()

	<-started
	go func() { results <- makeReq() }()

	time.Sleep(10 * time.Millisecond)
	close(unblock)

	r1, r2 := <-results, <-results

	if atomic.LoadInt64(&calls) != 1 {
		t.Fatalf("want 1 handler call, got %d", calls)
	}
	for _, rec := range []*httptest.ResponseRecorder{r1, r2} {
		if rec.Code != http.StatusCreated {
			t.Errorf("want %d, got %d", http.StatusCreated, rec.Code)
		}
		if rec.Body.String() != "result" {
			t.Errorf("want %q, got %q", "result", rec.Body.String())
		}
	}
}

func TestHandlerErrorReplayed(t *testing.T) {
	var calls int64
	mid := idempotency.New(idempotency.NewStore(10, time.Minute), handler(&calls, http.StatusInternalServerError, "oops"))

	w1 := request(mid, "k1", "body")
	w2 := request(mid, "k1", "body")

	if calls != 1 {
		t.Fatalf("want 1 handler call, got %d", calls)
	}
	if w1.Code != http.StatusInternalServerError || w2.Code != http.StatusInternalServerError {
		t.Fatalf("want 500 for both, got %d and %d", w1.Code, w2.Code)
	}
	if w1.Body.String() != w2.Body.String() {
		t.Fatalf("error response not replayed identically")
	}
}

func TestPanicAbandonsThenAllowsReexecution(t *testing.T) {
	var calls int64
	first := true

	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&calls, 1)
		if first {
			first = false
			panic("boom")
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	mid := idempotency.New(idempotency.NewStore(10, time.Minute), h)

	caught := func() (code int) {
		defer func() {
			if rec := recover(); rec != nil {
				code = -1
			}
		}()
		return request(mid, "k1", "body").Code
	}

	if got := caught(); got != -1 {
		t.Fatalf("expected panic to propagate, got %d", got)
	}

	w := request(mid, "k1", "body")
	if w.Code != http.StatusOK {
		t.Fatalf("want %d after re-execution, got %d", http.StatusOK, w.Code)
	}
	if atomic.LoadInt64(&calls) != 2 {
		t.Fatalf("want 2 handler calls, got %d", calls)
	}
}

func TestExpiry(t *testing.T) {
	var calls int64
	mid := idempotency.New(idempotency.NewStore(10, 50*time.Millisecond), handler(&calls, http.StatusOK, "ok"))

	request(mid, "k1", "body")
	time.Sleep(100 * time.Millisecond)
	request(mid, "k1", "body")

	if calls != 2 {
		t.Fatalf("want 2 handler calls after TTL expiry, got %d", calls)
	}
}

func TestBoundedCapacity(t *testing.T) {
	store := idempotency.NewStore(2, time.Hour)
	mid := idempotency.New(store, handler(nil, http.StatusOK, "ok"))

	request(mid, "k1", "body")
	request(mid, "k2", "body")
	request(mid, "k3", "body")
}

func TestConcurrentSameKey(t *testing.T) {
	const goroutines = 30
	var calls int64

	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&calls, 1)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	mid := idempotency.New(idempotency.NewStore(100, time.Minute), h)

	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := request(mid, "shared", "body")
			if w.Code != http.StatusOK {
				t.Errorf("want %d, got %d", http.StatusOK, w.Code)
			}
			if w.Body.String() != "ok" {
				t.Errorf("want %q, got %q", "ok", w.Body.String())
			}
		}()
	}
	wg.Wait()

	if atomic.LoadInt64(&calls) != 1 {
		t.Fatalf("want 1 handler call, got %d", calls)
	}
}

func TestDifferentKeysAreIndependent(t *testing.T) {
	var calls int64
	mid := idempotency.New(idempotency.NewStore(10, time.Minute), handler(&calls, http.StatusOK, "ok"))

	request(mid, "k1", "body")
	request(mid, "k2", "body")
	request(mid, "k1", "body")
	request(mid, "k2", "body")

	if calls != 2 {
		t.Fatalf("want 2 handler calls (one per key), got %d", calls)
	}
}

func TestHeadersReplayed(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Transaction-ID", "txn-42")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"id":42}`))
	})

	mid := idempotency.New(idempotency.NewStore(10, time.Minute), h)

	request(mid, "k1", "body")
	w := request(mid, "k1", "body")

	if w.Header().Get("X-Transaction-ID") != "txn-42" {
		t.Fatalf("response header not replayed")
	}
	if w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("content-type header not replayed")
	}
}
