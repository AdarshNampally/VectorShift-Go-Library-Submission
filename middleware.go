package idempotency

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
)

const idempotencyHeader = "Idempotency-Key"

type responseWriter struct {
	header        http.Header
	body          bytes.Buffer
	status        int
	headerWritten bool
}

func newResponseWriter() *responseWriter {
	return &responseWriter{header: make(http.Header), status: http.StatusOK}
}

func (rw *responseWriter) Header() http.Header { return rw.header }

func (rw *responseWriter) WriteHeader(code int) {
	if !rw.headerWritten {
		rw.status = code
		rw.headerWritten = true
	}
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	return rw.body.Write(b)
}

type middleware struct {
	store *Store
	next  http.Handler
}

// New wraps next with idempotency semantics backed by store.
// Requests without an Idempotency-Key header pass through unmodified.
func New(store *Store, next http.Handler) http.Handler {
	return &middleware{store: store, next: next}
}

func (m *middleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get(idempotencyHeader)
	if key == "" {
		m.next.ServeHTTP(w, r)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(body))

	fp := fingerprint(r.Method, r.URL.Path, body)
	e, existed := m.store.getOrCreate(key, fp)

	if existed {
		if e.fingerprint != fp {
			http.Error(w, "idempotency key reused with a different request fingerprint", http.StatusUnprocessableEntity)
			return
		}
		select {
		case <-e.ready:
		case <-r.Context().Done():
			http.Error(w, "request cancelled while awaiting in-flight execution", http.StatusServiceUnavailable)
			return
		}
		replay(w, e)
		return
	}

	rw := newResponseWriter()
	defer func() {
		if rec := recover(); rec != nil {
			m.store.abandon(key)
			panic(rec)
		}
	}()
	m.next.ServeHTTP(rw, r)
	m.store.commit(e, rw.status, rw.header, rw.body.Bytes())
	replay(w, e)
}

func replay(w http.ResponseWriter, e *entry) {
	h := w.Header()
	for k, vals := range e.header {
		for _, v := range vals {
			h.Add(k, v)
		}
	}
	w.WriteHeader(e.status)
	w.Write(e.body)
}

// fingerprint hashes method + path + body with null-byte separators to prevent
// cross-field collisions (e.g. method="POST\x00/a" body="" vs method="POST" path="\x00/a").
func fingerprint(method, path string, body []byte) string {
	h := sha256.New()
	h.Write([]byte(method))
	h.Write([]byte{0})
	h.Write([]byte(path))
	h.Write([]byte{0})
	h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}
