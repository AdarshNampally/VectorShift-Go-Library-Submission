package idempotency

import (
	"net/http"
	"sync"
	"time"
)

type entry struct {
	fingerprint string
	status      int
	header      http.Header
	body        []byte
	// ready is closed exactly once: by commit on success, or by abandon on handler panic.
	// Readers must wait on ready before accessing status/header/body.
	ready     chan struct{}
	createdAt time.Time
	expiresAt time.Time
}

// Store holds idempotency records in memory, bounded by capacity and TTL.
type Store struct {
	mu      sync.Mutex
	records map[string]*entry
	max     int
	ttl     time.Duration
}

// NewStore creates a Store capped at max entries, each surviving for ttl.
// A background goroutine sweeps expired records every ttl/2.
func NewStore(max int, ttl time.Duration) *Store {
	s := &Store{
		records: make(map[string]*entry, max),
		max:     max,
		ttl:     ttl,
	}
	go s.sweep()
	return s
}

// getOrCreate atomically retrieves a live entry (existed=true) or inserts a new
// in-flight entry (existed=false). Expired entries are evicted and replaced.
func (s *Store) getOrCreate(key, fp string) (*entry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	if e, ok := s.records[key]; ok {
		if now.Before(e.expiresAt) {
			return e, true
		}
		delete(s.records, key)
	}

	if len(s.records) >= s.max {
		s.evictLocked(now)
	}

	e := &entry{
		fingerprint: fp,
		ready:       make(chan struct{}),
		createdAt:   now,
		expiresAt:   now.Add(s.ttl),
	}
	s.records[key] = e
	return e, false
}

// commit writes the captured response into e and unblocks all waiting goroutines.
// Must be called by exactly the goroutine that received existed=false from getOrCreate.
// No lock is needed: writers finish before close(e.ready), and readers synchronize on it.
func (s *Store) commit(e *entry, status int, header http.Header, body []byte) {
	copied := make([]byte, len(body))
	copy(copied, body)
	e.status = status
	e.header = header
	e.body = copied
	close(e.ready)
}

// abandon removes the in-flight entry and unblocks any waiting goroutines with a 503.
// Called when the handler panics before a response can be captured. If the entry has
// already been committed (e.g. a downstream write panic), it is left intact.
func (s *Store) abandon(key string) {
	s.mu.Lock()
	e, ok := s.records[key]
	if ok && !entryDone(e) {
		delete(s.records, key)
		s.mu.Unlock()
		e.status = http.StatusServiceUnavailable
		e.header = http.Header{"Content-Type": {"text/plain; charset=utf-8"}}
		e.body = []byte("upstream handler panicked; retry the request\n")
		close(e.ready)
		return
	}
	s.mu.Unlock()
}

func (s *Store) evictLocked(now time.Time) {
	for k, e := range s.records {
		if !now.Before(e.expiresAt) && entryDone(e) {
			delete(s.records, k)
			return
		}
	}
	var oldest string
	var oldestAt time.Time
	for k, e := range s.records {
		if entryDone(e) && (oldest == "" || e.createdAt.Before(oldestAt)) {
			oldest = k
			oldestAt = e.createdAt
		}
	}
	if oldest != "" {
		delete(s.records, oldest)
	}
}

func (s *Store) sweep() {
	ticker := time.NewTicker(s.ttl / 2)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		s.mu.Lock()
		for k, e := range s.records {
			if !now.Before(e.expiresAt) && entryDone(e) {
				delete(s.records, k)
			}
		}
		s.mu.Unlock()
	}
}

func entryDone(e *entry) bool {
	select {
	case <-e.ready:
		return true
	default:
		return false
	}
}
