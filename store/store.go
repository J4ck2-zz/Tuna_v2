package store

import (
	"context"
	"errors"
	"sync"
)

type DB interface {
	Put([]byte, []byte) error
	Get([]byte) ([]byte, error)
}

type Entry struct {
	Key, Value []byte
}

type BatchDB interface{ WriteBatch([]Entry) error }

type ScanDB interface{ Scan([]byte) ([]Entry, error) }

var ErrNotFoundKey = errors.New("not found key")

var ErrClosed = errors.New("store closed")

// Store serializes backend access and supplies cancellable read notifications.
// Batch-capable backends commit all entries before any waiter is notified.
type Store struct {
	db      DB
	mu      sync.Mutex
	closed  bool
	pending map[string]map[chan struct{}]struct{}
}

func NewStore(db DB) *Store {
	return &Store{
		db:      db,
		pending: make(map[string]map[chan struct{}]struct{}),
	}
}

func (s *Store) Read(key []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}

	value, err := s.db.Get(key)
	return append([]byte(nil), value...), err
}

func (s *Store) Write(key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}

	if err := s.db.Put(key, value); err != nil {
		return err
	}

	s.notify(key)
	return nil
}

func (s *Store) WriteBatch(entries []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}

	backend, ok := s.db.(BatchDB)
	if !ok {
		return errors.New("backend does not support atomic batches")
	}

	if err := backend.WriteBatch(entries); err != nil {
		return err
	}

	for _, entry := range entries {
		s.notify(entry.Key)
	}

	return nil
}

func (s *Store) Scan(prefix []byte) ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}

	backend, ok := s.db.(ScanDB)
	if !ok {
		return nil, errors.New("backend does not support scans")
	}

	return backend.Scan(prefix)
}

func (s *Store) notify(key []byte) {
	for waiter := range s.pending[string(key)] {
		close(waiter)
	}

	delete(s.pending, string(key))
}

func (s *Store) NotifyReadContext(ctx context.Context, key []byte) ([]byte, error) {
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return nil, ErrClosed
		}

		value, err := s.db.Get(key)
		if err == nil {
			result := append([]byte(nil), value...)
			s.mu.Unlock()
			return result, nil
		}

		if !errors.Is(err, ErrNotFoundKey) {
			s.mu.Unlock()
			return nil, err
		}

		waiter := make(chan struct{})
		if s.pending[string(key)] == nil {
			s.pending[string(key)] = make(map[chan struct{}]struct{})
		}

		s.pending[string(key)][waiter] = struct{}{}
		s.mu.Unlock()
		select {
		case <-waiter:
		case <-ctx.Done():
			s.mu.Lock()
			delete(s.pending[string(key)], waiter)
			if len(s.pending[string(key)]) == 0 {
				delete(s.pending, string(key))
			}
			s.mu.Unlock()
			return nil, ctx.Err()
		}
	}
}

func (s *Store) NotifyRead(key []byte) []byte {
	value, _ := s.NotifyReadContext(context.Background(), key)
	return value
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}

	s.closed = true
	for key := range s.pending {
		s.notify([]byte(key))
	}

	if backend, ok := s.db.(interface{ Close() error }); ok {
		return backend.Close()
	}

	return nil
}
