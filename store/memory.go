package store

import (
	"bytes"
	"sort"
	"sync"
)

// MemoryDB supports the same atomic operations as NutsDB for deterministic DAG tests.
type MemoryDB struct {
	mu     sync.RWMutex
	values map[string][]byte
}

func NewMemoryDB() *MemoryDB {
	return &MemoryDB{
		values: make(map[string][]byte),
	}
}

func (m *MemoryDB) Put(key, value []byte) error {
	return m.WriteBatch([]Entry{
		{
			Key:   key,
			Value: value,
		},
	})
}

func (m *MemoryDB) Get(key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	value, ok := m.values[string(key)]
	if !ok {
		return nil, ErrNotFoundKey
	}

	return append([]byte(nil), value...), nil
}

func (m *MemoryDB) WriteBatch(entries []Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, entry := range entries {
		m.values[string(entry.Key)] = append([]byte(nil), entry.Value...)
	}

	return nil
}

func (m *MemoryDB) Scan(prefix []byte) ([]Entry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var entries []Entry

	for key, value := range m.values {
		if bytes.HasPrefix([]byte(key), prefix) {
			entries = append(entries, Entry{[]byte(key), append([]byte(nil), value...)})
		}
	}

	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].Key, entries[j].Key) < 0
	})
	return entries, nil
}
