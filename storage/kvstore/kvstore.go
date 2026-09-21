// Package kvstore implements a minimal in-memory key-value store.
//
// This is the very first storage appliance of AstraKV: a thread-safe map
// supporting Put, Get, and Delete. Later milestones will replace the backing
// structure with a WAL + MemTable + LSM engine while keeping this interface.
package kvstore

import "sync"

// Store is a thread-safe in-memory key-value store.
type Store struct {
	mu     sync.RWMutex
	values map[string]string
}

// New returns an empty Store.
func New() *Store {
	return &Store{values: make(map[string]string)}
}

// Put stores value under key. It replaces any previous value.
func (s *Store) Put(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[key] = value
}

// Get returns the value stored under key. The second result reports whether
// the key was present.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.values[key]
	return value, ok
}

// Delete removes key. It reports whether the key was present.
func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.values[key]; !ok {
		return false
	}
	delete(s.values, key)
	return true
}

// Len returns the number of stored keys.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.values)
}