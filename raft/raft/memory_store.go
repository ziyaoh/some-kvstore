package raft

import (
	"sort"
	"sync"
)

// MemoryStore implements the StableStore interface and serves as a storage option for raft
type MemoryStore struct {
	state   map[string][]byte
	stateMu sync.RWMutex

	logs   map[uint64]*LogEntry
	logsMu sync.RWMutex

	lastLogIndex uint64
}

// NewMemoryStore creates a new in-memory store
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		state:        make(map[string][]byte),
		logs:         make(map[uint64]*LogEntry),
		lastLogIndex: 0,
	}
}

// SetBytes sets a key-value pair
func (store *MemoryStore) SetBytes(key, value []byte) error {
	keyStr := string(key)
	store.stateMu.Lock()
	defer store.stateMu.Unlock()

	store.state[keyStr] = value
	return nil
}

// GetBytes retrives a value for the specified key
func (store *MemoryStore) GetBytes(key []byte) []byte {
	keyStr := string(key)
	store.stateMu.RLock()
	defer store.stateMu.RUnlock()

	return store.state[keyStr]
}

// SetUint64 sets a key-value pair
func (store *MemoryStore) SetUint64(key []byte, term uint64) error {
	keyStr := string(key)
	store.stateMu.Lock()
	defer store.stateMu.Unlock()

	store.state[keyStr] = uint64ToBytes(term)
	return nil
}

// GetUint64 retrieves a uint64 from Bolt
func (store *MemoryStore) GetUint64(key []byte) uint64 {
	keyStr := string(key)
	store.stateMu.RLock()
	defer store.stateMu.RUnlock()

	if v, ok := store.state[keyStr]; ok {
		return bytesToUint64(v)
	}

	return 0
}

// StoreLog grabs the next log index and stores a LogEntry into Bolt
func (store *MemoryStore) StoreLog(log *LogEntry) error {
	store.logsMu.Lock()
	defer store.logsMu.Unlock()

	index := log.GetIndex()
	store.logs[index] = log

	if index > store.lastLogIndex {
		store.lastLogIndex = index
	}

	return nil
}

// GetLog retrieves a LogEntry at a specific log index from Bolt
func (store *MemoryStore) GetLog(index uint64) *LogEntry {
	store.logsMu.RLock()
	defer store.logsMu.RUnlock()

	return store.logs[index]
}

// LastLogIndex gets the last index inserted into Bolt
func (store *MemoryStore) LastLogIndex() uint64 {
	store.logsMu.RLock()
	defer store.logsMu.RUnlock()

	return store.lastLogIndex
}

// TruncateLog deletes all logs starting from index
func (store *MemoryStore) TruncateLog(index uint64) error {
	store.logsMu.Lock()
	defer store.logsMu.Unlock()

	for key := range store.logs {
		if key >= index {
			delete(store.logs, key)
		}
	}

	if index <= store.lastLogIndex {
		store.lastLogIndex = index - 1
	}

	return nil
}

// AllLogs returns all logs in ascending order. Used for testing purposes.
func (store *MemoryStore) AllLogs() []*LogEntry {
	var res []*LogEntry

	for _, log := range store.logs {
		res = append(res, log)
	}

	sort.Slice(res, func(i, j int) bool {
		return res[i].GetIndex() < res[j].GetIndex()
	})

	return res
}

// Close closes the db.
func (store *MemoryStore) Close() {
	// Noop
}

// Remove deletes the db folder
func (store *MemoryStore) Remove() {
	// Noop
}

// Path returns the db path
func (store *MemoryStore) Path() string {
	return ""
}
