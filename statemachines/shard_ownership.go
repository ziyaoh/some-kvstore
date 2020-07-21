package statemachines

import "sync"

// ShardOwnership represents the current shards ownership state of a replication group
// This is used by both replication group nodes and shard kv machine, hence thread-safe
type ShardOwnership struct {
	mtx    sync.Mutex
	shards map[int]bool
}

// NewShardOwnership initializes a new ShardOwnership struct
func NewShardOwnership() *ShardOwnership {
	owner := new(ShardOwnership)
	owner.shards = make(map[int]bool)
	return owner
}

// ViewOwnership returns all currently owning shards in a slice
func (owner *ShardOwnership) ViewOwnership() []int {
	owner.mtx.Lock()
	defer owner.mtx.Unlock()

	owning := make([]int, 0)
	for shard := range owner.shards {
		owning = append(owning, shard)
	}
	return owning
}

// Owning checks if a specific shard is owned
func (owner *ShardOwnership) Owning(shard int) bool {
	owner.mtx.Lock()
	defer owner.mtx.Unlock()

	_, exist := owner.shards[shard]
	return exist
}

// Own takes ownership of a shard
func (owner *ShardOwnership) Own(shard int) {
	owner.mtx.Lock()
	defer owner.mtx.Unlock()

	owner.shards[shard] = true
}

// Unown remove a ownership of a shard
func (owner *ShardOwnership) Unown(shard int) {
	owner.mtx.Lock()
	defer owner.mtx.Unlock()

	if _, exist := owner.shards[shard]; exist {
		delete(owner.shards, shard)
	}
}
