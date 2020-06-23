package replicationgroup

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/ziyaoh/some-kvstore/raft/raft"
	"github.com/ziyaoh/some-kvstore/raft/statemachines"
	"github.com/ziyaoh/some-kvstore/util"
)

// CreateLocalReplicationGroup creates a new Raft cluster with the given config in the
// current process.
func CreateLocalReplicationGroup(config *raft.Config) ([]*Node, error) {
	err := raft.CheckConfig(config)
	if err != nil {
		return nil, err
	}

	nodes := make([]*Node, config.ClusterSize)

	var stableStore raft.StableStore
	if config.InMemory {
		stableStore = raft.NewMemoryStore()
	} else {
		stableStore = raft.NewBoltStore(filepath.Join(config.LogPath, fmt.Sprintf("raft%d", rand.Int())))
	}
	boltPath := filepath.Join(os.TempDir(), fmt.Sprintf("kvstore_%d", rand.Int()))
	kvstore, err := statemachines.NewKVStoreMachine(boltPath)
	if err != nil {
		panic(err)
	}
	if kvstore == nil {
		panic("kvstore should not be nil")
	}
	nodes[0], err = CreateNode(util.OpenPort(0), nil, config, kvstore, stableStore)
	if err != nil {
		util.Error.Printf("Error creating first node: %v", err)
		return nodes, err
	}

	for i := 1; i < config.ClusterSize; i++ {
		var stableStore raft.StableStore
		if config.InMemory {
			stableStore = raft.NewMemoryStore()
		} else {
			stableStore = raft.NewBoltStore(filepath.Join(config.LogPath, fmt.Sprintf("raft%d", rand.Int())))
		}
		boltPath := filepath.Join(os.TempDir(), fmt.Sprintf("kvstore_%d", rand.Int()))
		kvstore, err := statemachines.NewKVStoreMachine(boltPath)
		if err != nil {
			panic(err)
		}
		if kvstore == nil {
			panic("kvstore should not be nil")
		}
		nodes[i], err = CreateNode(util.OpenPort(0), nodes[0].Self, config, kvstore, stableStore)
		if err != nil {
			return nil, err
		}
	}

	return nodes, nil
}

// CreateDefinedLocalReplicationGroup creates a new Raft cluster with nodes listening at
// the given ports in the current process.
func CreateDefinedLocalReplicationGroup(config *raft.Config, ports []int) ([]*Node, error) {
	err := raft.CheckConfig(config)
	if err != nil {
		return nil, err
	}
	nodes := make([]*Node, config.ClusterSize)

	var stableStore raft.StableStore
	if config.InMemory {
		stableStore = raft.NewMemoryStore()
	} else {
		stableStore = raft.NewBoltStore(filepath.Join(config.LogPath, fmt.Sprintf("raft%d", ports[0])))
	}
	boltPath := filepath.Join(os.TempDir(), fmt.Sprintf("kvstore_%d", rand.Int()))
	kvstore, err := statemachines.NewKVStoreMachine(boltPath)
	if err != nil {
		panic(err)
	}
	if kvstore == nil {
		panic("kvstore should not be nil")
	}
	nodes[0], err = CreateNode(util.OpenPort(ports[0]), nil, config, kvstore, stableStore)
	if err != nil {
		util.Error.Printf("Error creating first node: %v", err)
		return nodes, err
	}

	for i := 1; i < config.ClusterSize; i++ {
		var stableStore raft.StableStore
		if config.InMemory {
			stableStore = raft.NewMemoryStore()
		} else {
			stableStore = raft.NewBoltStore(filepath.Join(config.LogPath, fmt.Sprintf("raft%d", ports[i])))
		}
		boltPath := filepath.Join(os.TempDir(), fmt.Sprintf("kvstore_%d", rand.Int()))
		kvstore, err := statemachines.NewKVStoreMachine(boltPath)
		if err != nil {
			panic(err)
		}
		if kvstore == nil {
			panic("kvstore should not be nil")
		}
		nodes[i], err = CreateNode(util.OpenPort(ports[i]), nodes[0].Self, config, kvstore, stableStore)
		if err != nil {
			util.Error.Printf("Error creating %v-th node: %v", i, err)
			return nil, err
		}
	}

	return nodes, nil
}

// CleanupReplicationGroup exits each node and removes its logs.
// Given a slice of Nodes representing a replication group,
func CleanupReplicationGroup(nodes []*Node) {
	for i := 0; i < len(nodes); i++ {
		node := nodes[i]
		node.server.Stop()
		go func(node *Node) {
			node.GracefulExit()
			node.raft.RemoveLogs()
		}(node)
	}
	time.Sleep(5 * time.Second)
}
