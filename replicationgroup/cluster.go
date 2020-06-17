package replicationgroup

import (
	"fmt"
	"math/rand"
	"path/filepath"

	"github.com/ziyaoh/some-kvstore/raft/raft"
	"github.com/ziyaoh/some-kvstore/raft/statemachines"
	"github.com/ziyaoh/some-kvstore/util"
)

// CreateLocalGroup creates a new Raft cluster with the given config in the
// current process.
func CreateLocalGroup(config *raft.Config) ([]*Node, error) {
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
	nodes[0], err = CreateNode(util.OpenPort(0), nil, config, new(statemachines.HashMachine), stableStore)
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
		nodes[i], err = CreateNode(util.OpenPort(0), nodes[0].Self, config, new(statemachines.HashMachine), stableStore)
		if err != nil {
			return nil, err
		}
	}

	return nodes, nil
}

// CreateDefinedLocalCluster creates a new Raft cluster with nodes listening at
// the given ports in the current process.
func CreateDefinedLocalCluster(config *raft.Config, ports []int) ([]*Node, error) {
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
	nodes[0], err = CreateNode(util.OpenPort(ports[0]), nil, config, new(statemachines.HashMachine), stableStore)
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
		nodes[i], err = CreateNode(util.OpenPort(ports[i]), nodes[0].Self, config, new(statemachines.HashMachine), stableStore)
		if err != nil {
			util.Error.Printf("Error creating %v-th node: %v", i, err)
			return nil, err
		}
	}

	return nodes, nil
}
