package shardorchestrator

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"time"

	"github.com/ziyaoh/some-kvstore/raft/raft"
	"github.com/ziyaoh/some-kvstore/statemachines"
	"github.com/ziyaoh/some-kvstore/util"
)

// CreateLocalShardOrchestrator creates a new Raft cluster with the given config in the
// current process.
func CreateLocalShardOrchestrator(numShards int, config *raft.Config) ([]*Node, error) {
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
	kicker := statemachines.NewShardingKicker()
	if err != nil {
		panic(err)
	}
	configMachine := statemachines.NewConfigMachine(numShards, kicker)
	if configMachine == nil {
		panic("configMachine should not be nil")
	}
	nodes[0], err = CreateNode(util.OpenPort(0), nil, config, configMachine, stableStore)
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
		kicker := statemachines.NewShardingKicker()
		if err != nil {
			panic(err)
		}
		configMachine := statemachines.NewConfigMachine(numShards, kicker)
		if configMachine == nil {
			panic("configMachine should not be nil")
		}
		nodes[i], err = CreateNode(util.OpenPort(0), nodes[0].Self, config, configMachine, stableStore)
		if err != nil {
			return nil, err
		}
	}

	return nodes, nil
}

// CreateDefinedLocalShardOrchestrator creates a new Raft cluster with nodes listening at
// the given ports in the current process.
func CreateDefinedLocalShardOrchestrator(numShards int, config *raft.Config, ports []int) ([]*Node, error) {
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
	kicker := statemachines.NewShardingKicker()
	if err != nil {
		panic(err)
	}
	configMachine := statemachines.NewConfigMachine(numShards, kicker)
	if configMachine == nil {
		panic("configMachine should not be nil")
	}
	nodes[0], err = CreateNode(util.OpenPort(ports[0]), nil, config, configMachine, stableStore)
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

		kicker := statemachines.NewShardingKicker()
		configMachine := statemachines.NewConfigMachine(numShards, kicker)
		if configMachine == nil {
			panic("configMachine should not be nil")
		}
		nodes[i], err = CreateNode(util.OpenPort(ports[i]), nodes[0].Self, config, configMachine, stableStore)
		if err != nil {
			util.Error.Printf("Error creating %v-th node: %v", i, err)
			return nil, err
		}
	}

	return nodes, nil
}

// CleanupShardOrchestrator exits each node and removes its logs.
// Given a slice of Nodes representing a replication group,
func CleanupShardOrchestrator(nodes []*Node) {
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
