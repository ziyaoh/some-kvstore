package raft

import (
	"fmt"
	"math/rand"
	"path/filepath"

	"github.com/ziyaoh/some-kvstore/raft/statemachines"
	"github.com/ziyaoh/some-kvstore/util"
	"google.golang.org/grpc"
)

// CreateLocalCluster creates a new Raft cluster with the given config in the
// current process.
func CreateLocalCluster(config *Config) ([]*Node, error) {
	err := CheckConfig(config)
	if err != nil {
		return nil, err
	}

	nodes := make([]*Node, config.ClusterSize)

	var stableStore StableStore
	if config.InMemory {
		stableStore = NewMemoryStore()
	} else {
		stableStore = NewBoltStore(filepath.Join(config.LogPath, fmt.Sprintf("raft%d", rand.Int())))
	}
	nodes[0], err = CreateNode(util.OpenPort(0), grpc.NewServer(), nil, config, new(statemachines.HashMachine), stableStore)
	if err != nil {
		util.Error.Printf("Error creating first node: %v", err)
		return nodes, err
	}

	for i := 1; i < config.ClusterSize; i++ {
		var stableStore StableStore
		if config.InMemory {
			stableStore = NewMemoryStore()
		} else {
			stableStore = NewBoltStore(filepath.Join(config.LogPath, fmt.Sprintf("raft%d", rand.Int())))
		}
		nodes[i], err = CreateNode(util.OpenPort(0), grpc.NewServer(), nodes[0].Self, config, new(statemachines.HashMachine), stableStore)
		if err != nil {
			return nil, err
		}
	}

	return nodes, nil
}

// CreateDefinedLocalCluster creates a new Raft cluster with nodes listening at
// the given ports in the current process.
func CreateDefinedLocalCluster(config *Config, ports []int) ([]*Node, error) {
	err := CheckConfig(config)
	if err != nil {
		return nil, err
	}
	nodes := make([]*Node, config.ClusterSize)

	var stableStore StableStore
	if config.InMemory {
		stableStore = NewMemoryStore()
	} else {
		stableStore = NewBoltStore(filepath.Join(config.LogPath, fmt.Sprintf("raft%d", ports[0])))
	}
	nodes[0], err = CreateNode(util.OpenPort(ports[0]), grpc.NewServer(), nil, config, new(statemachines.HashMachine), stableStore)
	if err != nil {
		util.Error.Printf("Error creating first node: %v", err)
		return nodes, err
	}

	for i := 1; i < config.ClusterSize; i++ {
		var stableStore StableStore
		if config.InMemory {
			stableStore = NewMemoryStore()
		} else {
			stableStore = NewBoltStore(filepath.Join(config.LogPath, fmt.Sprintf("raft%d", ports[i])))
		}
		nodes[i], err = CreateNode(util.OpenPort(ports[i]), grpc.NewServer(), nodes[0].Self, config, new(statemachines.HashMachine), stableStore)
		if err != nil {
			util.Error.Printf("Error creating %v-th node: %v", i, err)
			return nil, err
		}
	}

	return nodes, nil
}
