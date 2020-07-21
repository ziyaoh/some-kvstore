package replicationgroup

import (
	"time"

	"github.com/ziyaoh/some-kvstore/raft/raft"
	"github.com/ziyaoh/some-kvstore/util"
)

// CreateLocalReplicationGroup creates a new Raft cluster with the given config in the
// current process.
func CreateLocalReplicationGroup(groupID uint64, config *raft.Config, orchestrator string) ([]*Node, error) {
	err := raft.CheckConfig(config)
	if err != nil {
		return nil, err
	}

	nodes := make([]*Node, config.ClusterSize)

	shardkv, stableStore, queryer := getDependency(config, orchestrator, groupID)
	nodes[0], err = CreateNode(groupID, util.OpenPort(0), nil, config, shardkv, stableStore, queryer)
	if err != nil {
		util.Error.Printf("Error creating first node: %v", err)
		return nodes, err
	}

	for i := 1; i < config.ClusterSize; i++ {
		shardkv, stableStore, queryer := getDependency(config, orchestrator, groupID)
		nodes[i], err = CreateNode(groupID, util.OpenPort(0), nodes[0].Self, config, shardkv, stableStore, queryer)
		if err != nil {
			return nil, err
		}
	}

	return nodes, nil
}

// CreateEmptyLocalReplicationGroup creates a new Raft cluster with the given config in the
// current process.
func CreateEmptyLocalReplicationGroup(groupID uint64, config *raft.Config, orchestrator string) ([]*Node, error) {
	err := raft.CheckConfig(config)
	if err != nil {
		return nil, err
	}

	nodes := make([]*Node, config.ClusterSize)

	shardkv, stableStore, queryer := getEmptyDependency(config, orchestrator, groupID)
	nodes[0], err = CreateNode(groupID, util.OpenPort(0), nil, config, shardkv, stableStore, queryer)
	if err != nil {
		util.Error.Printf("Error creating first node: %v", err)
		return nodes, err
	}

	for i := 1; i < config.ClusterSize; i++ {
		shardkv, stableStore, queryer := getEmptyDependency(config, orchestrator, groupID)
		nodes[i], err = CreateNode(groupID, util.OpenPort(0), nodes[0].Self, config, shardkv, stableStore, queryer)
		if err != nil {
			return nil, err
		}
	}

	return nodes, nil
}

// CreateDefinedLocalReplicationGroup creates a new Raft cluster with nodes listening at
// the given ports in the current process.
func CreateDefinedLocalReplicationGroup(config *raft.Config, ports []int, orchestrator string) ([]*Node, error) {
	err := raft.CheckConfig(config)
	if err != nil {
		return nil, err
	}
	nodes := make([]*Node, config.ClusterSize)

	groupID := uint64(1)
	shardkv, stableStore, queryer := getDependency(config, orchestrator, groupID)
	nodes[0], err = CreateNode(groupID, util.OpenPort(ports[0]), nil, config, shardkv, stableStore, queryer)
	if err != nil {
		util.Error.Printf("Error creating first node: %v", err)
		return nodes, err
	}

	for i := 1; i < config.ClusterSize; i++ {
		groupID := uint64(1)
		shardkv, stableStore, queryer := getDependency(config, orchestrator, groupID)
		nodes[0], err = CreateNode(groupID, util.OpenPort(ports[i]), nodes[0].Self, config, shardkv, stableStore, queryer)
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
