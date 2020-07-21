package replicationgroup

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"

	"github.com/ziyaoh/some-kvstore/rpc"
	"github.com/ziyaoh/some-kvstore/util"

	"github.com/ziyaoh/some-kvstore/raft/raft"
	"github.com/ziyaoh/some-kvstore/shardorchestrator"
	"github.com/ziyaoh/some-kvstore/statemachines"
)

// Returns the leader in a raft cluster, and an error otherwise.
func findLeader(nodes []*Node) (*Node, error) {
	leaders := make([]*Node, 0)
	for _, node := range nodes {
		if node.raft.State == raft.LeaderState {
			leaders = append(leaders, node)
		}
	}

	if len(leaders) == 0 {
		return nil, fmt.Errorf("No leader found in slice of nodes")
	} else if len(leaders) == 1 {
		return leaders[0], nil
	} else {
		return nil, fmt.Errorf("Found too many leaders in slice of nodes: %v", len(leaders))
	}
}

func findFollower(nodes []*Node) (*Node, error) {
	for _, node := range nodes {
		if node.raft.State == raft.FollowerState {
			return node, nil
		}
	}
	return nil, fmt.Errorf("No Follower found in slice of nodes")
}

func findAllFollowers(nodes []*Node) ([]*Node, error) {
	followers := make([]*Node, 0)
	for _, node := range nodes {
		if node.raft.State == raft.FollowerState {
			followers = append(followers, node)
		}
	}

	if len(followers) == 0 {
		return nil, fmt.Errorf("No follower found in slice of nodes")
	}
	return followers, nil
}

func getEmptyShardKV(groupID uint64) *statemachines.ShardKVMachine {
	boltPath := filepath.Join(os.TempDir(), fmt.Sprintf("kvstore_%d", rand.Int()))
	kvstore, err := statemachines.NewKVStoreMachine(boltPath)
	if err != nil {
		panic(err)
	}
	if kvstore == nil {
		panic("kvstore should not be nil")
	}
	trans, err := statemachines.NewTransferer(rand.Uint64())
	if err != nil {
		panic(err)
	}
	shardkv, err := statemachines.NewShardKVMachine(groupID, kvstore, trans)
	if err != nil {
		panic(err)
	}
	return shardkv
}

func getPresetShardKV(groupID uint64) *statemachines.ShardKVMachine {
	shardkv := getEmptyShardKV(groupID)
	for shard := 0; shard < util.NumShards; shard++ {
		shardkv.Shards.Own(shard)
	}
	return shardkv
}

func getDependency(config *raft.Config, orchestrator string, groupID uint64) (*statemachines.ShardKVMachine, raft.StableStore, *shardorchestrator.InternalClient) {
	var stableStore raft.StableStore
	if config.InMemory {
		stableStore = raft.NewMemoryStore()
	} else {
		stableStore = raft.NewBoltStore(filepath.Join(config.LogPath, fmt.Sprintf("raft%d", rand.Int())))
	}
	shardkv := getPresetShardKV(groupID)
	queryer, err := shardorchestrator.InternalClientConnect(orchestrator, groupID)
	if err != nil {
		panic(err)
	}
	return shardkv, stableStore, queryer
}

func getEmptyDependency(config *raft.Config, orchestrator string, groupID uint64) (*statemachines.ShardKVMachine, raft.StableStore, *shardorchestrator.InternalClient) {
	var stableStore raft.StableStore
	if config.InMemory {
		stableStore = raft.NewMemoryStore()
	} else {
		stableStore = raft.NewBoltStore(filepath.Join(config.LogPath, fmt.Sprintf("raft%d", rand.Int())))
	}
	shardkv := getEmptyShardKV(groupID)
	queryer, err := shardorchestrator.InternalClientConnect(orchestrator, groupID)
	if err != nil {
		panic(err)
	}
	return shardkv, stableStore, queryer
}

func getClientRequest(clientid uint64, seq uint64, operation uint64, pair statemachines.KVPair) (*rpc.ClientRequest, error) {
	data, err := util.EncodeMsgPack(pair)
	if err != nil {
		return nil, err
	}

	payloadData := statemachines.KVStoreCommandPayload{
		Command: operation,
		Data:    data,
	}
	payloadBytes, err := util.EncodeMsgPack(payloadData)
	if err != nil {
		return nil, err
	}

	request := rpc.ClientRequest{
		ClientId:        clientid,
		SequenceNum:     seq,
		StateMachineCmd: statemachines.KVStoreCommand,
		Data:            payloadBytes,
	}
	return &request, nil
}

func getMockSO(forGroup uint64) *shardorchestrator.MockSONode {
	orchestrator := shardorchestrator.CreateDefaultMockSONode()
	config := util.NewConfiguration(util.NumShards)
	config.Groups[forGroup] = []string{}
	for i := range config.Location {
		config.Location[i] = forGroup
	}
	orchestrator.CurrentConfig = config

	return orchestrator
}
