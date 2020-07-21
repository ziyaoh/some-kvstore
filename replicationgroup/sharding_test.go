package replicationgroup

import (
	"testing"
	"time"

	"github.com/ziyaoh/some-kvstore/rpc"
	"github.com/ziyaoh/some-kvstore/shardorchestrator"
	"github.com/ziyaoh/some-kvstore/statemachines"

	"github.com/stretchr/testify/assert"

	"github.com/ziyaoh/some-kvstore/raft/raft"
	"github.com/ziyaoh/some-kvstore/util"
)

func TestReplicaGroupShardInAndOut(t *testing.T) {
	// util.SuppressLoggers()

	groupID := uint64(1)
	orchestrator := shardorchestrator.CreateDefaultMockSONode()
	defer orchestrator.GracefulExit()

	nodes, err := CreateEmptyLocalReplicationGroup(groupID, raft.DefaultConfig(), orchestrator.Self.Addr)
	if err != nil {
		t.Errorf("Create local replication group failed: %v\n", err)
	}
	defer CleanupReplicationGroup(nodes)
	time.Sleep(500 * time.Millisecond)

	leader, err := findLeader(nodes)
	if err != nil {
		t.Fatalf("find leader from local replication group fail: %v\n", err)
	}
	addrs := make([]string, 0)
	for _, peer := range leader.raft.Peers {
		addrs = append(addrs, peer.Addr)
	}

	key := []byte("key")
	value := []byte("value")
	pair := statemachines.KVPair{
		Key:   key,
		Value: value,
	}
	shard := util.KeyToShard(key)
	shard2 := (shard + 1) % util.NumShards
	shard3 := (shard + 2) % util.NumShards

	t.Run("check initial empty ownership", func(t *testing.T) {
		initialShards := leader.shardOwnership.ViewOwnership()
		assert.Equal(t, 0, len(initialShards))
	})

	t.Run("initial shard kicking in", func(t *testing.T) {
		config := orchestrator.CurrentConfig.NextConfig()
		config.Groups[groupID] = addrs
		for i := range config.Location {
			config.Location[i] = groupID
		}
		orchestrator.CurrentConfig = config
		time.Sleep(500 * time.Millisecond)

		sharddata := make(map[int][]statemachines.KVPair)
		for shard := 0; shard < util.NumShards; shard++ {
			sharddata[shard] = nil
		}
		payload := statemachines.ShardInPayload{
			ConfigVersion: config.Version,
			Data:          sharddata,
		}
		data, err := util.EncodeMsgPack(payload)
		assert.Nil(t, err)
		request := rpc.ClientRequest{
			ClientId:        uint64(0),
			SequenceNum:     uint64(0),
			StateMachineCmd: statemachines.ShardIn,
			Data:            data,
		}
		leader.ClientRequest(&request)

		shards := leader.shardOwnership.ViewOwnership()
		assert.Equal(t, util.NumShards, len(shards))
	})

	t.Run("test shard out", func(t *testing.T) {
		request, err := getClientRequest(uint64(0), uint64(0), statemachines.KVStorePut, pair)
		assert.Nil(t, err)
		blah := leader.ClientRequest(request)
		assert.Equal(t, rpc.ClientStatus_OK, blah.Status)

		newGroup2 := uint64(2)
		newGroup3 := uint64(3)
		config := orchestrator.CurrentConfig.NextConfig()
		config.Groups[newGroup2] = []string{"2.2.2.2"}
		config.Groups[newGroup3] = []string{"3.3.3.3"}
		config.Location[shard] = newGroup2
		config.Location[shard2] = newGroup2
		config.Location[shard3] = newGroup3
		orchestrator.CurrentConfig = config
		time.Sleep(1 * time.Second)

		assert.False(t, leader.shardOwnership.Owning(shard))
		assert.False(t, leader.shardOwnership.Owning(shard2))
		assert.False(t, leader.shardOwnership.Owning(shard3))

		getRequest, err := getClientRequest(uint64(0), uint64(1), statemachines.KVStoreGet, pair)
		assert.Nil(t, err)
		res := leader.ClientRequest(getRequest)
		assert.Equal(t, rpc.ClientStatus_REQ_FAILED, res.Status)
	})

	t.Run("test shard in", func(t *testing.T) {
		config := orchestrator.CurrentConfig.NextConfig()
		config.Location[shard] = groupID
		orchestrator.CurrentConfig = config

		time.Sleep(1 * time.Second)
		assert.False(t, leader.shardOwnership.Owning(shard))

		newValue := []byte("new value")
		shardInData := map[int][]statemachines.KVPair{
			shard: []statemachines.KVPair{statemachines.KVPair{Key: key, Value: newValue}},
		}
		payload := statemachines.ShardInPayload{
			ConfigVersion: config.Version,
			Data:          shardInData,
		}
		shardInBytes, err := util.EncodeMsgPack(payload)
		assert.Nil(t, err)

		shardInRequest := rpc.ClientRequest{
			ClientId:        uint64(0),
			SequenceNum:     uint64(100),
			StateMachineCmd: statemachines.ShardIn,
			Data:            shardInBytes,
		}
		res := leader.ClientRequest(&shardInRequest)
		assert.Equal(t, rpc.ClientStatus_OK, res.Status)
		assert.True(t, leader.shardOwnership.Owning(shard))

		getRequest, err := getClientRequest(uint64(0), uint64(101), statemachines.KVStoreGet, pair)
		assert.Nil(t, err)
		res = leader.ClientRequest(getRequest)
		assert.Equal(t, rpc.ClientStatus_OK, res.Status)
		assert.Equal(t, newValue, res.Response)
	})

	t.Run("test shard ownership converge to config", func(t *testing.T) {
		shardInData := map[int][]statemachines.KVPair{
			shard2: []statemachines.KVPair{},
			shard3: []statemachines.KVPair{},
		}
		payload := statemachines.ShardInPayload{
			ConfigVersion: orchestrator.CurrentConfig.Version,
			Data:          shardInData,
		}
		shardInBytes, err := util.EncodeMsgPack(payload)
		assert.Nil(t, err)

		shardInRequest := rpc.ClientRequest{
			ClientId:        uint64(0),
			SequenceNum:     uint64(200),
			StateMachineCmd: statemachines.ShardIn,
			Data:            shardInBytes,
		}
		res := leader.ClientRequest(&shardInRequest)
		assert.Equal(t, rpc.ClientStatus_OK, res.Status)

		time.Sleep(1 * time.Second)
		assert.False(t, leader.shardOwnership.Owning(shard2))
		assert.False(t, leader.shardOwnership.Owning(shard3))
	})

	t.Run("simulate last replica group leave", func(t *testing.T) {
		config := orchestrator.CurrentConfig.NextConfig()
		config.Groups = make(map[uint64][]string)
		for i := range config.Location {
			config.Location[i] = uint64(0)
		}
		orchestrator.CurrentConfig = config

		time.Sleep(1 * time.Second)
		shards := leader.shardOwnership.ViewOwnership()
		assert.Equal(t, 0, len(shards))
	})
}
