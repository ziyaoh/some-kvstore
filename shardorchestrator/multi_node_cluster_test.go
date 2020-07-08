package shardorchestrator

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/ziyaoh/some-kvstore/raft/raft"
	"github.com/ziyaoh/some-kvstore/rpc"
	"github.com/ziyaoh/some-kvstore/statemachines"
	"github.com/ziyaoh/some-kvstore/util"
)

func TestShardOrchestratorRegisterClient(t *testing.T) {
	util.SuppressLoggers()

	nodes, err := CreateLocalShardOrchestrator(numShards, raft.DefaultConfig())
	assert.Nil(t, err)
	defer CleanupShardOrchestrator(nodes)
	time.Sleep(500 * time.Millisecond)

	leader, err := findLeader(nodes)
	assert.Nil(t, err)

	result := leader.RegisterClient(&rpc.RegisterClientRequest{})
	assert.NotNil(t, result)
	assert.Equal(t, rpc.ClientStatus_OK, result.Status)

	newResult := leader.RegisterClient(&rpc.RegisterClientRequest{})
	assert.NotNil(t, newResult)
	assert.Equal(t, rpc.ClientStatus_OK, newResult.Status)

	assert.NotEqual(t, result.ClientId, newResult.ClientId)
}

func TestShardOrchestratorClientRequests(t *testing.T) {
	util.SuppressLoggers()

	nodes, err := CreateLocalShardOrchestrator(numShards, raft.DefaultConfig())
	assert.Nil(t, err)
	defer CleanupShardOrchestrator(nodes)
	time.Sleep(500 * time.Millisecond)

	leader, err := findLeader(nodes)
	assert.Nil(t, err)

	result := leader.RegisterClient(&rpc.RegisterClientRequest{})
	assert.NotNil(t, result)
	assert.Equal(t, rpc.ClientStatus_OK, result.Status)
	clientID := result.ClientId

	steps := []testStep{
		testStep{
			name:       "query on initial orchestrator",
			operation:  statemachines.ConfigQuery,
			data:       statemachines.ConfigQueryPayload{ShardVersion: int64(-1)},
			expectFail: false,
		},
		testStep{
			name:      "internal query on initial orchestrator",
			operation: statemachines.ConfigInternalQuery,
			data: statemachines.ConfigInternalQueryPayload{
				ShardVersion: int64(-1),
				SrcGroupID:   uint64(1),
				Addrs:        []string{"0.0.0.0"},
			},
			expectFail: false,
		},
		testStep{
			name:      "group leave on initial orchestrator",
			operation: statemachines.ConfigLeave,
			data: statemachines.ConfigLeavePayload{
				GroupID: uint64(1),
			},
			expectFail: true,
		},
		testStep{
			name:      "shard move on initial orchestrator",
			operation: statemachines.ConfigMove,
			data: statemachines.ConfigMovePayload{
				Shard:     0,
				DestGroup: uint64(1),
			},
			expectFail: true,
		},
		testStep{
			name:      "first join",
			operation: statemachines.ConfigJoin,
			data: statemachines.ConfigJoinPayload{
				GroupID: uint64(1),
				Addrs:   []string{"0.0.0.1"},
			},
			expectFail: false,
		},
		testStep{
			name:      "second join",
			operation: statemachines.ConfigJoin,
			data: statemachines.ConfigJoinPayload{
				GroupID: uint64(2),
				Addrs:   []string{"0.0.0.2"},
			},
			expectFail: false,
		},
		testStep{
			name:      "try query",
			operation: statemachines.ConfigQuery,
			data: statemachines.ConfigQueryPayload{
				ShardVersion: int64(-1),
			},
			expectFail: false,
		},
		testStep{
			name:      "try move",
			operation: statemachines.ConfigMove,
			data: statemachines.ConfigMovePayload{
				Shard:     1,
				DestGroup: uint64(1),
			},
			expectFail: false,
		},
		testStep{
			name:      "try move to invalid group",
			operation: statemachines.ConfigMove,
			data: statemachines.ConfigMovePayload{
				Shard:     1,
				DestGroup: uint64(0),
			},
			expectFail: true,
		},
		testStep{
			name:      "internal query by new group",
			operation: statemachines.ConfigInternalQuery,
			data: statemachines.ConfigInternalQueryPayload{
				ShardVersion: int64(-1),
				SrcGroupID:   uint64(3),
				Addrs:        []string{"0.0.0.3"},
			},
			expectFail: false,
		},
		testStep{
			name:      "group 1 leave",
			operation: statemachines.ConfigLeave,
			data: statemachines.ConfigLeavePayload{
				GroupID: uint64(1),
			},
			expectFail: false,
		},
		testStep{
			name:      "group 2 leave",
			operation: statemachines.ConfigLeave,
			data: statemachines.ConfigLeavePayload{
				GroupID: uint64(2),
			},
			expectFail: false,
		},
		testStep{
			name:      "last query on empty orchestrator",
			operation: statemachines.ConfigQuery,
			data: statemachines.ConfigQueryPayload{
				ShardVersion: int64(-1),
			},
			expectFail: false,
		},
	}
	for stepNum, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			dataBytes, err := util.EncodeMsgPack(step.data)
			assert.Nil(t, err)
			ackSeq := []uint64{}
			if stepNum > 0 {
				ackSeq = []uint64{uint64(stepNum - 1)}
			}
			reply := leader.ClientRequest(&rpc.ClientRequest{
				ClientId:        clientID,
				SequenceNum:     uint64(stepNum),
				StateMachineCmd: step.operation,
				Data:            dataBytes,
				AckSeqs:         ackSeq,
			})

			if step.expectFail {
				assert.NotEqual(t, rpc.ClientStatus_OK, reply.GetStatus())
			} else {
				assert.Equal(t, rpc.ClientStatus_OK, reply.GetStatus())
				if step.operation == statemachines.ConfigQuery || step.operation == statemachines.ConfigInternalQuery {
					var config util.Configuration
					err := util.DecodeMsgPack(reply.GetResponse(), &config)
					assert.Nil(t, err)
					assert.Nil(t, config.Validate())
				}
			}
		})
	}
}

func TestReplicationGroupFollowerInteraction(t *testing.T) {
	util.SuppressLoggers()

	nodes, err := CreateLocalShardOrchestrator(numShards, raft.DefaultConfig())
	assert.Nil(t, err)
	defer CleanupShardOrchestrator(nodes)
	time.Sleep(500 * time.Millisecond)

	leader, err := findLeader(nodes)
	if err != nil {
		t.Fatalf("find leader from local replication group fail: %v\n", err)
	}
	follower, err := findFollower(nodes)
	if err != nil {
		t.Fatalf("find follower from local replication group fail: %v\n", err)
	}

	// mock request
	result := follower.RegisterClient(&rpc.RegisterClientRequest{})
	assert.NotNil(t, result)
	assert.Equal(t, rpc.ClientStatus_NOT_LEADER, result.Status)
	assert.Equal(t, leader.Self.Addr, result.LeaderHint.Addr)

	data, err := util.EncodeMsgPack(statemachines.ConfigQueryPayload{})
	assert.Nil(t, err)
	reply := follower.ClientRequest(&rpc.ClientRequest{
		ClientId:        rand.Uint64(),
		SequenceNum:     uint64(0),
		StateMachineCmd: statemachines.ConfigQuery,
		Data:            data,
		AckSeqs:         []uint64{},
	})

	assert.Equal(t, rpc.ClientStatus_NOT_LEADER, reply.GetStatus())
	assert.Equal(t, leader.Self.Addr, result.LeaderHint.Addr)
}

func TestReplicationGroupCandidateInteraction(t *testing.T) {
	util.SuppressLoggers()

	nodes, err := CreateLocalShardOrchestrator(numShards, raft.DefaultConfig())
	if err != nil {
		t.Errorf("Create local replication group failed: %v\n", err)
	}
	defer CleanupShardOrchestrator(nodes)
	time.Sleep(500 * time.Millisecond)

	follower, err := findFollower(nodes)
	if err != nil {
		t.Fatalf("find follower from local replication group fail: %v\n", err)
	}
	follower.raft.NetworkPolicy.PauseWorld(true)
	time.Sleep(300 * time.Millisecond)

	// mock request
	result := follower.RegisterClient(&rpc.RegisterClientRequest{})
	assert.NotNil(t, result)
	assert.Equal(t, rpc.ClientStatus_ELECTION_IN_PROGRESS, result.Status)

	data, err := util.EncodeMsgPack(statemachines.ConfigQueryPayload{})
	assert.Nil(t, err)
	reply := follower.ClientRequest(&rpc.ClientRequest{
		ClientId:        rand.Uint64(),
		SequenceNum:     uint64(0),
		StateMachineCmd: statemachines.ConfigQuery,
		Data:            data,
		AckSeqs:         []uint64{},
	})

	assert.Equal(t, rpc.ClientStatus_ELECTION_IN_PROGRESS, reply.GetStatus())
}
