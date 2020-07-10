package shardorchestrator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/ziyaoh/some-kvstore/raft/raft"
	"github.com/ziyaoh/some-kvstore/rpc"
	"github.com/ziyaoh/some-kvstore/statemachines"
	"github.com/ziyaoh/some-kvstore/util"
)

func TestNodeCreation(t *testing.T) {
	util.SuppressLoggers()

	config := oneNodeClusterConfig()
	node, err := CreateNode(util.OpenPort(0), nil, config, new(statemachines.ConfigMachine), raft.NewMemoryStore())
	if err != nil {
		t.Errorf("Create Single Node fail: %v", err)
	}

	if node == nil {
		t.Error("Create single node returns nil node")
	}
	node.GracefulExit()
}

func TestOneNodeClusterRegisterClient(t *testing.T) {
	util.SuppressLoggers()

	configMachine := statemachines.NewConfigMachine(util.NumShards)
	config := oneNodeClusterConfig()
	node, err := CreateNode(util.OpenPort(0), nil, config, configMachine, raft.NewMemoryStore())
	assert.Nil(t, err)
	defer node.GracefulExit()
	time.Sleep(500 * time.Millisecond)

	result := node.RegisterClient(&rpc.RegisterClientRequest{})
	assert.NotNil(t, result)
	assert.Equal(t, rpc.ClientStatus_OK, result.Status)

	newResult := node.RegisterClient(&rpc.RegisterClientRequest{})
	assert.NotNil(t, newResult)
	assert.Equal(t, rpc.ClientStatus_OK, newResult.Status)

	assert.NotEqual(t, result.ClientId, newResult.ClientId)
}

func TestOneNodeClusterRegisterClientIdempotency(t *testing.T) {
	util.SuppressLoggers()

	configMachine := statemachines.NewConfigMachine(util.NumShards)
	config := oneNodeClusterConfig()
	node, err := CreateNode(util.OpenPort(0), nil, config, configMachine, raft.NewMemoryStore())
	assert.Nil(t, err)
	defer node.GracefulExit()
	time.Sleep(500 * time.Millisecond)

	result := node.RegisterClient(&rpc.RegisterClientRequest{
		Idempotent:    true,
		IdempotencyID: uint64(123),
	})
	assert.NotNil(t, result)
	assert.Equal(t, rpc.ClientStatus_OK, result.Status)

	newResult := node.RegisterClient(&rpc.RegisterClientRequest{
		Idempotent:    true,
		IdempotencyID: uint64(1234),
	})
	assert.NotNil(t, newResult)
	assert.Equal(t, rpc.ClientStatus_OK, newResult.Status)
	assert.NotEqual(t, result.ClientId, newResult.ClientId)

	newNewResult := node.RegisterClient(&rpc.RegisterClientRequest{
		Idempotent:    true,
		IdempotencyID: uint64(123),
	})
	assert.NotNil(t, newNewResult)
	assert.Equal(t, rpc.ClientStatus_OK, result.Status)
	assert.NotEqual(t, newNewResult.ClientId, newResult.ClientId)
	assert.Equal(t, result.ClientId, newNewResult.ClientId)
}

type testStep struct {
	name                string
	seq                 uint64
	operation           uint64
	data                interface{}
	expectFail          bool
	expectConfigVersion uint64
	expectConfigGroups  *map[uint64][]string
}

func TestOneNodeClusterBasicRequest(t *testing.T) {
	util.SuppressLoggers()

	numShards := 3

	configMachine := statemachines.NewConfigMachine(numShards)
	config := oneNodeClusterConfig()
	node, err := CreateNode(util.OpenPort(0), nil, config, configMachine, raft.NewMemoryStore())
	assert.Nil(t, err)
	defer node.GracefulExit()
	time.Sleep(500 * time.Millisecond)

	result := node.RegisterClient(&rpc.RegisterClientRequest{})
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
			name:      "internal query with changed group",
			operation: statemachines.ConfigInternalQuery,
			data: statemachines.ConfigInternalQueryPayload{
				ShardVersion: int64(-1),
				SrcGroupID:   uint64(1),
				Addrs:        []string{"0.0.0.10"},
			},
			expectFail: false,
			expectConfigGroups: &map[uint64][]string{
				uint64(1): []string{"0.0.0.10"},
				uint64(2): []string{"0.0.0.2"},
			},
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
			reply := node.ClientRequest(&rpc.ClientRequest{
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
					if step.expectConfigGroups != nil {
						assert.Equal(t, *step.expectConfigGroups, config.Groups)
					}
				}
			}
		})
	}
}

func TestOneNodeClusterIdempotency(t *testing.T) {
	util.SuppressLoggers()

	configMachine := statemachines.NewConfigMachine(util.NumShards)
	config := oneNodeClusterConfig()
	node, err := CreateNode(util.OpenPort(0), nil, config, configMachine, raft.NewMemoryStore())
	assert.Nil(t, err)
	defer node.GracefulExit()
	time.Sleep(500 * time.Millisecond)

	result := node.RegisterClient(&rpc.RegisterClientRequest{})
	assert.NotNil(t, result)
	assert.Equal(t, rpc.ClientStatus_OK, result.Status)
	clientID := result.ClientId

	steps := []testStep{
		testStep{
			seq:       uint64(0),
			operation: statemachines.ConfigJoin,
			data: statemachines.ConfigJoinPayload{
				GroupID: uint64(1),
				Addrs:   []string{"0.0.0.1"},
			},
		},
		testStep{
			seq:       uint64(1),
			operation: statemachines.ConfigJoin,
			data: statemachines.ConfigJoinPayload{
				GroupID: uint64(2),
				Addrs:   []string{"0.0.0.2"},
			},
		},
		testStep{
			seq:       uint64(2),
			operation: statemachines.ConfigQuery,
			data: statemachines.ConfigQueryPayload{
				ShardVersion: int64(-1),
			},
			expectConfigVersion: uint64(2),
		},
		testStep{
			seq:       uint64(1),
			operation: statemachines.ConfigJoin,
			data: statemachines.ConfigJoinPayload{
				GroupID: uint64(2),
				Addrs:   []string{"0.0.0.2"},
			},
		},
		testStep{
			seq:       uint64(3),
			operation: statemachines.ConfigQuery,
			data: statemachines.ConfigQueryPayload{
				ShardVersion: int64(-1),
			},
			expectConfigVersion: uint64(2),
		},
	}
	for _, step := range steps {
		dataBytes, err := util.EncodeMsgPack(step.data)
		assert.Nil(t, err)
		reply := node.ClientRequest(&rpc.ClientRequest{
			ClientId:        clientID,
			SequenceNum:     step.seq,
			StateMachineCmd: step.operation,
			Data:            dataBytes,
		})

		assert.Equal(t, rpc.ClientStatus_OK, reply.GetStatus())
		if step.operation == statemachines.ConfigQuery || step.operation == statemachines.ConfigInternalQuery {
			var config util.Configuration
			err := util.DecodeMsgPack(reply.GetResponse(), &config)
			assert.Nil(t, err)
			assert.Nil(t, config.Validate())
			assert.Equal(t, step.expectConfigVersion, config.Version)
		}
	}
}

func oneNodeClusterConfig() *raft.Config {
	config := raft.DefaultConfig()
	config.ClusterSize = 1
	return config
}
