package shardorchestrator

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/ziyaoh/some-kvstore/raft/raft"
	"github.com/ziyaoh/some-kvstore/statemachines"
	"github.com/ziyaoh/some-kvstore/util"
)

type stepData struct {
	groupID      uint64
	addrs        []string
	shardVersion int64
	shard        int
}

var numShards = 2

func TestIntegrationNormal(t *testing.T) {
	util.SuppressLoggers()

	nodes, err := CreateLocalShardOrchestrator(numShards, raft.DefaultConfig())
	assert.Nil(t, err)
	defer CleanupShardOrchestrator(nodes)
	time.Sleep(500 * time.Millisecond)
	addr := nodes[0].Self.Addr

	admin, err := AdminConnect(addr)
	assert.Nil(t, err)
	client, err := ClientConnect(addr)
	assert.Nil(t, err)
	groupID := rand.Uint64()
	internalClient, err := InternalClientConnect(addr, groupID)
	assert.Nil(t, err)
	newInternalClient, err := InternalClientConnect(addr, groupID)
	assert.Nil(t, err)
	assert.Equal(t, internalClient.requester.ID, newInternalClient.requester.ID)

	cases := []struct {
		name       string
		client     string
		operation  uint64
		data       stepData
		expectFail bool
		expected   util.Configuration
	}{
		{
			name:       "query on initial orchestrator",
			client:     "client",
			operation:  statemachines.ConfigQuery,
			data:       stepData{shardVersion: int64(-1)},
			expectFail: false,
			expected:   util.NewConfiguration(numShards),
		},
		{
			name:       "admin query on initial orchestrator",
			client:     "admin",
			operation:  statemachines.ConfigQuery,
			data:       stepData{shardVersion: int64(-1)},
			expectFail: false,
			expected:   util.NewConfiguration(numShards),
		},
		{
			name:      "internal query on initial orchestrator",
			client:    "internal client",
			operation: statemachines.ConfigInternalQuery,
			data: stepData{
				shardVersion: int64(-1),
				groupID:      uint64(1),
				addrs:        []string{"0.0.0.1"},
			},
			expectFail: false,
			expected:   util.NewConfiguration(numShards),
		},
		{
			name:      "move in initial orchestrator",
			client:    "admin",
			operation: statemachines.ConfigMove,
			data: stepData{
				shard:   0,
				groupID: uint64(1),
			},
			expectFail: true,
		},
		{
			name:      "leave in initial orchestrator",
			client:    "admin",
			operation: statemachines.ConfigLeave,
			data: stepData{
				groupID: uint64(1),
			},
			expectFail: true,
		},
		{
			name:      "first join",
			client:    "admin",
			operation: statemachines.ConfigJoin,
			data: stepData{
				groupID: uint64(1),
				addrs:   []string{"0.0.0.1"},
			},
			expectFail: false,
		},
		{
			name:      "internal query with changed addrs",
			client:    "internal client",
			operation: statemachines.ConfigInternalQuery,
			data: stepData{
				shardVersion: int64(-1),
				groupID:      uint64(1),
				addrs:        []string{"0.0.0.10"},
			},
			expectFail: false,
			expected: util.Configuration{
				Version:   uint64(1),
				NumShards: 2,
				Groups:    map[uint64][]string{uint64(1): []string{"0.0.0.10"}},
				Location:  []uint64{uint64(1), uint64(1)},
			},
		},
		{
			name:      "changed addrs visible to following queries",
			client:    "client",
			operation: statemachines.ConfigQuery,
			data: stepData{
				shardVersion: int64(-1),
			},
			expectFail: false,
			expected: util.Configuration{
				Version:   uint64(1),
				NumShards: 2,
				Groups:    map[uint64][]string{uint64(1): []string{"0.0.0.10"}},
				Location:  []uint64{uint64(1), uint64(1)},
			},
		},
		{
			name:      "duplicate join",
			client:    "admin",
			operation: statemachines.ConfigJoin,
			data: stepData{
				groupID: uint64(1),
				addrs:   []string{"0.0.0.1"},
			},
			expectFail: true,
		},
		{
			name:      "second join",
			client:    "admin",
			operation: statemachines.ConfigJoin,
			data: stepData{
				groupID: uint64(2),
				addrs:   []string{"0.0.0.2"},
			},
			expectFail: false,
		},
		{
			name:      "first move",
			client:    "admin",
			operation: statemachines.ConfigMove,
			data: stepData{
				shard:   0,
				groupID: uint64(2),
			},
			expectFail: false,
		},
		{
			name:      "second move",
			client:    "admin",
			operation: statemachines.ConfigMove,
			data: stepData{
				shard:   1,
				groupID: uint64(2),
			},
			expectFail: false,
		},
		{
			name:      "admin query",
			client:    "admin",
			operation: statemachines.ConfigQuery,
			data: stepData{
				shardVersion: int64(-1),
			},
			expectFail: false,
			expected: util.Configuration{
				Version:   uint64(3),
				NumShards: 2,
				Groups:    map[uint64][]string{uint64(1): []string{"0.0.0.10"}, uint64(2): []string{"0.0.0.2"}},
				Location:  []uint64{uint64(2), uint64(2)},
			},
		},
		{
			name:      "group 2 leave",
			client:    "admin",
			operation: statemachines.ConfigLeave,
			data: stepData{
				groupID: uint64(2),
			},
			expectFail: false,
		},
		{
			name:      "verify group 2 leave",
			client:    "client",
			operation: statemachines.ConfigQuery,
			data: stepData{
				shardVersion: int64(-1),
			},
			expectFail: false,
			expected: util.Configuration{
				Version:   uint64(4),
				NumShards: 2,
				Groups:    map[uint64][]string{uint64(1): []string{"0.0.0.10"}},
				Location:  []uint64{uint64(1), uint64(1)},
			},
		},
		{
			name:      "group 2 join back",
			client:    "admin",
			operation: statemachines.ConfigJoin,
			data: stepData{
				groupID: uint64(2),
				addrs:   []string{"0.0.0.2"},
			},
			expectFail: false,
		},
		{
			name:      "group 1 leave",
			client:    "admin",
			operation: statemachines.ConfigLeave,
			data: stepData{
				groupID: uint64(1),
			},
			expectFail: false,
		},
		{
			name:      "group 2 leave",
			client:    "admin",
			operation: statemachines.ConfigLeave,
			data: stepData{
				groupID: uint64(2),
			},
			expectFail: false,
		},
	}
	for _, step := range cases {
		t.Run(step.name, func(t *testing.T) {
			switch step.client {
			case "admin":
				switch step.operation {
				case statemachines.ConfigJoin:
					err = admin.Join(step.data.groupID, step.data.addrs)
					assert.Equal(t, step.expectFail, err != nil)
				case statemachines.ConfigLeave:
					err = admin.Leave(step.data.groupID)
					assert.Equal(t, step.expectFail, err != nil)
				case statemachines.ConfigMove:
					err = admin.Move(step.data.shard, step.data.groupID)
					assert.Equal(t, step.expectFail, err != nil)
				case statemachines.ConfigQuery:
					config, err := admin.Query(step.data.shardVersion)
					assert.Equal(t, step.expectFail, err != nil)
					if !step.expectFail && (step.operation == statemachines.ConfigInternalQuery || step.operation == statemachines.ConfigQuery) {
						assert.Equal(t, step.expected, config)
					}
				default:
					t.Error("unknown operation")
				}

			case "client":
				config, err := client.Query(step.data.shardVersion)
				assert.Equal(t, step.expectFail, err != nil)
				if !step.expectFail && (step.operation == statemachines.ConfigInternalQuery || step.operation == statemachines.ConfigQuery) {
					assert.Equal(t, step.expected, config)
				}
			case "internal client":
				config, err := internalClient.InternalQuery(step.data.shardVersion, step.data.groupID, step.data.addrs)
				assert.Equal(t, step.expectFail, err != nil)
				if !step.expectFail && (step.operation == statemachines.ConfigInternalQuery || step.operation == statemachines.ConfigQuery) {
					assert.Equal(t, step.expected, config)
				}
			default:
				t.Errorf("unknown client type %s", step.client)
			}
		})
	}
}

func TestIntegrationOnClusterPartition(t *testing.T) {
	util.SuppressLoggers()

	nodes, err := CreateLocalShardOrchestrator(numShards, raft.DefaultConfig())
	if err != nil {
		t.Errorf("Create local shard orchestrator failed: %v\n", err)
	}
	defer CleanupShardOrchestrator(nodes)
	time.Sleep(500 * time.Millisecond)

	leader, err := findLeader(nodes)
	assert.Nil(t, err, "find leader fail")
	addr := nodes[0].Self.Addr

	admin, err := AdminConnect(addr)
	assert.Nil(t, err, "admin connect to shard orchestrator fail")

	leader.raft.NetworkPolicy.PauseWorld(true)
	channel := make(chan error)
	go func() {
		err = admin.Join(uint64(1), []string{"0.0.0.1"})
		channel <- err
	}()
	time.Sleep(300 * time.Millisecond)
	leader.raft.NetworkPolicy.PauseWorld(false)

	err = <-channel
	assert.Nilf(t, err, "admin join fail: %v", err)

	expectedConfig := util.Configuration{
		Version:   uint64(1),
		NumShards: 2,
		Groups:    map[uint64][]string{uint64(1): []string{"0.0.0.1"}},
		Location:  []uint64{uint64(1), uint64(1)},
	}
	config, err := admin.Query(int64(-1))
	assert.Nilf(t, err, "admin query fail: %v", err)
	assert.Equal(t, expectedConfig, config)
}
