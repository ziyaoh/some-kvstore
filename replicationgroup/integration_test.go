package replicationgroup

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/ziyaoh/some-kvstore/raft/raft"
	"github.com/ziyaoh/some-kvstore/raft/statemachines"
	"github.com/ziyaoh/some-kvstore/util"
)

type ClientStep struct {
	operation uint64
	key       []byte
	value     []byte
}

func TestIntegrationNormal(t *testing.T) {
	util.SuppressLoggers()

	cases := []struct {
		name      string
		steps     []ClientStep
		operation uint64
		key       []byte
		value     []byte
		expected  []byte
	}{
		{
			name:      "get from empty store",
			steps:     []ClientStep{},
			operation: statemachines.KVStoreGet,
			key:       []byte("key"),
			value:     nil,
			expected:  []byte{},
		},
		{
			name: "simple put get",
			steps: []ClientStep{
				{
					operation: statemachines.KVStorePut,
					key:       []byte("key"),
					value:     []byte("value"),
				},
			},
			operation: statemachines.KVStoreGet,
			key:       []byte("key"),
			value:     nil,
			expected:  []byte("value"),
		},
		{
			name: "simple append get",
			steps: []ClientStep{
				{
					operation: statemachines.KVStoreAppend,
					key:       []byte("key"),
					value:     []byte("value"),
				},
			},
			operation: statemachines.KVStoreGet,
			key:       []byte("key"),
			value:     nil,
			expected:  []byte("value"),
		},
		{
			name: "simple put append get",
			steps: []ClientStep{
				{
					operation: statemachines.KVStorePut,
					key:       []byte("key"),
					value:     []byte("value"),
				},
				{
					operation: statemachines.KVStoreAppend,
					key:       []byte("key"),
					value:     []byte("value"),
				},
			},
			operation: statemachines.KVStoreGet,
			key:       []byte("key"),
			value:     nil,
			expected:  []byte("valuevalue"),
		},
		{
			name: "test put overwriting",
			steps: []ClientStep{
				{
					operation: statemachines.KVStorePut,
					key:       []byte("key"),
					value:     []byte("old value"),
				},
				{
					operation: statemachines.KVStorePut,
					key:       []byte("key"),
					value:     []byte("new value"),
				},
			},
			operation: statemachines.KVStoreGet,
			key:       []byte("key"),
			value:     nil,
			expected:  []byte("new value"),
		},
		{
			name: "test simple delete",
			steps: []ClientStep{
				{
					operation: statemachines.KVStorePut,
					key:       []byte("key"),
					value:     []byte("value"),
				},
				{
					operation: statemachines.KVStoreDelete,
					key:       []byte("key"),
					value:     nil,
				},
			},
			operation: statemachines.KVStoreGet,
			key:       []byte("key"),
			value:     nil,
			expected:  nil,
		},
		{
			name: "test delete non-existing key",
			steps: []ClientStep{
				{
					operation: statemachines.KVStoreDelete,
					key:       []byte("key"),
					value:     nil,
				},
			},
			operation: statemachines.KVStoreGet,
			key:       []byte("key"),
			value:     nil,
			expected:  nil,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			nodes, err := CreateLocalReplicationGroup(raft.DefaultConfig())
			if err != nil {
				t.Errorf("Create local replication group failed: %v\n", err)
			}
			defer CleanupReplicationGroup(nodes)
			time.Sleep(500 * time.Millisecond)

			addr := nodes[0].Self.Addr

			client, err := Connect(addr)
			assert.Nil(t, err, "client connect to replication group fail")

			// steps before final test
			for seq, step := range testCase.steps {
				switch step.operation {
				case statemachines.KVStoreGet:
					_, err := client.Get(step.key)
					assert.Nilf(t, err, "expecting operation %d to succeed", seq)
				case statemachines.KVStorePut:
					_, err := client.Put(step.key, step.value)
					assert.Nilf(t, err, "expecting operation %d to succeed", seq)
				case statemachines.KVStoreAppend:
					_, err := client.Append(step.key, step.value)
					assert.Nilf(t, err, "expecting operation %d to succeed", seq)
				case statemachines.KVStoreDelete:
					_, err := client.Delete(step.key)
					assert.Nilf(t, err, "expecting operation %d to succeed", seq)
				default:
					t.Errorf("unknown operation %v\n", step.operation)
				}
			}

			// final operation and verification
			var result []byte
			switch testCase.operation {
			case statemachines.KVStoreGet:
				result, err = client.Get(testCase.key)
				assert.Nil(t, err, "expecting final operation to succeed")
			case statemachines.KVStorePut:
				result, err = client.Put(testCase.key, testCase.value)
				assert.Nil(t, err, "expecting final operation to succeed")
			case statemachines.KVStoreAppend:
				result, err = client.Append(testCase.key, testCase.value)
				assert.Nil(t, err, "expecting final operation to succeed")
			case statemachines.KVStoreDelete:
				result, err = client.Delete(testCase.key)
				assert.Nil(t, err, "expecting final operation to succeed")
			default:
				t.Errorf("unknown operation %v\n", testCase.operation)
			}

			if !bytes.Equal(result, testCase.expected) {
				t.Errorf("Expected reply to be %v, got %v\n", &testCase.expected, &result)
			}
		})
	}
}

func TestIntegrationOnClusterPartition(t *testing.T) {
	util.SuppressLoggers()

	nodes, err := CreateLocalReplicationGroup(raft.DefaultConfig())
	if err != nil {
		t.Errorf("Create local replication group failed: %v\n", err)
	}
	defer CleanupReplicationGroup(nodes)
	time.Sleep(500 * time.Millisecond)

	leader, err := findLeader(nodes)
	assert.Nil(t, err, "find leader fail")
	addr := nodes[0].Self.Addr

	client, err := Connect(addr)
	assert.Nil(t, err, "client connect to replication group fail")

	_, err = client.Put([]byte("key"), []byte("value"))
	assert.Nilf(t, err, "client try put fail: %v", err)

	leader.raft.NetworkPolicy.PauseWorld(true)
	channel := make(chan error)
	go func() {
		_, err := client.Append([]byte("key"), []byte("tail"))
		channel <- err
	}()
	time.Sleep(300 * time.Millisecond)
	leader.raft.NetworkPolicy.PauseWorld(false)

	err = <-channel
	assert.Nilf(t, err, "client append fail: %v", err)

	result, err := client.Get([]byte("key"))
	assert.Nilf(t, err, "client get key fail: %v", err)
	assert.Equal(t, []byte("valuetail"), result, "result mismatch")
}
