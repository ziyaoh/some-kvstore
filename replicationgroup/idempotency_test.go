package replicationgroup

import (
	"bytes"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/ziyaoh/some-kvstore/raft/raft"
	"github.com/ziyaoh/some-kvstore/rpc"
	"github.com/ziyaoh/some-kvstore/statemachines"
	"github.com/ziyaoh/some-kvstore/util"
)

func TestIdempotencyOnSameLeader(t *testing.T) {
	util.SuppressLoggers()

	defaultClientID := rand.Uint64()
	cases := []struct {
		name  string
		steps []Req
	}{
		{
			name: "simple idempotency on append",
			steps: []Req{
				{
					clientid:  defaultClientID,
					seq:       uint64(0),
					operation: statemachines.KVStorePut,
					data: statemachines.KVPair{
						Key:   []byte("key"),
						Value: []byte("value"),
					},
					expected: nil,
				},
				{
					clientid:  defaultClientID,
					seq:       uint64(1),
					operation: statemachines.KVStoreAppend,
					data: statemachines.KVPair{
						Key:   []byte("key"),
						Value: []byte("trailing"),
					},
					expected: nil,
				},
				{
					clientid:  defaultClientID,
					seq:       uint64(1),
					operation: statemachines.KVStoreAppend,
					data: statemachines.KVPair{
						Key:   []byte("key"),
						Value: []byte("trailing"),
					},
					expected: nil,
				},
				{
					clientid:  defaultClientID,
					seq:       uint64(2),
					operation: statemachines.KVStoreGet,
					data: statemachines.KVPair{
						Key:   []byte("key"),
						Value: nil,
					},
					expected: &rpc.ClientReply{
						Status:   rpc.ClientStatus_OK,
						Response: []byte("valuetrailing"),
					},
				},
			},
		},
		{
			name: "different client with same seq",
			steps: []Req{
				{
					clientid:  defaultClientID,
					seq:       uint64(0),
					operation: statemachines.KVStorePut,
					data: statemachines.KVPair{
						Key:   []byte("key"),
						Value: []byte("value"),
					},
					expected: nil,
				},
				{
					clientid:  defaultClientID,
					seq:       uint64(1),
					operation: statemachines.KVStoreAppend,
					data: statemachines.KVPair{
						Key:   []byte("key"),
						Value: []byte("trailing"),
					},
					expected: nil,
				},
				{
					clientid:  rand.Uint64(),
					seq:       uint64(1),
					operation: statemachines.KVStoreAppend,
					data: statemachines.KVPair{
						Key:   []byte("key"),
						Value: []byte("trailing"),
					},
					expected: nil,
				},
				{
					clientid:  defaultClientID,
					seq:       uint64(2),
					operation: statemachines.KVStoreGet,
					data: statemachines.KVPair{
						Key:   []byte("key"),
						Value: nil,
					},
					expected: &rpc.ClientReply{
						Status:   rpc.ClientStatus_OK,
						Response: []byte("valuetrailingtrailing"),
					},
				},
			},
		},
		{
			name: "simulate out of order old request",
			steps: []Req{
				{
					clientid:  defaultClientID,
					seq:       uint64(0),
					operation: statemachines.KVStorePut,
					data: statemachines.KVPair{
						Key:   []byte("key"),
						Value: []byte("old value"),
					},
					expected: nil,
				},
				{
					clientid:  defaultClientID,
					seq:       uint64(1),
					operation: statemachines.KVStorePut,
					data: statemachines.KVPair{
						Key:   []byte("key"),
						Value: []byte("new value"),
					},
					expected: nil,
				},
				{
					clientid:  defaultClientID,
					seq:       uint64(0),
					operation: statemachines.KVStorePut,
					data: statemachines.KVPair{
						Key:   []byte("key"),
						Value: []byte("old value"),
					},
					expected: nil,
				},
				{
					clientid:  defaultClientID,
					seq:       uint64(2),
					operation: statemachines.KVStoreGet,
					data: statemachines.KVPair{
						Key:   []byte("key"),
						Value: nil,
					},
					expected: &rpc.ClientReply{
						Status:   rpc.ClientStatus_OK,
						Response: []byte("new value"),
					},
				},
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			groupID := uint64(1)
			orchestrator := getMockSO(groupID)
			nodes, err := CreateLocalReplicationGroup(groupID, raft.DefaultConfig(), orchestrator.Self.Addr)
			if err != nil {
				t.Errorf("Create local replication group failed: %v\n", err)
			}
			defer CleanupReplicationGroup(nodes)
			time.Sleep(500 * time.Millisecond)

			leader, err := findLeader(nodes)
			assert.Nil(t, err, "Finding leader from local replication group fail")
			assert.NotNil(t, leader, "Finding leader from local replication group returns nil")

			// steps before final test
			for _, step := range testCase.steps {
				data, err := util.EncodeMsgPack(step.data)
				if err != nil {
					t.Fatalf("encoding kv pair fail: %v", err)
				}
				payloadData := statemachines.KVStoreCommandPayload{
					Command: step.operation,
					Data:    data,
				}
				payloadBytes, err := util.EncodeMsgPack(payloadData)
				assert.Nil(t, err)
				request := rpc.ClientRequest{
					ClientId:        step.clientid,
					SequenceNum:     step.seq,
					StateMachineCmd: statemachines.KVStoreCommand,
					Data:            payloadBytes,
				}
				reply := leader.ClientRequest(&request)
				reply.LeaderHint = nil
				if step.expected != nil && (reply.Status != step.expected.Status || !bytes.Equal(reply.Response, step.expected.Response)) {
					t.Errorf("Expected reply to be %v, got %v\n", step.expected, &reply)
				}
			}
		})
	}
}

func TestIdempotencyAcrossLeaders(t *testing.T) {
	util.SuppressLoggers()

	groupID := uint64(1)
	orchestrator := getMockSO(groupID)
	nodes, err := CreateLocalReplicationGroup(groupID, raft.DefaultConfig(), orchestrator.Self.Addr)
	if err != nil {
		t.Errorf("Create local replication group failed: %v\n", err)
	}
	defer CleanupReplicationGroup(nodes)
	time.Sleep(500 * time.Millisecond)

	leader, err := findLeader(nodes)
	assert.Nil(t, err, "Finding leader from local replication group fail")
	assert.NotNil(t, leader, "Finding leader from local replication group returns nil")
	followers, err := findAllFollowers(nodes)
	assert.Nil(t, err, "Finding followers from local replication group fail")
	assert.NotNil(t, leader, "Finding followers from local replication group returns nil")

	pair := statemachines.KVPair{
		Key:   []byte("key"),
		Value: []byte("value"),
	}

	clientid := rand.Uint64()
	request, err := getClientRequest(clientid, uint64(0), statemachines.KVStorePut, pair)
	assert.Nil(t, err)
	reply := leader.ClientRequest(request)
	assert.Equal(t, rpc.ClientStatus_OK, reply.Status, "client put request failed")

	appendRequest, err := getClientRequest(clientid, uint64(1), statemachines.KVStoreAppend, pair)
	assert.Nil(t, err)
	appendReply := leader.ClientRequest(appendRequest)
	assert.Equal(t, rpc.ClientStatus_OK, appendReply.Status, "client append request failed")

	// simulate old leader partitioned before sending out result
	leader.raft.NetworkPolicy.PauseWorld(true)
	time.Sleep(500 * time.Millisecond)

	newLeader, err := findLeader(followers)
	assert.Nil(t, err, "Finding new leader from local replication group fail")
	assert.NotNil(t, leader, "Finding new leader from local replication group returns nil")
	newAppendReply := newLeader.ClientRequest(appendRequest)
	assert.Equal(t, rpc.ClientStatus_OK, newAppendReply.Status, "client append request failed")

	getRequest, err := getClientRequest(clientid, uint64(2), statemachines.KVStoreGet, pair)
	getReply := newLeader.ClientRequest(getRequest)
	assert.Equal(t, rpc.ClientStatus_OK, getReply.Status, "client get request failed")
	assert.Equal(t, []byte("valuevalue"), getReply.Response, "value mismatch")
}
