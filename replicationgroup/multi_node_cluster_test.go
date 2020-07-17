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

func TestReplicationGroupBasic(t *testing.T) {
	util.SuppressLoggers()

	clientid := rand.Uint64()
	cases := []struct {
		name      string
		steps     []Step
		clientid  uint64
		operation uint64
		data      statemachines.KVPair
		expected  rpc.ClientReply
	}{
		{
			name:      "get from empty store",
			steps:     []Step{},
			clientid:  clientid,
			operation: statemachines.KVStoreGet,
			data: statemachines.KVPair{
				Key:   []byte("key"),
				Value: nil,
			},
			expected: rpc.ClientReply{
				Status:     rpc.ClientStatus_OK,
				Response:   []byte{},
				LeaderHint: nil,
			},
		},
		{
			name: "simple put get",
			steps: []Step{
				{
					clientid:  clientid,
					operation: statemachines.KVStorePut,
					data: statemachines.KVPair{
						Key:   []byte("key"),
						Value: []byte("value"),
					},
				},
			},
			clientid:  clientid,
			operation: statemachines.KVStoreGet,
			data: statemachines.KVPair{
				Key:   []byte("key"),
				Value: nil,
			},
			expected: rpc.ClientReply{
				Status:     rpc.ClientStatus_OK,
				Response:   []byte("value"),
				LeaderHint: nil,
			},
		},
		{
			name: "simple append get",
			steps: []Step{
				{
					clientid:  clientid,
					operation: statemachines.KVStoreAppend,
					data: statemachines.KVPair{
						Key:   []byte("key"),
						Value: []byte("value"),
					},
				},
			},
			clientid:  clientid,
			operation: statemachines.KVStoreGet,
			data: statemachines.KVPair{
				Key:   []byte("key"),
				Value: nil,
			},
			expected: rpc.ClientReply{
				Status:     rpc.ClientStatus_OK,
				Response:   []byte("value"),
				LeaderHint: nil,
			},
		},
		{
			name: "simple put append get",
			steps: []Step{
				{
					clientid:  clientid,
					operation: statemachines.KVStorePut,
					data: statemachines.KVPair{
						Key:   []byte("key"),
						Value: []byte("value"),
					},
				},
				{
					clientid:  clientid,
					operation: statemachines.KVStoreAppend,
					data: statemachines.KVPair{
						Key:   []byte("key"),
						Value: []byte("value"),
					},
				},
			},
			clientid:  clientid,
			operation: statemachines.KVStoreGet,
			data: statemachines.KVPair{
				Key:   []byte("key"),
				Value: nil,
			},
			expected: rpc.ClientReply{
				Status:     rpc.ClientStatus_OK,
				Response:   []byte("valuevalue"),
				LeaderHint: nil,
			},
		},
		{
			name: "test put overwriting",
			steps: []Step{
				{
					clientid:  clientid,
					operation: statemachines.KVStorePut,
					data: statemachines.KVPair{
						Key:   []byte("key"),
						Value: []byte("old value"),
					},
				},
				{
					clientid:  clientid,
					operation: statemachines.KVStorePut,
					data: statemachines.KVPair{
						Key:   []byte("key"),
						Value: []byte("new value"),
					},
				},
			},
			clientid:  clientid,
			operation: statemachines.KVStoreGet,
			data: statemachines.KVPair{
				Key:   []byte("key"),
				Value: nil,
			},
			expected: rpc.ClientReply{
				Status:     rpc.ClientStatus_OK,
				Response:   []byte("new value"),
				LeaderHint: nil,
			},
		},
		{
			name: "test simple delete",
			steps: []Step{
				{
					clientid:  clientid,
					operation: statemachines.KVStorePut,
					data: statemachines.KVPair{
						Key:   []byte("key"),
						Value: []byte("value"),
					},
				},
				{
					clientid:  clientid,
					operation: statemachines.KVStoreDelete,
					data: statemachines.KVPair{
						Key:   []byte("key"),
						Value: nil,
					},
				},
			},
			clientid:  clientid,
			operation: statemachines.KVStoreGet,
			data: statemachines.KVPair{
				Key:   []byte("key"),
				Value: nil,
			},
			expected: rpc.ClientReply{
				Status:     rpc.ClientStatus_OK,
				Response:   nil,
				LeaderHint: nil,
			},
		},
		{
			name: "test delete non-existing key",
			steps: []Step{
				{
					clientid:  clientid,
					operation: statemachines.KVStoreDelete,
					data: statemachines.KVPair{
						Key:   []byte("key"),
						Value: nil,
					},
				},
			},
			clientid:  clientid,
			operation: statemachines.KVStoreGet,
			data: statemachines.KVPair{
				Key:   []byte("key"),
				Value: nil,
			},
			expected: rpc.ClientReply{
				Status:     rpc.ClientStatus_OK,
				Response:   nil,
				LeaderHint: nil,
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
			if err != nil {
				t.Fatalf("find leader from local replication group fail: %v\n", err)
			}

			// steps before final test
			for seq, step := range testCase.steps {
				request, err := getClientRequest(step.clientid, uint64(seq), step.operation, step.data)
				assert.Nil(t, err)
				reply := leader.ClientRequest(request)
				if reply.Status != rpc.ClientStatus_OK {
					t.Errorf("step %d failed: %s\n", seq, reply.Status)
				}
			}

			// final operation and verification
			request, err := getClientRequest(testCase.clientid, uint64(len(testCase.steps)), testCase.operation, testCase.data)
			assert.Nil(t, err)
			reply := leader.ClientRequest(request)

			reply.LeaderHint = nil
			if reply.Status != testCase.expected.Status || !bytes.Equal(reply.Response, testCase.expected.Response) {
				t.Errorf("Expected reply to be %v, got %v\n", &testCase.expected, &reply)
			}
		})
	}
}

func TestReplicationGroupFollowerInteraction(t *testing.T) {
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
	if err != nil {
		t.Fatalf("find leader from local replication group fail: %v\n", err)
	}
	follower, err := findFollower(nodes)
	if err != nil {
		t.Fatalf("find follower from local replication group fail: %v\n", err)
	}

	// mock request
	pair := statemachines.KVPair{
		Key:   []byte("key"),
		Value: []byte("value"),
	}
	data, err := util.EncodeMsgPack(pair)
	if err != nil {
		t.Fatalf("encoding kv pair fail: %v", err)
	}
	request := rpc.ClientRequest{
		ClientId:        rand.Uint64(),
		SequenceNum:     uint64(0),
		StateMachineCmd: statemachines.KVStorePut,
		Data:            data,
	}
	followerReply := follower.ClientRequest(&request)
	if followerReply.Status != rpc.ClientStatus_NOT_LEADER {
		t.Fatalf("Expected reply status to be %v, got %v\n", rpc.ClientStatus_NOT_LEADER, followerReply.Status)
	}
	if followerReply.LeaderHint.Id != leader.Self.Id || followerReply.LeaderHint.Addr != leader.Self.Addr {
		t.Fatalf("Expedted leader hint to be %v, got %v\n", leader.Self, followerReply.LeaderHint)
	}
}

func TestReplicationGroupCandidateInteraction(t *testing.T) {
	util.SuppressLoggers()

	groupID := uint64(1)
	orchestrator := getMockSO(groupID)
	nodes, err := CreateLocalReplicationGroup(groupID, raft.DefaultConfig(), orchestrator.Self.Addr)
	if err != nil {
		t.Errorf("Create local replication group failed: %v\n", err)
	}
	defer CleanupReplicationGroup(nodes)
	time.Sleep(500 * time.Millisecond)

	follower, err := findFollower(nodes)
	if err != nil {
		t.Fatalf("find follower from local replication group fail: %v\n", err)
	}
	follower.raft.NetworkPolicy.PauseWorld(true)
	time.Sleep(300 * time.Millisecond)

	// mock request
	pair := statemachines.KVPair{
		Key:   []byte("key"),
		Value: []byte("value"),
	}
	data, err := util.EncodeMsgPack(pair)
	if err != nil {
		t.Fatalf("encoding kv pair fail: %v", err)
	}
	request := rpc.ClientRequest{
		ClientId:        rand.Uint64(),
		SequenceNum:     uint64(0),
		StateMachineCmd: statemachines.KVStorePut,
		Data:            data,
	}
	candidateReply := follower.ClientRequest(&request)
	if candidateReply.Status != rpc.ClientStatus_ELECTION_IN_PROGRESS {
		t.Fatalf("Expected reply status to be %v, got %v\n", rpc.ClientStatus_ELECTION_IN_PROGRESS, candidateReply.Status)
	}
}
