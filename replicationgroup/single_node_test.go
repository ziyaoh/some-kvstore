package replicationgroup

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ziyaoh/some-kvstore/rpc"
	"github.com/ziyaoh/some-kvstore/statemachines"

	"github.com/ziyaoh/some-kvstore/raft/raft"
	hashmachine "github.com/ziyaoh/some-kvstore/raft/statemachines"
	"github.com/ziyaoh/some-kvstore/util"
)

func TestNodeCreation(t *testing.T) {
	util.SuppressLoggers()

	config := oneNodeClusterConfig()
	node, err := CreateNode(util.OpenPort(0), nil, config, new(hashmachine.HashMachine), raft.NewMemoryStore())
	if err != nil {
		t.Errorf("Create Single Node fail: %v", err)
	}

	if node == nil {
		t.Error("Create single node returns nil node")
	}
	node.GracefulExit()
}

type Step struct {
	clientid  uint64
	operation uint64
	data      statemachines.KVPair
}

func TestOneNodeClusterBasic(t *testing.T) {
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
			// set up single node cluster
			boltPath := filepath.Join(os.TempDir(), fmt.Sprintf("kvstore_%d", rand.Int()))
			defer os.Remove(boltPath)

			kvstore, err := statemachines.NewKVStoreMachine(boltPath)
			if err != nil {
				t.Error(err)
			}
			if kvstore == nil {
				t.Fail()
			}
			defer kvstore.Close()

			config := oneNodeClusterConfig()
			node, err := CreateNode(util.OpenPort(0), nil, config, kvstore, raft.NewMemoryStore())
			if err != nil {
				t.Errorf("Create Single Node fail: %v", err)
			}
			if node == nil {
				t.Error("Create single node returns nil node")
			}
			defer node.GracefulExit()
			time.Sleep(500 * time.Millisecond)

			// steps before final test
			for seq, step := range testCase.steps {
				data, err := util.EncodeMsgPack(step.data)
				if err != nil {
					t.Fatalf("encoding kv pair fail: %v", err)
				}
				request := rpc.ClientRequest{
					ClientId:        step.clientid,
					SequenceNum:     uint64(seq),
					StateMachineCmd: step.operation,
					Data:            data,
				}
				reply := node.ClientRequest(&request)
				if reply.Status != rpc.ClientStatus_OK {
					t.Errorf("step %d failed: %s\n", seq, reply.Status)
				}
			}

			// final operation and verification
			finalData, err := util.EncodeMsgPack(testCase.data)
			if err != nil {
				t.Fatalf("encoding kv pair fail: %v", err)
			}
			finalRequest := rpc.ClientRequest{
				ClientId:        testCase.clientid,
				SequenceNum:     uint64(len(testCase.steps)),
				StateMachineCmd: testCase.operation,
				Data:            finalData,
			}
			reply := node.ClientRequest(&finalRequest)
			reply.LeaderHint = nil
			if reply.Status != testCase.expected.Status || !bytes.Equal(reply.Response, testCase.expected.Response) {
				t.Errorf("Expected reply to be %v, got %v\n", testCase.expected, reply)
			}
		})
	}
}

type Req struct {
	clientid  uint64
	seq       uint64
	operation uint64
	data      statemachines.KVPair
	expected  *rpc.ClientReply
}

func TestOneNodeClusterIdempotency(t *testing.T) {
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
			// set up single node cluster
			boltPath := filepath.Join(os.TempDir(), fmt.Sprintf("kvstore_%d", rand.Int()))
			defer os.Remove(boltPath)

			kvstore, err := statemachines.NewKVStoreMachine(boltPath)
			if err != nil {
				t.Error(err)
			}
			if kvstore == nil {
				t.Fail()
			}
			defer kvstore.Close()

			config := oneNodeClusterConfig()
			node, err := CreateNode(util.OpenPort(0), nil, config, kvstore, raft.NewMemoryStore())
			if err != nil {
				t.Errorf("Create Single Node fail: %v", err)
			}
			if node == nil {
				t.Error("Create single node returns nil node")
			}
			defer node.GracefulExit()
			time.Sleep(500 * time.Millisecond)

			// steps before final test
			for _, step := range testCase.steps {
				data, err := util.EncodeMsgPack(step.data)
				if err != nil {
					t.Fatalf("encoding kv pair fail: %v", err)
				}
				request := rpc.ClientRequest{
					ClientId:        step.clientid,
					SequenceNum:     step.seq,
					StateMachineCmd: step.operation,
					Data:            data,
				}
				reply := node.ClientRequest(&request)
				reply.LeaderHint = nil
				if step.expected != nil && (reply.Status != step.expected.Status || !bytes.Equal(reply.Response, step.expected.Response)) {
					t.Errorf("Expected reply to be %v, got %v\n", step.expected, &reply)
				}
			}
		})
	}
}

func oneNodeClusterConfig() *raft.Config {
	config := raft.DefaultConfig()
	config.ClusterSize = 1
	return config
}
