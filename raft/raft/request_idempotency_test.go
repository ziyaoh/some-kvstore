package raft

import (
	"math/rand"
	"testing"
	"time"

	"github.com/ziyaoh/some-kvstore/raft/statemachines"

	"github.com/stretchr/testify/assert"
	"github.com/ziyaoh/some-kvstore/rpc"
	"github.com/ziyaoh/some-kvstore/util"
)

func TestIdempotencyBasic(t *testing.T) {
	util.SuppressLoggers()

	config := DefaultConfig()
	cluster, err := CreateLocalCluster(config)
	defer cleanupCluster(cluster)
	assert.Nilf(t, err, "create local raft cluster fail: %v", err)

	// Wait for a leader to be elected
	time.Sleep(time.Second * 1)
	leader, err := findLeader(cluster)
	assert.Nilf(t, err, "local cluster finding leader fail: %v", err)

	clientid := rand.Uint64()
	initRequest := rpc.ClientRequest{
		ClientId:        clientid,
		SequenceNum:     uint64(0),
		StateMachineCmd: statemachines.HashChainInit,
		Data:            []byte{1, 2, 3, 4},
		AckSeqs:         []uint64{},
	}
	initReply := leader.ClientRequest(&initRequest)
	assert.Equalf(t, rpc.ClientStatus_OK, initReply.Status, "client request fail: %v", initReply.Status)

	addRequest := rpc.ClientRequest{
		ClientId:        clientid,
		SequenceNum:     uint64(1),
		StateMachineCmd: statemachines.HashChainAdd,
		Data:            nil,
		AckSeqs:         []uint64{uint64(0)},
	}
	addReply := leader.ClientRequest(&addRequest)
	assert.Equalf(t, rpc.ClientStatus_OK, addReply.Status, "client request fail: %v", addReply.Status)
	oldResult := addReply.GetResponse()

	addReply = leader.ClientRequest(&addRequest)
	assert.Equalf(t, rpc.ClientStatus_OK, addReply.Status, "client request fail: %v", addReply.Status)
	assert.Equalf(t, oldResult, addReply.GetResponse(), "result should be the same")

	newAddRequest := rpc.ClientRequest{
		ClientId:        clientid,
		SequenceNum:     uint64(2),
		StateMachineCmd: statemachines.HashChainAdd,
		Data:            nil,
		AckSeqs:         []uint64{uint64(1)},
	}
	newAddReply := leader.ClientRequest(&newAddRequest)
	assert.Equalf(t, rpc.ClientStatus_OK, newAddReply.Status, "client request fail: %v", newAddReply.Status)
	newResult := newAddReply.GetResponse()
	assert.NotEqualf(t, oldResult, newResult, "hash should change after the second add")

	// a hacky test to make sure the cached client request is removed
	addReply = leader.ClientRequest(&addRequest)
	assert.Equalf(t, rpc.ClientStatus_OK, addReply.Status, "client request fail: %v", addReply.Status)
	assert.NotEqualf(t, oldResult, addReply.GetResponse(), "hash should not be the same with old result")
	assert.NotEqualf(t, newResult, addReply.GetResponse(), "hash should not be the same with new result")
}

func TestIdempotencyAcrossNodes(t *testing.T) {
	util.SuppressLoggers()

	config := DefaultConfig()
	cluster, err := CreateLocalCluster(config)
	defer cleanupCluster(cluster)
	assert.Nilf(t, err, "create local raft cluster fail: %v", err)

	// Wait for a leader to be elected
	time.Sleep(time.Second * 1)
	leader, err := findLeader(cluster)
	assert.Nilf(t, err, "local cluster finding leader fail: %v", err)
	followers, err := findAllFollowers(cluster)
	assert.Nilf(t, err, "local cluster finding all followers fail: %v", err)

	clientid := rand.Uint64()
	initRequest := rpc.ClientRequest{
		ClientId:        clientid,
		SequenceNum:     uint64(0),
		StateMachineCmd: statemachines.HashChainInit,
		Data:            []byte{1, 2, 3, 4},
		AckSeqs:         []uint64{},
	}
	initReply := leader.ClientRequest(&initRequest)
	assert.Equalf(t, rpc.ClientStatus_OK, initReply.Status, "client request fail: %v", initReply.Status)

	addRequest := rpc.ClientRequest{
		ClientId:        clientid,
		SequenceNum:     uint64(1),
		StateMachineCmd: statemachines.HashChainAdd,
		Data:            nil,
		AckSeqs:         []uint64{uint64(0)},
	}
	addReply := leader.ClientRequest(&addRequest)
	assert.Equalf(t, rpc.ClientStatus_OK, addReply.Status, "client request fail: %v", addReply.Status)
	oldResult := addReply.GetResponse()

	leader.NetworkPolicy.PauseWorld(true)
	time.Sleep(1 * time.Second)
	newLeader, err := findLeader(followers)
	assert.Nilf(t, err, "local cluster finding new leader fail: %v", err)

	addReply = newLeader.ClientRequest(&addRequest)
	assert.Equalf(t, rpc.ClientStatus_OK, addReply.Status, "client request fail: %v", addReply.Status)
	assert.Equalf(t, oldResult, addReply.GetResponse(), "idempotency cache does not work across different leaders")

	newAddRequest := rpc.ClientRequest{
		ClientId:        clientid,
		SequenceNum:     uint64(2),
		StateMachineCmd: statemachines.HashChainAdd,
		Data:            nil,
		AckSeqs:         []uint64{uint64(1)},
	}
	newAddReply := newLeader.ClientRequest(&newAddRequest)
	assert.Equalf(t, rpc.ClientStatus_OK, newAddReply.Status, "client request fail: %v", newAddReply.Status)
	newResult := newAddReply.GetResponse()
	assert.NotEqualf(t, oldResult, newResult, "hash should change after the second add")

	leader.NetworkPolicy.PauseWorld(false)
	time.Sleep(1 * time.Second)
	// try to change leader
	newestLeader, err := findLeader(cluster)
	assert.Nilf(t, err, "local cluster finding newest leader fail: %v", err)
	for newestLeader.Self.Id == newLeader.Self.Id {
		leader.setCurrentTerm(leader.GetCurrentTerm() + uint64(100))
		time.Sleep(1 * time.Second)
		newestLeader, err = findLeader(cluster)
		assert.Nilf(t, err, "local cluster finding newest leader fail: %v", err)
	}

	// a hacky test to make sure the cached client request is removed on the newest leader as well
	addReply = newestLeader.ClientRequest(&addRequest)
	assert.Equalf(t, rpc.ClientStatus_OK, addReply.Status, "client request fail: %v", addReply.Status)
	assert.NotEqualf(t, oldResult, addReply.GetResponse(), "hash should not be the same with old result")
	assert.NotEqualf(t, newResult, addReply.GetResponse(), "hash should not be the same with new result")
}
