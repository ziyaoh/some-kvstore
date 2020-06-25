package replicationgroup

import (
	"math/rand"
	"testing"

	"github.com/ziyaoh/some-kvstore/rpc"
	"github.com/ziyaoh/some-kvstore/util"

	"github.com/stretchr/testify/assert"
)

func TestClientWithNormalLeader(t *testing.T) {
	util.SuppressLoggers()

	leader := CreateDefaultMockRGNode()
	leader.t = t
	defer leader.GracefulExit()

	addr := leader.Self.Addr
	client, err := Connect(rand.Uint64(), addr)
	assert.Nil(t, err, "Client connect to server fail")

	leader.expected = &rpc.ClientRequest{
		AckSeqs: []uint64{},
	}
	result, err := client.Get([]byte("key"))
	assert.Nil(t, err, "client request fail")
	assert.Equal(t, []byte("value"), result, "client Get result incorrect")

	leader.expected = &rpc.ClientRequest{
		AckSeqs: []uint64{0},
	}
	client.Get([]byte("key"))
}

func TestClientWithFailLeader(t *testing.T) {
	util.SuppressLoggers()

	normalLeader := CreateDefaultMockRGNode()
	defer normalLeader.GracefulExit()

	addr := normalLeader.Self.Addr
	client, err := Connect(rand.Uint64(), addr)
	client.Get([]byte("key"))

	failLeader := CreateFailMockRGNode()
	failLeader.t = t
	defer failLeader.GracefulExit()
	failLeader.expected = &rpc.ClientRequest{
		AckSeqs: []uint64{uint64(0)},
	}

	client.Leader = failLeader.Self
	result, err := client.Get([]byte("key"))
	assert.NotNil(t, err, "expect Get to error")
	assert.Nil(t, result, "expect result to be empty")

	// make sure client doesn't miss a to-be-ack seq in case request fail
	client.Get([]byte("key"))
}

func TestClientWithFollower(t *testing.T) {
	util.SuppressLoggers()

	leader := CreateDefaultMockRGNode()
	leader.t = t
	defer leader.GracefulExit()
	follower := CreateFollowerMockRGNode(leader.Self)
	defer follower.GracefulExit()

	addr := follower.Self.Addr
	client, err := Connect(rand.Uint64(), addr)
	assert.Nil(t, err, "Client connect to server fail")

	result, err := client.Get([]byte("key"))
	assert.Nil(t, err, "client request fail")
	assert.Equal(t, []byte("value"), result, "client Get result incorrect")

	leader.expected = &rpc.ClientRequest{
		AckSeqs: []uint64{0},
	}
	client.Leader = follower.Self
	client.Get([]byte("key"))
}

func TestClientWithStartingNode(t *testing.T) {
	util.SuppressLoggers()

	leader := CreateStartingMockRGNode()
	defer leader.GracefulExit()
	addr := leader.Self.Addr
	client, err := Connect(rand.Uint64(), addr)
	assert.Nil(t, err, "Client connect to server fail")

	result, err := client.Get([]byte("key"))
	assert.NotNil(t, err, "expect Get to error")
	assert.Nil(t, result, "expect result to be empty")
}

func TestClientWithCandidate(t *testing.T) {
	util.SuppressLoggers()

	candidate := CreateCandidateMockRGNode()
	defer candidate.GracefulExit()
	addr := candidate.Self.Addr
	client, err := Connect(rand.Uint64(), addr)
	assert.Nil(t, err, "Client connect to server fail")

	result, err := client.Get([]byte("key"))
	assert.Nil(t, err, "client request fail")
	assert.Equal(t, []byte("value"), result, "client Get result incorrect")
}
