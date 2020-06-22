package replicationgroup

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClientWithNormalLeader(t *testing.T) {
	leader := CreateDefaultMockRGNode()
	defer leader.GracefulExit()
	addr := leader.Self.Addr
	client, err := Connect(addr)
	assert.Nil(t, err, "Client connect to server fail")

	result, err := client.Get([]byte("key"))
	assert.Nil(t, err, "client request fail")
	assert.Equal(t, []byte("value"), result, "client Get result incorrect")
}

func TestClientWithFailLeader(t *testing.T) {
	leader := CreateFailMockRGNode()
	defer leader.GracefulExit()
	addr := leader.Self.Addr
	client, err := Connect(addr)
	assert.Nil(t, err, "Client connect to server fail")

	result, err := client.Get([]byte("key"))
	assert.NotNil(t, err, "expect Get to error")
	assert.Nil(t, result, "expect result to be empty")
}

func TestClientWithFollower(t *testing.T) {
	leader := CreateDefaultMockRGNode()
	defer leader.GracefulExit()
	follower := CreateFollowerMockRGNode(leader.Self)
	defer follower.GracefulExit()

	addr := follower.Self.Addr
	client, err := Connect(addr)
	assert.Nil(t, err, "Client connect to server fail")

	result, err := client.Get([]byte("key"))
	assert.Nil(t, err, "client request fail")
	assert.Equal(t, []byte("value"), result, "client Get result incorrect")
}

func TestClientWithStartingNode(t *testing.T) {
	leader := CreateStartingMockRGNode()
	defer leader.GracefulExit()
	addr := leader.Self.Addr
	client, err := Connect(addr)
	assert.Nil(t, err, "Client connect to server fail")

	result, err := client.Get([]byte("key"))
	assert.NotNil(t, err, "expect Get to error")
	assert.Nil(t, result, "expect result to be empty")
}

func TestClientWithCandidate(t *testing.T) {
	candidate := CreateCandidateMockRGNode()
	defer candidate.GracefulExit()
	addr := candidate.Self.Addr
	client, err := Connect(addr)
	assert.Nil(t, err, "Client connect to server fail")

	result, err := client.Get([]byte("key"))
	assert.Nil(t, err, "client request fail")
	assert.Equal(t, []byte("value"), result, "client Get result incorrect")
}
