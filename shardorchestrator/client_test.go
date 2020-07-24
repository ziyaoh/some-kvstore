package shardorchestrator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ziyaoh/some-kvstore/rpc"
	"github.com/ziyaoh/some-kvstore/util"
)

func TestClientsWithNormalLeader(t *testing.T) {
	util.SuppressLoggers()

	leader := CreateDefaultMockSONode()
	leader.t = t
	defer leader.GracefulExit()

	addr := leader.Self.Addr
	t.Run("Client Interaction", func(t *testing.T) {
		client, err := ClientConnect(addr)
		assert.Nil(t, err, "Client connect to server fail")
		assert.Equal(t, uint64(1), client.requester.ID)

		leader.expected = &rpc.ClientRequest{
			AckSeqs: []uint64{},
		}
		result, err := client.Query(int64(-1))
		assert.Nil(t, err, "client request fail")
		assert.Equal(t, util.NewConfiguration(util.NumShards), result, "client Get result incorrect")

		leader.expected = &rpc.ClientRequest{
			AckSeqs: []uint64{0},
		}
		client.Query(int64(-1))
	})
	t.Run("Internal Client Interaction", func(t *testing.T) {
		internalClient, err := InternalClientConnect(addr)
		assert.Nil(t, err, "Client connect to server fail")
		assert.Equal(t, uint64(2), internalClient.requester.ID)

		leader.expected = &rpc.ClientRequest{
			AckSeqs: []uint64{},
		}
		result, err := internalClient.InternalQuery(int64(-1), uint64(1), []string{"0.0.0.0"})
		assert.Nil(t, err, "internal client request fail")
		assert.Equal(t, util.NewConfiguration(util.NumShards), result, "client Get result incorrect")

		leader.expected = &rpc.ClientRequest{
			AckSeqs: []uint64{0},
		}
		internalClient.InternalQuery(int64(-1), uint64(1), []string{"0.0.0.0"})
	})
	t.Run("Admin Interaction", func(t *testing.T) {
		admin, err := AdminConnect(addr)
		assert.Nil(t, err, "Admin connect to server fail")
		assert.Equal(t, uint64(3), admin.requester.ID)

		leader.expected = &rpc.ClientRequest{
			AckSeqs: []uint64{},
		}
		err = admin.Join(uint64(1), []string{"0.0.0.0"})
		assert.Nil(t, err, "internal client request fail")

		leader.expected = &rpc.ClientRequest{
			AckSeqs: []uint64{0},
		}
		config, err := admin.Query(int64(-1))
		assert.Nil(t, err)
		assert.Equal(t, util.NewConfiguration(util.NumShards), config, "Admin Query result incorrect")
	})
}

func TestClientsWithFailLeader(t *testing.T) {
	util.SuppressLoggers()

	normalLeader := CreateDefaultMockSONode()
	defer normalLeader.GracefulExit()

	failLeader := CreateFailMockSONode()
	failLeader.t = t
	defer failLeader.GracefulExit()
	failLeader.expected = &rpc.ClientRequest{
		AckSeqs: []uint64{uint64(0)},
	}

	addr := normalLeader.Self.Addr
	client, err := ClientConnect(addr)
	assert.Nil(t, err)
	client.Query(int64(-1))

	client.requester.Leader = failLeader.Self
	_, err = client.Query(int64(-1))
	assert.NotNil(t, err, "expect Get to error")

	// make sure client doesn't miss a to-be-ack seq in case request fail
	client.Query(int64(-1))
}

func TestClientWithFollower(t *testing.T) {
	util.SuppressLoggers()

	leader := CreateDefaultMockSONode()
	leader.t = t
	defer leader.GracefulExit()
	follower := CreateFollowerMockSONode(leader.Self)
	defer follower.GracefulExit()

	addr := follower.Self.Addr
	client, err := ClientConnect(addr)
	assert.Nil(t, err, "Client connect to server fail")

	result, err := client.Query(int64(-1))
	assert.Nil(t, err, "client request fail")
	assert.Equal(t, util.NewConfiguration(util.NumShards), result, "client Get result incorrect")

	leader.expected = &rpc.ClientRequest{
		AckSeqs: []uint64{0},
	}
	client.requester.Leader = follower.Self
	client.Query(int64(-1))
}

func TestClientWithStartingNode(t *testing.T) {
	util.SuppressLoggers()

	leader := CreateStartingMockSONode()
	defer leader.GracefulExit()
	addr := leader.Self.Addr
	_, err := ClientConnect(addr)
	assert.NotNil(t, err, "Expect Client connect to starting server fail")
}

func TestClientWithCandidate(t *testing.T) {
	util.SuppressLoggers()

	candidate := CreateCandidateMockSONode()
	defer candidate.GracefulExit()
	addr := candidate.Self.Addr
	client, err := ClientConnect(addr)
	assert.Nil(t, err, "Client connect to server fail")
	assert.Equal(t, uint64(1), client.requester.ID)

	candidate.expected = &rpc.ClientRequest{
		AckSeqs: []uint64{},
	}
	result, err := client.Query(int64(-1))
	assert.Nil(t, err, "client request fail")
	assert.Equal(t, util.NewConfiguration(util.NumShards), result, "client Get result incorrect")

	candidate.expected = &rpc.ClientRequest{
		AckSeqs: []uint64{0},
	}
	client.Query(int64(-1))
}
