package raft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/ziyaoh/some-kvstore/raft/statemachines"
	"github.com/ziyaoh/some-kvstore/rpc"
	"github.com/ziyaoh/some-kvstore/util"
	"golang.org/x/net/context"
)

func TestHandleHeartbeat_Follower(t *testing.T) {
	util.SuppressLoggers()
	config := DefaultConfig()
	cluster, err := CreateLocalCluster(config)
	defer cleanupCluster(cluster)

	time.Sleep(2 * time.Second)

	t.Run("Handle RequestVote with Stale Term", func(t *testing.T) {

		if err != nil {
			t.Fatal(err)
		}
		leader, err := findLeader(cluster)
		if err != nil {
			t.Fatal(err)
		}
		follower, err := findFollower(cluster)
		if err != nil {
			t.Fatal(err)
		}

		// make sure the client get the correct response while registering itself with a candidate
		reply, _ := follower.AppendEntriesCaller(context.Background(), &rpc.AppendEntriesRequest{
			Term:         uint64(1),
			Leader:       leader.Self,
			PrevLogIndex: uint64(3),
			PrevLogTerm:  uint64(1),
			Entries:      []*rpc.LogEntry{},
			LeaderCommit: uint64(5),
		})
		if reply.Success {
			t.Fatal("Should've denied vote")
		}
	})
}

func TestLeaderSendHeartbeatsInLaggyCase(t *testing.T) {
	util.SuppressLoggers()
	leader, _, err := MockLaggyCluster(true, DefaultConfig(), t)
	assert.Nil(t, err)

	time.Sleep(500 * time.Millisecond)
	request := rpc.ClientRequest{
		ClientId:        uint64(1),
		SequenceNum:     uint64(1),
		StateMachineCmd: statemachines.HashChainInit,
	}
	leader.ClientRequest(&request)
	time.Sleep(1 * time.Second)
}
