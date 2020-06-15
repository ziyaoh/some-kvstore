package raft

import (
	"testing"
	"time"

	"github.com/ziyaoh/some-kvstore/rpc"
	"golang.org/x/net/context"
)

func TestHandleHeartbeat_Follower(t *testing.T) {
	suppressLoggers()
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
