package raft

import (
	"errors"
	"testing"
	"time"

	"github.com/ziyaoh/some-kvstore/rpc"
)

func TestPartition(t *testing.T) {
	suppressLoggers()

	config := DefaultConfig()
	config.ClusterSize = 5
	cluster, err := CreateLocalCluster(config)
	defer cleanupCluster(cluster)
	if err != nil {
		t.Fatal(err)
	}

	// wait for a leader to be elected
	time.Sleep(time.Second * WaitPeriod)
	leader, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}

	followers := make([]*Node, 0)
	for _, node := range cluster {
		if node != leader {
			followers = append(followers, node)
		}
	}

	// partition into 2 clusters: one with leader and first follower; other with remaining 3 followers
	ff := followers[0]
	for _, follower := range followers[1:] {
		follower.NetworkPolicy.RegisterPolicy(*follower.Self, *ff.Self, false)
		follower.NetworkPolicy.RegisterPolicy(*follower.Self, *leader.Self, false)

		ff.NetworkPolicy.RegisterPolicy(*ff.Self, *follower.Self, false)
		leader.NetworkPolicy.RegisterPolicy(*leader.Self, *follower.Self, false)
	}

	// allow a new leader to be elected in partition of 3 nodes
	time.Sleep(time.Second * WaitPeriod)
	newLeader, err := findLeader(followers)
	if err != nil {
		t.Fatal(err)
	}

	// check that old leader, which is cut off from new leader, still thinks it's leader
	if leader.State != LeaderState {
		t.Fatal(errors.New("leader should remain leader even when partitioned"))
	}

	if leader.GetCurrentTerm() >= newLeader.GetCurrentTerm() {
		t.Fatal(errors.New("new leader should have higher term"))
	}

	// add a new log entry to the old leader; should NOT be replicated
	leader.leaderMutex.Lock()
	logEntry := &rpc.LogEntry{
		Index:  leader.LastLogIndex() + 1,
		TermId: leader.GetCurrentTerm(),
		Type:   rpc.CommandType_NOOP,
		Data:   []byte{1, 2, 3, 4},
	}
	leader.StoreLog(logEntry)
	leader.leaderMutex.Unlock()

	// add a new log entry to the new leader; SHOULD be replicated
	newLeader.leaderMutex.Lock()
	logEntry = &rpc.LogEntry{
		Index:  newLeader.LastLogIndex() + 1,
		TermId: newLeader.GetCurrentTerm(),
		Type:   rpc.CommandType_NOOP,
		Data:   []byte{5, 6, 7, 8},
	}
	newLeader.StoreLog(logEntry)
	newLeader.leaderMutex.Unlock()

	time.Sleep(time.Second * WaitPeriod)

	// rejoin the cluster
	for _, follower := range followers[1:] {
		follower.NetworkPolicy.RegisterPolicy(*follower.Self, *ff.Self, true)
		follower.NetworkPolicy.RegisterPolicy(*follower.Self, *leader.Self, true)

		ff.NetworkPolicy.RegisterPolicy(*ff.Self, *follower.Self, true)
		leader.NetworkPolicy.RegisterPolicy(*leader.Self, *follower.Self, true)
	}

	// wait for larger cluster to stabilize
	time.Sleep(time.Second * WaitPeriod)

	if newLeader.State != LeaderState {
		t.Errorf("New leader should still be leader when old leader rejoins, but in %v state", newLeader.State)
	}

	if leader.State != FollowerState {
		t.Errorf("Old leader should fall back to the follower state after rejoining (was in %v state)", leader.State)
	}

	if !logsMatch(newLeader, cluster) {
		t.Errorf("logs incorrect")
	}
}
