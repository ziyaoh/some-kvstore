package raft

import (
	"math"
	"testing"
	"time"

	"github.com/ziyaoh/some-kvstore/rpc"
	"github.com/ziyaoh/some-kvstore/util"
)

// Leader is partitioned then rejoins back
func TestLeaderFailsAndRejoins(t *testing.T) {
	util.SuppressLoggers()

	config := DefaultConfig()
	config.ClusterSize = 5
	cluster, err := CreateLocalCluster(config)
	defer cleanupCluster(cluster)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for a leader to be elected
	time.Sleep(time.Second * WaitPeriod)
	leader, err := findLeader(cluster)
	if err != nil {
		t.Errorf("Failed to find leader: %v", err)
		return
	}

	followers := make([]*Node, 0)
	for _, node := range cluster {
		if node != leader {
			followers = append(followers, node)
		}
	}

	leader.NetworkPolicy.PauseWorld(true)

	// Wait until a new leader is selected in among the remaining four followers
	time.Sleep(time.Second * WaitPeriod)

	// Old leader should remain leader
	if leader.State != LeaderState {
		t.Errorf("Old leader should remain leader after partition")
		return
	}

	newLeaders := make([]*Node, 0)

	for _, follower := range followers {
		if follower.State == LeaderState {
			newLeaders = append(newLeaders, follower)
		}
	}

	// There should be only one new leader
	if len(newLeaders) != 1 {
		t.Errorf("The number of new leader is not correct")
		return
	}

	newLeader := newLeaders[0]
	newLeaderTerm := newLeader.GetCurrentTerm()
	// New leader's term shoud be greater than old leader's term
	if newLeader.GetCurrentTerm() <= leader.GetCurrentTerm() {
		t.Errorf("New leader's term shoud be greater than old leader's term")
		return
	}

	// Add a new log entry to the new leader; SHOULD be replicated
	newLeader.leaderMutex.Lock()
	logEntry := &rpc.LogEntry{
		Index:  newLeader.LastLogIndex() + 1,
		TermId: newLeader.GetCurrentTerm(),
		Type:   rpc.CommandType_NOOP,
		Data:   []byte{5, 6, 7, 8},
	}
	newLeader.StoreLog(logEntry)
	newLeader.leaderMutex.Unlock()

	// Wait until replicates to propagate
	time.Sleep(time.Second * WaitPeriod)

	// Leader rejoins
	leader.NetworkPolicy.PauseWorld(false)

	// Wait until stablized
	time.Sleep(time.Second * WaitPeriod)

	rejoinedLeader, err := findLeader(cluster)
	if err != nil {
		t.Errorf("Failed to find leader: %v", err)
		return
	}

	if leader.State != FollowerState && leader.GetCurrentTerm() <= newLeaderTerm {
		t.Errorf("Old leader should become a follower or at least have a higher term after rejoining back. leader's state is %v", leader.State)
		return
	}

	if leader.GetCurrentTerm() != rejoinedLeader.GetCurrentTerm() {
		t.Errorf("Old leader node should have the same term as the current leader")
		return
	}

	if leader.LastLogIndex() != rejoinedLeader.LastLogIndex() {
		t.Errorf("Old leader node should have the same last log index as the current leader")
		return
	}

	if rejoinedLeader != newLeader {
		t.Errorf("Leader after rejoining should be newLeader")
	}

	if rejoinedLeader.GetCurrentTerm() != newLeaderTerm {
		t.Errorf("The term of leader after rejoining should be the same as the newLeader")
		return
	}
}

// A follower is partitioned. While it is partitioned, an new log entry is added to leader and replicates. Then the follower rejoins.
func TestFollowerPartitionedAndRejoinWithNewLog(t *testing.T) {
	util.SuppressLoggers()

	config := DefaultConfig()
	config.ClusterSize = 5
	cluster, err := CreateLocalCluster(config)
	defer cleanupCluster(cluster)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for a leader to be elected
	time.Sleep(time.Second * WaitPeriod)

	leader, err := findLeader(cluster)
	if err != nil {
		t.Errorf("Failed to find leader: %v", err)
		return
	}

	followers := make([]*Node, 0)
	for _, node := range cluster {
		if node != leader {
			followers = append(followers, node)
		}
	}

	partitionedFollower := followers[0]

	partitionedFollower.NetworkPolicy.PauseWorld(true)

	// Add a new log entry to the leader;
	leader.leaderMutex.Lock()
	logEntry := &rpc.LogEntry{
		Index:  leader.LastLogIndex() + 1,
		TermId: leader.GetCurrentTerm(),
		Type:   rpc.CommandType_NOOP,
		Data:   []byte{1, 2, 3, 4},
	}
	leader.StoreLog(logEntry)
	leader.leaderMutex.Unlock()

	// Wait for the new log replicating
	time.Sleep(time.Second * WaitPeriod)

	if partitionedFollower.State != CandidateState {
		t.Errorf("Partitioned follower should become a candidate")
		return
	}

	if partitionedFollower.GetCurrentTerm() <= leader.GetCurrentTerm() {
		t.Errorf("Partitioned follower's term should be greater then the leader's")
		return
	}

	// The partitioned follower joins back
	partitionedFollower.NetworkPolicy.PauseWorld(false)

	// Wait until stablized
	time.Sleep(time.Second * WaitPeriod)

	if partitionedFollower.State != FollowerState {
		t.Errorf("The partitioned follower should remain a follower after it joining back and stablized")
		return
	}

	rejoinedLeader, err := findLeader(cluster)

	if rejoinedLeader.GetCurrentTerm() != partitionedFollower.GetCurrentTerm() {
		t.Errorf("The rejoined leader's term should be the same as the partitioned follower's current term")
		return
	}
}

func TestCandidateAndLeaderFallbackAfterPartition(t *testing.T) {
	util.SuppressLoggers()
	config := DefaultConfig()
	config.ClusterSize = 5
	cluster, err := CreateLocalCluster(config)
	defer cleanupCluster(cluster)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for a leader to be elected
	time.Sleep(time.Second * WaitPeriod)

	leader, err := findLeader(cluster)
	if err != nil {
		t.Errorf("Failed to find leader: %v", err)
		return
	}

	followers := make([]*Node, 0)
	for _, node := range cluster {
		if node != leader {
			followers = append(followers, node)
			node.NetworkPolicy.PauseWorld(true)
		}
	}

	superFollower := followers[0]

	// Wait followers to become candidates and increase their terms
	time.Sleep(time.Second * WaitPeriod)

	if leader.State != LeaderState {
		t.Errorf("Leader's current state should remain to be a leader")
		return
	}

	// Bring back superfollower first
	superFollower.NetworkPolicy.PauseWorld(false)

	if superFollower.GetCurrentTerm() <= leader.GetCurrentTerm() {
		t.Errorf("Leader's current term should be smaller than the rejoined follower's")
		return
	}

	// Wait for superfollower turns leader back to a follower
	time.Sleep(time.Second * WaitPeriod)

	if leader.State == LeaderState {
		t.Errorf("Old leader should fallback to a follower")
		return
	}

	// Set superfollower's term to a large number
	superFollower.setCurrentTerm(uint64(math.MaxUint64 / 2))

	// Bring back all other partitioned followers
	for _, node := range followers {
		if node != superFollower {
			node.NetworkPolicy.PauseWorld(false)
		}
	}

	// Wait until a new leader is elected
	time.Sleep(time.Second * WaitPeriod)

	newLeader, err := findLeader(cluster)
	if err != nil {
		t.Errorf("Failed to find leader: %v", err)
		return
	}

	if newLeader.GetCurrentTerm() < uint64(math.MaxUint64/2) {
		t.Errorf("New leader's term should be greater or equal to the large term we set")
		return
	}
}
