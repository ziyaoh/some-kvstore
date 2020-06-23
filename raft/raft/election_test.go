package raft

import (
	"testing"
	"time"

	"github.com/ziyaoh/some-kvstore/rpc"
	"github.com/ziyaoh/some-kvstore/util"
	"golang.org/x/net/context"
)

// Test making sure follower would behave correctly when handling RequestVote
func TestVote_Follower(t *testing.T) {
	util.SuppressLoggers()
	config := DefaultConfig()

	t.Run("Handle RequestVote with Stale Term", func(t *testing.T) {
		cluster, err := CreateLocalCluster(config)
		defer cleanupCluster(cluster)

		time.Sleep(2 * time.Second)

		if err != nil {
			t.Fatal(err)
		}

		leader, err := findLeader(cluster)
		if err != nil {
			t.Fatal(err)
		}
		followers, err := findAllFollowers(cluster)
		if err != nil {
			t.Fatal(err)
		}
		leader.setCurrentTerm(3)
		time.Sleep(1 * time.Second)

		// make sure the client get the correct response while registering itself with a candidate
		reply, _ := followers[0].RequestVoteCaller(context.Background(), &rpc.RequestVoteRequest{
			Term:         uint64(1),
			Candidate:    followers[1].Self,
			LastLogIndex: uint64(3),
			LastLogTerm:  uint64(1),
		})
		if reply.VoteGranted {
			t.Fatal("Should've denied vote")
		}
	})

	t.Run("Handle RequestVote with Higher Term", func(t *testing.T) {
		cluster, err := CreateLocalCluster(config)
		defer cleanupCluster(cluster)

		time.Sleep(2 * time.Second)

		if err != nil {
			t.Fatal(err)
		}

		leader, err := findLeader(cluster)
		if err != nil {
			t.Fatal(err)
		}
		followers, err := findAllFollowers(cluster)
		if err != nil {
			t.Fatal(err)
		}

		leader.leaderMutex.Lock()
		logEntry := &rpc.LogEntry{
			Index:  leader.LastLogIndex() + 1,
			TermId: leader.GetCurrentTerm(),
			Type:   rpc.CommandType_NOOP,
			Data:   []byte{1, 2, 3, 4},
		}
		leader.StoreLog(logEntry)
		leader.leaderMutex.Unlock()
		time.Sleep(1 * time.Second)

		reply, _ := followers[0].RequestVoteCaller(context.Background(), &rpc.RequestVoteRequest{
			Term:         uint64(3),
			Candidate:    followers[1].Self,
			LastLogIndex: uint64(1),
			LastLogTerm:  uint64(1),
		})
		if reply.VoteGranted {
			t.Fatal("Should've denied vote")
		}

		reply, _ = followers[0].RequestVoteCaller(context.Background(), &rpc.RequestVoteRequest{
			Term:         uint64(3),
			Candidate:    followers[1].Self,
			LastLogIndex: uint64(2),
			LastLogTerm:  uint64(1),
		})
		if !reply.VoteGranted {
			t.Fatal("Should've granted vote")
		}
	})
}

// Test making sure candidate would behave correctly when handling RequestVote
func TestVote_Candidate(t *testing.T) {
	util.SuppressLoggers()
	config := DefaultConfig()
	config.ClusterSize = 5

	cluster, err := CreateLocalCluster(config)
	defer cleanupCluster(cluster)

	time.Sleep(2 * time.Second)

	if err != nil {
		t.Fatal(err)
	}

	leader, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}
	leader.setCurrentTerm(3)
	leader.leaderMutex.Lock()
	logEntry := &rpc.LogEntry{
		Index:  leader.LastLogIndex() + 1,
		TermId: leader.GetCurrentTerm(),
		Type:   rpc.CommandType_NOOP,
		Data:   []byte{1, 2, 3, 4},
	}
	leader.StoreLog(logEntry)
	leader.leaderMutex.Unlock()
	time.Sleep(1 * time.Second)

	followers, err := findAllFollowers(cluster)
	if err != nil {
		t.Fatal(err)
	}
	if followers[0].GetCurrentTerm() != 3 {
		t.Fatalf("Term should've changed to %d but is %d", 3, followers[0].GetCurrentTerm())
	}

	followers[1].setCurrentTerm(3)
	followers[1].config.ElectionTimeout = 1 * time.Second
	followers[3].NetworkPolicy.PauseWorld(true)
	followers[2].NetworkPolicy.PauseWorld(true)
	leader.NetworkPolicy.PauseWorld(true)

	t.Run("Handle competing RequestVote with Stale Term", func(t *testing.T) {

		time.Sleep(500 * time.Millisecond)
		reply, _ := followers[0].RequestVoteCaller(context.Background(), &rpc.RequestVoteRequest{
			Term:         uint64(1),
			Candidate:    followers[1].Self,
			LastLogIndex: uint64(3),
			LastLogTerm:  uint64(1),
		})
		if reply.VoteGranted {
			t.Fatal("Should've denied vote")
		}
	})

	t.Run("Handle competing RequestVote with Higher Term and Out-of-date log", func(t *testing.T) {

		time.Sleep(500 * time.Millisecond)
		reply, _ := followers[0].RequestVoteCaller(context.Background(), &rpc.RequestVoteRequest{
			Term:         uint64(100),
			Candidate:    followers[1].Self,
			LastLogIndex: uint64(1),
			LastLogTerm:  uint64(1),
		})
		if reply.VoteGranted {
			t.Fatal("Should've denied vote")
		}
	})

	t.Run("Handle competing RequestVote with Higher Term and Up-to-date log", func(t *testing.T) {

		time.Sleep(500 * time.Millisecond)
		reply, _ := followers[0].RequestVoteCaller(context.Background(), &rpc.RequestVoteRequest{
			Term:         uint64(200),
			Candidate:    followers[1].Self,
			LastLogIndex: uint64(2),
			LastLogTerm:  uint64(3),
		})
		if !reply.VoteGranted {
			t.Fatal("Should've granted vote")
		}
	})
}

// Test making sure candidate would behave correctly when handling RequestVote
func TestVote_Leader(t *testing.T) {
	util.SuppressLoggers()
	config := DefaultConfig()
	cluster, err := CreateLocalCluster(config)
	defer cleanupCluster(cluster)

	time.Sleep(2 * time.Second)

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
	leader.setCurrentTerm(3)
	leader.leaderMutex.Lock()
	logEntry := &rpc.LogEntry{
		Index:  leader.LastLogIndex() + 1,
		TermId: leader.GetCurrentTerm(),
		Type:   rpc.CommandType_NOOP,
		Data:   []byte{1, 2, 3, 4},
	}
	leader.StoreLog(logEntry)
	leader.leaderMutex.Unlock()

	t.Run("Leader handle competing RequestVote with Stale Term", func(t *testing.T) {

		time.Sleep(500 * time.Millisecond)
		reply, _ := leader.RequestVoteCaller(context.Background(), &rpc.RequestVoteRequest{
			Term:         uint64(1),
			Candidate:    follower.Self,
			LastLogIndex: uint64(3),
			LastLogTerm:  uint64(1),
		})
		if reply.VoteGranted {
			t.Fatal("Should've denied vote")
		}
		if leader.State != LeaderState {
			t.Fatalf("Leader should've stayed in leader state but is %s", leader.State)
		}
	})

	t.Run("Leader handle competing RequestVote with Higher Term and Out-of-date log", func(t *testing.T) {

		time.Sleep(500 * time.Millisecond)
		reply, _ := leader.RequestVoteCaller(context.Background(), &rpc.RequestVoteRequest{
			Term:         uint64(100),
			Candidate:    follower.Self,
			LastLogIndex: uint64(1),
			LastLogTerm:  uint64(1),
		})
		if reply.VoteGranted {
			t.Fatal("Leader should've denied vote")
		}
	})

	t.Run("Handle competing RequestVote with Higher Term and Up-to-date log", func(t *testing.T) {

		time.Sleep(2 * time.Second)
		leader, err = findLeader(cluster)
		if err != nil {
			t.Fatal(err)
		}
		follower, err = findFollower(cluster)
		if err != nil {
			t.Fatal(err)
		}
		reply, _ := leader.RequestVoteCaller(context.Background(), &rpc.RequestVoteRequest{
			Term:         uint64(200),
			Candidate:    follower.Self,
			LastLogIndex: uint64(3),
			LastLogTerm:  uint64(150),
		})
		if !reply.VoteGranted {
			t.Fatal("Leader should've granted vote")
		}
	})
}
