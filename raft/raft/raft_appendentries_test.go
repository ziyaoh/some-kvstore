package raft

import (
	"testing"
	"time"

	"github.com/ziyaoh/some-kvstore/raft/statemachines"
	"golang.org/x/net/context"
)

// Test making sure leaders can register the client and process the request from clients properly
// Making sure that heartbeats replicate logs correctly under multiple partition scenarios
func TestAppendEntriesFromClient(t *testing.T) {
	suppressLoggers()
	config := DefaultConfig()
	config.ClusterSize = 7
	cluster, _ := CreateLocalCluster(config)
	defer cleanupCluster(cluster)

	time.Sleep(2 * time.Second)

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

	// First make sure we can register a client correctly
	reply, _ := leader.RegisterClientCaller(context.Background(), &RegisterClientRequest{})

	if reply.Status != ClientStatus_OK {
		t.Fatal("Counld not register client")
	}

	clientid := reply.ClientId

	data := make([]byte, 5)
	for j := 0; j < 5; j++ {
		data[j] = byte(j)
	}
	cliReq := ClientRequest{
		ClientId:        clientid,
		SequenceNum:     uint64(1),
		StateMachineCmd: statemachines.HashChainInit,
		Data:            data,
	}

	clientResult, _ := leader.ClientRequestCaller(context.Background(), &cliReq)
	if clientResult.Status != ClientStatus_OK {
		t.Errorf("Leader failed to commit a client request hash chain init")
		return
	}

	for i := 2; i < 5; i++ {
		cliReq := ClientRequest{
			ClientId:        clientid,
			SequenceNum:     uint64(i),
			StateMachineCmd: statemachines.HashChainAdd,
			Data:            []byte{},
		}

		clientResult, _ := leader.ClientRequestCaller(context.Background(), &cliReq)
		if clientResult.Status != ClientStatus_OK {
			t.Errorf("Leader failed to commit a client request %v", i)
			return
		}
	}

	time.Sleep(5 * time.Second)

	for _, node := range followers {
		if leader.LastLogIndex() != node.LastLogIndex() {
			t.Errorf("Leader's log does not match other nodes' log")
			return
		}
		if leader.GetCurrentTerm() != node.GetCurrentTerm() {
			t.Errorf("Leader's current term does not match other nodes' term")
			return
		}
	}

	if !logsMatch(leader, followers) {
		t.Errorf("Leader's log does not match other nodes' log")
		return
	}

	followers[0].NetworkPolicy.PauseWorld(true)
	for i := 5; i < 7; i++ {
		cliReq := ClientRequest{
			ClientId:        clientid,
			SequenceNum:     uint64(i),
			StateMachineCmd: statemachines.HashChainAdd,
			Data:            []byte{},
		}

		clientResult, _ := leader.ClientRequestCaller(context.Background(), &cliReq)
		if clientResult.Status != ClientStatus_OK {
			t.Errorf("Leader failed to commit a client request %v", i)
			return
		}
	}

	time.Sleep(5 * time.Second)
	if !logsMatch(leader, followers[1:]) {
		t.Error("Leader's log does not match followers'")
		return
	}
	if logsMatch(leader, followers[:1]) {
		t.Errorf("Leader's log should not match partitioned follower's")
		return
	}

	leader.NetworkPolicy.PauseWorld(true)
	go func() {
		for i := 7; i < 9; i++ {
			cliReq := ClientRequest{
				ClientId:        clientid,
				SequenceNum:     uint64(i),
				StateMachineCmd: statemachines.HashChainAdd,
				Data:            []byte{},
			}

			clientResult, _ := leader.ClientRequestCaller(context.Background(), &cliReq)
			if clientResult.Status == ClientStatus_OK {
				t.Errorf("Old leader should have failed to commit a client request %v", i)
				return
			}
		}
	}()

	time.Sleep(5 * time.Second)
	var newLeader *Node
	for _, node := range followers {
		if node.State == LeaderState {
			newLeader = node
			break
		}
	}
	for i := 9; i < 11; i++ {
		cliReq := ClientRequest{
			ClientId:        clientid,
			SequenceNum:     uint64(i),
			StateMachineCmd: statemachines.HashChainAdd,
			Data:            []byte{},
		}

		clientResult, _ := newLeader.ClientRequestCaller(context.Background(), &cliReq)
		if clientResult.Status != ClientStatus_OK {
			t.Errorf("Leader failed to commit a client request %v", i)
			return
		}
	}

	followers[0].NetworkPolicy.PauseWorld(false)
	leader.NetworkPolicy.PauseWorld(false)
	time.Sleep(5 * time.Second)
	if !logsMatch(followers[0], followers) {
		t.Errorf("All nodes' log should match")
		return
	}
}

// Test the AppendEntriesRequest with the term lower than the follower's term
func TestAppendEntriesTerm(t *testing.T) {
	config := DefaultConfig()
	config.ClusterSize = 3
	cluster, _ := CreateLocalCluster(config)
	defer cleanupCluster(cluster)

	time.Sleep(2 * time.Second)

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

	prevLogIndex := leader.LastLogIndex()
	prevLogEntry := leader.GetLog(prevLogIndex)
	prevLogTerm := prevLogEntry.GetTermId()
	request := AppendEntriesRequest{
		Term:         0,
		Leader:       leader.Self,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      nil,
		LeaderCommit: leader.commitIndex,
	}

	reply, err := followers[0].Self.AppendEntriesRPC(leader, &request)

	if err != nil {
		t.Fatal(err)
	}

	if reply.Success && reply.Term != leader.GetCurrentTerm() {
		t.Fatal("AppendEntriesRequest should be rejected")
	}
}

// Test AppendEntriesRequest with the prevLogIndex higher the follower's lastLogIndex
func TestAppendEntriesLogIndex(t *testing.T) {
	config := DefaultConfig()
	config.ClusterSize = 3
	cluster, _ := CreateLocalCluster(config)
	defer cleanupCluster(cluster)

	time.Sleep(2 * time.Second)

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

	prevLogIndex := leader.LastLogIndex()
	prevLogEntry := leader.GetLog(prevLogIndex)
	prevLogTerm := prevLogEntry.TermId
	request := AppendEntriesRequest{
		Term:         leader.GetCurrentTerm(),
		Leader:       leader.Self,
		PrevLogIndex: prevLogIndex + 100,
		PrevLogTerm:  prevLogTerm,
		Entries:      nil,
		LeaderCommit: leader.commitIndex,
	}

	reply, err := followers[0].Self.AppendEntriesRPC(leader, &request)

	if err != nil {
		t.Fatal(err)
	}

	if reply.Success && reply.Term != leader.GetCurrentTerm() {
		t.Fatal("AppendEntriesRequest should be rejected")
	}
}

// Test AppendEntriesRequest with PrevLogIndex and PrevLogTerm not matching
func TestAppendEntriesLogTerm(t *testing.T) {
	config := DefaultConfig()
	config.ClusterSize = 3
	cluster, _ := CreateLocalCluster(config)
	defer cleanupCluster(cluster)

	time.Sleep(2 * time.Second)

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

	prevLogIndex := leader.LastLogIndex()
	prevLogEntry := leader.GetLog(prevLogIndex)
	prevLogTerm := prevLogEntry.TermId
	request := AppendEntriesRequest{
		Term:         leader.GetCurrentTerm(),
		Leader:       leader.Self,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm + 100,
		Entries:      nil,
		LeaderCommit: leader.commitIndex,
	}

	reply, err := followers[0].Self.AppendEntriesRPC(leader, &request)

	if err != nil {
		t.Fatal(err)
	}

	if reply.Success != false && reply.Term != leader.GetCurrentTerm() {
		t.Fatal("AppendEntriesRequest should be rejected")
	}
}
