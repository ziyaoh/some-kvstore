package raft

import (
	"bytes"
	"testing"
	"time"

	"github.com/ziyaoh/some-kvstore/raft/statemachines"
	"github.com/ziyaoh/some-kvstore/rpc"
	"github.com/ziyaoh/some-kvstore/util"
)

// Example test making sure leaders can register the client and process the request from clients properly
func TestClientInteraction_Leader(t *testing.T) {
	util.SuppressLoggers()
	// SetDebug(true)
	config := DefaultConfig()
	cluster, _ := CreateLocalCluster(config)
	defer cleanupCluster(cluster)

	time.Sleep(2 * time.Second)

	leader, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}

	// First make sure we can register a client correctly
	reply := leader.RegisterClient(&rpc.RegisterClientRequest{})

	if reply.Status != rpc.ClientStatus_OK {
		t.Fatal("Counld not register client")
	}

	clientid := reply.ClientId

	// Hash initialization request
	initReq := rpc.ClientRequest{
		ClientId:        clientid,
		SequenceNum:     1,
		StateMachineCmd: statemachines.HashChainInit,
		Data:            []byte("hello"),
	}
	clientResult := leader.ClientRequest(&initReq)
	if clientResult.Status != rpc.ClientStatus_OK {
		t.Fatal("Leader failed to commit a client request")
	}

	// Make sure further request is correct processed
	ClientReq := rpc.ClientRequest{
		ClientId:        clientid,
		SequenceNum:     2,
		StateMachineCmd: statemachines.HashChainAdd,
		Data:            []byte{},
	}
	clientResult = leader.ClientRequest(&ClientReq)
	if clientResult.Status != rpc.ClientStatus_OK {
		t.Fatal("Leader failed to commit a client request")
	}

	clientDupResult := leader.ClientRequest(&ClientReq)
	if clientDupResult.Status != rpc.ClientStatus_OK || bytes.Compare(clientDupResult.Response, clientResult.Response) != 0 {
		t.Fatal("Leader failed to handle duplicate requests")
	}

	// Make sure further request is correct processed
	ClientNewReq := rpc.ClientRequest{
		ClientId:        clientid,
		SequenceNum:     3,
		StateMachineCmd: statemachines.HashChainAdd,
		Data:            []byte{},
	}

	resultChan := make(chan rpc.ClientReply)
	go func() {
		clientResult = leader.ClientRequest(&ClientNewReq)
		resultChan <- clientResult
	}()
	go func() {
		clientResult = leader.ClientRequest(&ClientNewReq)
		resultChan <- clientResult
	}()
	result1 := <-resultChan
	result2 := <-resultChan
	if result1.Status != result2.Status || bytes.Compare(result1.Response, result2.Response) != 0 {
		t.Fatal("Leader failed to handle simultanous duplicate requests")
	}
}

func TestClientInteraction_LeaderFallback(t *testing.T) {
	util.SuppressLoggers()
	// SetDebug(true)
	config := DefaultConfig()
	cluster, _ := CreateLocalCluster(config)
	defer cleanupCluster(cluster)

	time.Sleep(2 * time.Second)

	leader, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}

	// First make sure we can register a client correctly
	reply := leader.RegisterClient(&rpc.RegisterClientRequest{})

	if reply.Status != rpc.ClientStatus_OK {
		t.Fatal("Counld not register client")
	}

	clientid := reply.ClientId

	leader.NetworkPolicy.PauseWorld(true)

	registerChan := make(chan rpc.RegisterClientReply)
	go func() {
		registerResult := leader.RegisterClient(&rpc.RegisterClientRequest{})
		registerChan <- registerResult
	}()

	requestChan := make(chan rpc.ClientReply)
	go func() {
		// Hash initialization request
		initReq := rpc.ClientRequest{
			ClientId:        clientid,
			SequenceNum:     1,
			StateMachineCmd: statemachines.HashChainInit,
			Data:            []byte("hello"),
		}
		requestResult := leader.ClientRequest(&initReq)
		requestChan <- requestResult
	}()

	time.Sleep(2 * time.Second)
	leader.NetworkPolicy.PauseWorld(false)

	registerResult := <-registerChan
	if registerResult.Status != rpc.ClientStatus_NOT_LEADER {
		t.Error(registerResult.Status)
		t.Fatal("Wrong response for client registration when leader fallback to follower")
	}
	requestResult := <-requestChan
	if requestResult.Status != rpc.ClientStatus_NOT_LEADER {
		t.Error(requestResult.Status)
		t.Fatal("Wrong response for client request when leader fallback to follower")
	}
}

// Example test making sure the follower would reject the registration and requests from clients with correct messages
// The test on candidates can be similar with these sample tests
func TestClientInteraction_Follower(t *testing.T) {
	util.SuppressLoggers()
	config := DefaultConfig()
	// set the ElectionTimeout long enough to keep nodes in the state of follower
	config.ElectionTimeout = 60 * time.Second
	config.ClusterSize = 3
	config.HeartbeatTimeout = 500 * time.Millisecond
	cluster, err := CreateLocalCluster(config)
	defer cleanupCluster(cluster)

	time.Sleep(2 * time.Second)

	if err != nil {
		t.Fatal(err)
	}

	// make sure the client get the correct response while registering itself with a follower
	reply := cluster[0].RegisterClient(&rpc.RegisterClientRequest{})
	if reply.Status != rpc.ClientStatus_NOT_LEADER && reply.Status != rpc.ClientStatus_ELECTION_IN_PROGRESS {
		t.Error(reply.Status)
		t.Fatal("Wrong response when registering a client to a follower")
	}

	// make sure the client get the correct response while sending a request to a follower
	req := rpc.ClientRequest{
		ClientId:        1,
		SequenceNum:     1,
		StateMachineCmd: statemachines.HashChainInit,
		Data:            []byte("hello"),
	}
	clientResult := cluster[0].ClientRequest(&req)
	if clientResult.Status != rpc.ClientStatus_NOT_LEADER && clientResult.Status != rpc.ClientStatus_ELECTION_IN_PROGRESS {
		t.Fatal("Wrong response when sending a client request to a follower")
	}
}

// Example test making sure the candidate would reject the registration and requests from clients with correct messages
func TestClientInteraction_Candidate(t *testing.T) {
	util.SuppressLoggers()
	config := DefaultConfig()
	cluster, err := CreateLocalCluster(config)
	defer cleanupCluster(cluster)

	time.Sleep(2 * time.Second)

	if err != nil {
		t.Fatal(err)
	}

	follower, err := findFollower(cluster)
	if err != nil {
		t.Fatal(err)
	}

	// parition follower from leader to keep it in candidate state
	// follower.NetworkPolicy.RegisterPolicy(*leader.Self, *follower.Self, false)
	follower.NetworkPolicy.PauseWorld(true)
	time.Sleep(2 * time.Second)

	// make sure the client get the correct response while registering itself with a candidate
	reply := follower.RegisterClient(&rpc.RegisterClientRequest{})
	if reply.Status != rpc.ClientStatus_NOT_LEADER && reply.Status != rpc.ClientStatus_ELECTION_IN_PROGRESS {
		t.Error(reply.Status)
		t.Fatal("Wrong response when registering a client to a candidate")
	}

	// make sure the client get the correct response while sending a request to a candidate
	req := rpc.ClientRequest{
		ClientId:        1,
		SequenceNum:     1,
		StateMachineCmd: statemachines.HashChainInit,
		Data:            []byte("hello"),
	}
	clientResult := follower.ClientRequest(&req)
	if clientResult.Status != rpc.ClientStatus_NOT_LEADER && clientResult.Status != rpc.ClientStatus_ELECTION_IN_PROGRESS {
		t.Fatal("Wrong response when sending a client request to a candidate")
	}
}
