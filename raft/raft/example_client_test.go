package raft

import (
	"bytes"
	"testing"
	"time"

	"github.com/ziyaoh/some-kvstore/raft/statemachines"
	"golang.org/x/net/context"
)

// Example test making sure leaders can register the client and process the request from clients properly
func TestClientInteraction_Leader(t *testing.T) {
	suppressLoggers()
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
	reply, _ := leader.RegisterClientCaller(context.Background(), &RegisterClientRequest{})

	if reply.Status != ClientStatus_OK {
		t.Fatal("Counld not register client")
	}

	clientid := reply.ClientId

	// Hash initialization request
	initReq := ClientRequest{
		ClientId:        clientid,
		SequenceNum:     1,
		StateMachineCmd: hashmachine.HashChainInit,
		Data:            []byte("hello"),
	}
	clientResult, _ := leader.ClientRequestCaller(context.Background(), &initReq)
	if clientResult.Status != ClientStatus_OK {
		t.Fatal("Leader failed to commit a client request")
	}

	// Make sure further request is correct processed
	ClientReq := ClientRequest{
		ClientId:        clientid,
		SequenceNum:     2,
		StateMachineCmd: hashmachine.HashChainAdd,
		Data:            []byte{},
	}
	clientResult, _ = leader.ClientRequestCaller(context.Background(), &ClientReq)
	if clientResult.Status != ClientStatus_OK {
		t.Fatal("Leader failed to commit a client request")
	}

	clientDupResult, _ := leader.ClientRequestCaller(context.Background(), &ClientReq)
	if clientDupResult.Status != ClientStatus_OK || bytes.Compare(clientDupResult.Response, clientResult.Response) != 0 {
		t.Fatal("Leader failed to handle duplicate requests")
	}

	// Make sure further request is correct processed
	ClientNewReq := ClientRequest{
		ClientId:        clientid,
		SequenceNum:     3,
		StateMachineCmd: hashmachine.HashChainAdd,
		Data:            []byte{},
	}

	resultChan := make(chan *ClientReply)
	go func() {
		clientResult, _ = leader.ClientRequestCaller(context.Background(), &ClientNewReq)
		resultChan <- clientResult
	}()
	go func() {
		clientResult, _ = leader.ClientRequestCaller(context.Background(), &ClientNewReq)
		resultChan <- clientResult
	}()
	result1 := <-resultChan
	result2 := <-resultChan
	if result1.Status != result2.Status || bytes.Compare(result1.Response, result2.Response) != 0 {
		t.Fatal("Leader failed to handle simultanous duplicate requests")
	}
}

// Example test making sure the follower would reject the registration and requests from clients with correct messages
// The test on candidates can be similar with these sample tests
func TestClientInteraction_Follower(t *testing.T) {
	suppressLoggers()
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
	reply, _ := cluster[0].RegisterClientCaller(context.Background(), &RegisterClientRequest{})
	if reply.Status != ClientStatus_NOT_LEADER && reply.Status != ClientStatus_ELECTION_IN_PROGRESS {
		t.Error(reply.Status)
		t.Fatal("Wrong response when registering a client to a follower")
	}

	// make sure the client get the correct response while sending a request to a follower
	req := ClientRequest{
		ClientId:        1,
		SequenceNum:     1,
		StateMachineCmd: hashmachine.HashChainInit,
		Data:            []byte("hello"),
	}
	clientResult, _ := cluster[0].ClientRequestCaller(context.Background(), &req)
	if clientResult.Status != ClientStatus_NOT_LEADER && clientResult.Status != ClientStatus_ELECTION_IN_PROGRESS {
		t.Fatal("Wrong response when sending a client request to a follower")
	}
}

// Example test making sure the candidate would reject the registration and requests from clients with correct messages
func TestClientInteraction_Candidate(t *testing.T) {
	suppressLoggers()
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
	reply, _ := follower.RegisterClientCaller(context.Background(), &RegisterClientRequest{})
	if reply.Status != ClientStatus_NOT_LEADER && reply.Status != ClientStatus_ELECTION_IN_PROGRESS {
		t.Error(reply.Status)
		t.Fatal("Wrong response when registering a client to a candidate")
	}

	// make sure the client get the correct response while sending a request to a candidate
	req := ClientRequest{
		ClientId:        1,
		SequenceNum:     1,
		StateMachineCmd: hashmachine.HashChainInit,
		Data:            []byte("hello"),
	}
	clientResult, _ := follower.ClientRequestCaller(context.Background(), &req)
	if clientResult.Status != ClientStatus_NOT_LEADER && clientResult.Status != ClientStatus_ELECTION_IN_PROGRESS {
		t.Fatal("Wrong response when sending a client request to a candidate")
	}
}
