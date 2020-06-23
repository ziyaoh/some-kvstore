package replicationgroup

import (
	"errors"
	"fmt"
	"math/rand"
	"time"

	errHelp "github.com/pkg/errors"
	"github.com/ziyaoh/some-kvstore/raft/statemachines"
	"github.com/ziyaoh/some-kvstore/rpc"
	"github.com/ziyaoh/some-kvstore/util"
)

// Client represents a client that connects to a known node in the replication group
// to issue commands. It can be used by a CLI, web app, or other application to
// interface with Raft.
type Client struct {
	ID          uint64          // Client ID, determined by the Raft node
	Leader      *rpc.RemoteNode // Raft node we're connected to (also last known leader)
	SequenceNum uint64          // Sequence number of the latest request sent by the client
}

// MaxRetries is the maximum times the Client will retry a request after
// receiving a REQ_FAILED reply from the Raft cluster
const MaxRetries = 3

// Connect creates a new Client without actually connect to the address, assuming that it works
func Connect(addr string) (cp *Client, err error) {
	cp = new(Client)

	// Note: we don't yet know the ID of the remoteNode, so just set it to an
	// empty string.
	remoteNode := &rpc.RemoteNode{Id: "", Addr: addr}

	// We've registered with the leader!
	// TODO: Id should be known after registering to shard master
	cp.ID = rand.Uint64()
	cp.Leader = remoteNode

	return
}

// Put puts a key/value pair to the connected replication group
func (c *Client) Put(key []byte, value []byte) ([]byte, error) {
	kvPair := statemachines.KVPair{
		Key:   key,
		Value: value,
	}
	bytes, err := util.EncodeMsgPack(kvPair)
	if err != nil {
		return nil, errHelp.Wrapf(err, "Client: Put encoding kvPair fail: %v\n", kvPair)
	}
	return c.sendRequest(statemachines.KVStorePut, bytes)
}

// Get gets a value for a key from the connected replication group
func (c *Client) Get(key []byte) ([]byte, error) {
	kvPair := statemachines.KVPair{
		Key:   key,
		Value: nil,
	}
	bytes, err := util.EncodeMsgPack(kvPair)
	if err != nil {
		return nil, errHelp.Wrapf(err, "Client: Get encoding kvPair fail: %v\n", kvPair)
	}
	return c.sendRequest(statemachines.KVStoreGet, bytes)
}

// Append appends a value to the end of the existing value for the key in the connected replication group
func (c *Client) Append(key []byte, value []byte) ([]byte, error) {
	kvPair := statemachines.KVPair{
		Key:   key,
		Value: value,
	}
	bytes, err := util.EncodeMsgPack(kvPair)
	if err != nil {
		return nil, errHelp.Wrapf(err, "Client: Append encoding kvPair fail: %v\n", kvPair)
	}
	return c.sendRequest(statemachines.KVStoreAppend, bytes)
}

// Delete deletes a key/value pair from the connected replication group
func (c *Client) Delete(key []byte) ([]byte, error) {
	kvPair := statemachines.KVPair{
		Key:   key,
		Value: nil,
	}
	bytes, err := util.EncodeMsgPack(kvPair)
	if err != nil {
		return nil, errHelp.Wrapf(err, "Client: Delete encoding kvPair fail: %v\n", kvPair)
	}
	return c.sendRequest(statemachines.KVStoreDelete, bytes)
}

// sendRequest sends a command the associated data to the last known leader of
// the Raft cluster, and handles responses.
func (c *Client) sendRequest(command uint64, data []byte) ([]byte, error) {
	request := rpc.ClientRequest{
		ClientId:        c.ID,
		SequenceNum:     c.SequenceNum,
		StateMachineCmd: command,
		Data:            data,
	}

	c.SequenceNum++

	var reply *rpc.ClientReply
	var err error
	retries := 0

	for retries < MaxRetries {
		reply, err = c.Leader.ClientRequestRPC(&request)
		if err != nil {
			return nil, err
		}

		switch reply.Status {
		case rpc.ClientStatus_OK:
			fmt.Printf("%v is the leader\n", c.Leader)
			fmt.Printf("Request returned \"%v\"\n", reply.Response)
			return reply.GetResponse(), nil
		case rpc.ClientStatus_REQ_FAILED:
			fmt.Printf("Request failed: %v\n", reply.Response)
			fmt.Println("Retrying...")
			retries++
		case rpc.ClientStatus_NOT_LEADER:
			// The person we've contacted isn't the leader. Use their hint to find
			// the leader.
			c.Leader = reply.LeaderHint
		case rpc.ClientStatus_ELECTION_IN_PROGRESS:
			// An election is in progress. Accept the hint and wait an appropriate
			// amount of time, so the election can finish.
			c.Leader = reply.LeaderHint
			time.Sleep(time.Millisecond * 200)
		case rpc.ClientStatus_CLUSTER_NOT_STARTED:
			return nil, errors.New("cluster hasn't started")
		}
	}

	return nil, errors.New("request failed")
}
