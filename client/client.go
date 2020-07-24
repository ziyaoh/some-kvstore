package client

import (
	"fmt"
	"time"

	errHelp "github.com/pkg/errors"
	"github.com/ziyaoh/some-kvstore/replicationgroup"
	"github.com/ziyaoh/some-kvstore/shardorchestrator"
	"github.com/ziyaoh/some-kvstore/util"
)

// MaxRetries is the maximum times the Client will retry a request after
// receiving a REQ_FAILED reply from the Raft cluster
const MaxRetries = 3
const retryInterval = 200 * time.Millisecond

// Client provides serveral API for user to use the some-kvstore
type Client struct {
	orchClient   *shardorchestrator.Client
	groupClients map[uint64]*replicationgroup.Client
}

// NewClient returns a new Client struct
func NewClient(orchAddr string) (*Client, error) {
	orchClient, err := shardorchestrator.ClientConnect(orchAddr)
	if err != nil {
		return nil, errHelp.Wrap(err, "Client: connect to shard orchestrator failed\n")
	}
	client := Client{
		orchClient:   orchClient,
		groupClients: map[uint64]*replicationgroup.Client{},
	}
	return &client, nil
}

func (client *Client) getReplicaGroupClient(key []byte) (*replicationgroup.Client, error) {
	config, err := client.orchClient.Query(int64(-1))
	if err != nil {
		return nil, errHelp.Wrap(err, "Client: query sharding configuration failed\n")
	}

	shard := util.KeyToShard(key)
	groupID := config.Location[shard]
	if groupID == uint64(0) {
		return nil, fmt.Errorf("Client: KVStore is starting with not replication group")
	}

	if groupClient, exist := client.groupClients[groupID]; exist {
		return groupClient, nil
	}

	addrs := config.Groups[groupID]
	var groupClient *replicationgroup.Client
	for _, addr := range addrs {
		groupClient, err = replicationgroup.Connect(client.orchClient.GetID(), addr)
		if err == nil {
			client.groupClients[groupID] = groupClient
			return groupClient, nil
		}
	}
	return nil, errHelp.Wrapf(err, "Client: client connect to group %v (%v) failed", groupID, addrs)
}

// Put puts a key/value pair to the connected kv store
func (client *Client) Put(key []byte, value []byte) error {
	var groupClient *replicationgroup.Client
	var err error
	for i := 0; i < MaxRetries; i++ {
		groupClient, err = client.getReplicaGroupClient(key)
		if err != nil {
			continue
		}
		_, err = groupClient.Put(key, value)
		if err == nil {
			return nil
		}
		time.Sleep(retryInterval)
	}
	return errHelp.Wrapf(err, "Client: Client Put failed, Key: %v, Value: %v\n", key, value)
}

// Get gets a value for a key from connect kv store
func (client *Client) Get(key []byte) ([]byte, error) {
	var groupClient *replicationgroup.Client
	var err error
	var value []byte
	for i := 0; i < MaxRetries; i++ {
		groupClient, err = client.getReplicaGroupClient(key)
		if err != nil {
			continue
		}
		value, err = groupClient.Get(key)
		if err == nil {
			return value, nil
		}
		time.Sleep(retryInterval)
	}
	return nil, errHelp.Wrapf(err, "Client: Client Get failed, Key: %v\n", key)
}

// Append appends a value to the end of the existing value for the key
func (client *Client) Append(key []byte, value []byte) error {
	var groupClient *replicationgroup.Client
	var err error
	for i := 0; i < MaxRetries; i++ {
		groupClient, err = client.getReplicaGroupClient(key)
		if err != nil {
			continue
		}
		_, err = groupClient.Append(key, value)
		if err == nil {
			return nil
		}
		time.Sleep(retryInterval)
	}
	return errHelp.Wrapf(err, "Client: Client Append failed, Key: %v, Value: %v\n", key, value)
}

// Delete deletes a key/value pair from the connected kv store
func (client *Client) Delete(key []byte) error {
	var groupClient *replicationgroup.Client
	var err error
	for i := 0; i < MaxRetries; i++ {
		groupClient, err = client.getReplicaGroupClient(key)
		if err != nil {
			continue
		}
		_, err = groupClient.Delete(key)
		if err == nil {
			return nil
		}
		time.Sleep(retryInterval)
	}
	return errHelp.Wrapf(err, "Client: Client Get failed, Key: %v\n", key)
}
