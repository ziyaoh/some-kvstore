package replicationgroup

import (
	errHelp "github.com/pkg/errors"
	"github.com/ziyaoh/some-kvstore/statemachines"
	"github.com/ziyaoh/some-kvstore/util"
)

// Client represents a client that connects to a known node in the replication group
// to issue commands. It can be used by a CLI, web app, or other application to
// interface with Raft.
type Client struct {
	requester *util.Requester
}

// Connect creates a new Client without actually connect to the address, assuming that it works
func Connect(id uint64, addr string) (cp *Client, err error) {
	cp = new(Client)
	requester, err := util.ConnectReplicationGroup(id, addr)
	if err != nil {
		return nil, errHelp.Wrap(err, "Client: connecting to Replication Group fail\n")
	}
	cp.requester = requester

	return cp, nil
}

// Put puts a key/value pair to the connected replication group
func (client *Client) Put(key []byte, value []byte) ([]byte, error) {
	kvPair := statemachines.KVPair{
		Key:   key,
		Value: value,
	}
	bytes, err := util.EncodeMsgPack(kvPair)
	if err != nil {
		return nil, errHelp.Wrapf(err, "Client: Put encoding kvPair fail: %v\n", kvPair)
	}
	payloadData := statemachines.KVStoreCommandPayload{
		Command: statemachines.KVStorePut,
		Data:    bytes,
	}
	payloadBytes, err := util.EncodeMsgPack(payloadData)
	if err != nil {
		return nil, errHelp.Wrapf(err, "Client: Put encoding KVStoreCommandPayload fail: %v\n", kvPair)
	}
	return client.sendRequest(statemachines.KVStoreCommand, payloadBytes)
}

// Get gets a value for a key from the connected replication group
func (client *Client) Get(key []byte) ([]byte, error) {
	kvPair := statemachines.KVPair{
		Key:   key,
		Value: nil,
	}
	bytes, err := util.EncodeMsgPack(kvPair)
	if err != nil {
		return nil, errHelp.Wrapf(err, "Client: Get encoding kvPair fail: %v\n", kvPair)
	}
	// return client.sendRequest(statemachines.KVStoreGet, bytes)
	payloadData := statemachines.KVStoreCommandPayload{
		Command: statemachines.KVStoreGet,
		Data:    bytes,
	}
	payloadBytes, err := util.EncodeMsgPack(payloadData)
	if err != nil {
		return nil, errHelp.Wrapf(err, "Client: Get encoding KVStoreCommandPayload fail: %v\n", kvPair)
	}
	return client.sendRequest(statemachines.KVStoreCommand, payloadBytes)
}

// Append appends a value to the end of the existing value for the key in the connected replication group
func (client *Client) Append(key []byte, value []byte) ([]byte, error) {
	kvPair := statemachines.KVPair{
		Key:   key,
		Value: value,
	}
	bytes, err := util.EncodeMsgPack(kvPair)
	if err != nil {
		return nil, errHelp.Wrapf(err, "Client: Append encoding kvPair fail: %v\n", kvPair)
	}
	// return client.sendRequest(statemachines.KVStoreAppend, bytes)
	payloadData := statemachines.KVStoreCommandPayload{
		Command: statemachines.KVStoreAppend,
		Data:    bytes,
	}
	payloadBytes, err := util.EncodeMsgPack(payloadData)
	if err != nil {
		return nil, errHelp.Wrapf(err, "Client: Append encoding KVStoreCommandPayload fail: %v\n", kvPair)
	}
	return client.sendRequest(statemachines.KVStoreCommand, payloadBytes)
}

// Delete deletes a key/value pair from the connected replication group
func (client *Client) Delete(key []byte) ([]byte, error) {
	kvPair := statemachines.KVPair{
		Key:   key,
		Value: nil,
	}
	bytes, err := util.EncodeMsgPack(kvPair)
	if err != nil {
		return nil, errHelp.Wrapf(err, "Client: Delete encoding kvPair fail: %v\n", kvPair)
	}
	// return client.sendRequest(statemachines.KVStoreDelete, bytes)
	payloadData := statemachines.KVStoreCommandPayload{
		Command: statemachines.KVStoreDelete,
		Data:    bytes,
	}
	payloadBytes, err := util.EncodeMsgPack(payloadData)
	if err != nil {
		return nil, errHelp.Wrapf(err, "Client: Delete encoding KVStoreCommandPayload fail: %v\n", kvPair)
	}
	return client.sendRequest(statemachines.KVStoreCommand, payloadBytes)
}

func (client *Client) sendRequest(command uint64, data []byte) ([]byte, error) {
	return client.requester.SendRequest(command, data)
}
