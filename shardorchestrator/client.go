package shardorchestrator

import (
	errHelp "github.com/pkg/errors"
	"github.com/ziyaoh/some-kvstore/statemachines"
	"github.com/ziyaoh/some-kvstore/util"
)

// Admin represents a client that connects to a known node in the shard orchestrator
// to issue commands. It can be used by a CLI, web app, or other application to
// interface with Raft.
type Admin struct {
	requester *util.Requester
}

// AdminConnect creates a new Admin for ShardOrchestrator and registers with the Raft node at the given address.
func AdminConnect(addr string) (*Admin, error) {
	admin := new(Admin)
	requester, err := util.ConnectShardOrchestrator(addr)
	if err != nil {
		return nil, errHelp.Wrap(err, "Client: connecting to ShardOrchestrator fail\n")
	}
	admin.requester = requester

	return admin, nil
}

func (admin *Admin) Join(groupID uint64, addrs []string) error {
	payload := statemachines.ConfigJoinPayload{
		GroupID: groupID,
		Addrs:   addrs,
	}
	bytes, err := util.EncodeMsgPack(payload)
	if err != nil {
		return errHelp.Wrapf(err, "Admin: Join encoding ConfigJoinPayload fail: %v\n", err)
	}
	_, err = admin.requester.SendRequest(statemachines.ConfigJoin, bytes)
	return err
}

func (admin *Admin) Leave(groupID uint64) error {
	payload := statemachines.ConfigLeavePayload{GroupID: groupID}
	bytes, err := util.EncodeMsgPack(payload)
	if err != nil {
		return errHelp.Wrapf(err, "Admin: Leave encoding ConfigLeavePayload fail: %v\n", err)
	}
	_, err = admin.requester.SendRequest(statemachines.ConfigLeave, bytes)
	return err
}

func (admin *Admin) Move(shard int, destGroup uint64) error {
	payload := statemachines.ConfigMovePayload{
		Shard:     shard,
		DestGroup: destGroup,
	}
	bytes, err := util.EncodeMsgPack(payload)
	if err != nil {
		return errHelp.Wrapf(err, "Admin: Move encoding ConfigMovePayload fail: %v\n", err)
	}
	_, err = admin.requester.SendRequest(statemachines.ConfigMove, bytes)
	return err
}

func (admin *Admin) Query(shardVersion int64) (util.Configuration, error) {
	var config util.Configuration

	payload := statemachines.ConfigQueryPayload{ShardVersion: shardVersion}
	bytes, err := util.EncodeMsgPack(payload)
	if err != nil {
		return config, errHelp.Wrapf(err, "Admin: Query encoding ConfigQueryPayload fail: %v\n", err)
	}
	resBytes, err := admin.requester.SendRequest(statemachines.ConfigQuery, bytes)
	if err != nil {
		return config, errHelp.Wrapf(err, "Admin: Quering shardVersio %d fail\n", shardVersion)
	}
	err = util.DecodeMsgPack(resBytes, &config)
	if err != nil {
		return config, errHelp.Wrapf(err, "Admin: decoding Configuration fail\n")
	}
	return config, nil
}

// Client represents a client that connects to a known node in the shard orchestrator
// to issue commands. It can be used by a CLI, web app, or other application to
// interface with Raft.
type Client struct {
	requester *util.Requester
}

// ClientConnect creates a new Client for ShardOrchestrator and registers with the Raft node at the given address.
func ClientConnect(addr string) (*Client, error) {
	client := new(Client)
	requester, err := util.ConnectShardOrchestrator(addr)
	if err != nil {
		return nil, errHelp.Wrap(err, "Client: connecting to ShardOrchestrator fail\n")
	}
	client.requester = requester

	return client, nil
}

func (client *Client) Query(shardVersion int64) (util.Configuration, error) {
	var config util.Configuration

	payload := statemachines.ConfigQueryPayload{ShardVersion: shardVersion}
	bytes, err := util.EncodeMsgPack(payload)
	if err != nil {
		return config, errHelp.Wrapf(err, "Client: Query encoding ConfigQueryPayload fail: %v\n", err)
	}
	resBytes, err := client.requester.SendRequest(statemachines.ConfigQuery, bytes)
	if err != nil {
		return config, errHelp.Wrapf(err, "Admin: Quering shardVersio %d fail\n", shardVersion)
	}
	err = util.DecodeMsgPack(resBytes, &config)
	if err != nil {
		return config, errHelp.Wrapf(err, "Admin: decoding Configuration fail\n")
	}
	return config, nil
}

// InternalClient represents a client used by replication groups, that connects
// to a known node in the shard orchestrator to issue commands.
type InternalClient struct {
	requester *util.Requester
}

// InternalClientConnect creates a new InternalClient for ShardOrchestrator and registers with the Raft node at the given address.
func InternalClientConnect(addr string) (*InternalClient, error) {
	client := new(InternalClient)
	requester, err := util.ConnectShardOrchestrator(addr)
	if err != nil {
		return nil, errHelp.Wrap(err, "Client: connecting to ShardOrchestrator fail\n")
	}
	client.requester = requester

	return client, nil
}

func (internalClient *InternalClient) InternalQuery(shardVersion int64, srcGroupID uint64, addrs []string) (util.Configuration, error) {
	var config util.Configuration

	payload := statemachines.ConfigInternalQueryPayload{
		ShardVersion: shardVersion,
		SrcGroupID:   srcGroupID,
		Addrs:        addrs,
	}
	bytes, err := util.EncodeMsgPack(payload)
	if err != nil {
		return config, errHelp.Wrapf(err, "Client: Query encoding ConfigQueryPayload fail: %v\n", err)
	}
	resBytes, err := internalClient.requester.SendRequest(statemachines.ConfigInternalQuery, bytes)
	if err != nil {
		return config, errHelp.Wrapf(err, "Admin: Quering shardVersio %d fail\n", shardVersion)
	}
	err = util.DecodeMsgPack(resBytes, &config)
	if err != nil {
		return config, errHelp.Wrapf(err, "Admin: decoding Configuration fail\n")
	}
	return config, nil
}

// GetID returns the ID of current InternalClient
func (internalClient *InternalClient) GetID() uint64 {
	return internalClient.requester.ID
}
