package statemachines

import (
	"fmt"

	errHelp "github.com/pkg/errors"
	"github.com/ziyaoh/some-kvstore/util"
)

const (
	KVStoreCommand uint64 = iota
	ShardIn
	ShardOut
)

type KVStoreCommandPayload struct {
	Command uint64
	Data    []byte
}

type ShardInPayload struct {
	ConfigVersion uint64
	Data          map[int][]KVPair
}

type ShardOutPayload struct {
	Shards        []int
	ConfigVersion uint64
	DestAddrs     []string
}

// ShardKVMachine implements the raft.StateMachine interface.
// It manages one or more logical shards of the entire dataset.
// Servers as the state machine of a replication group node.
type ShardKVMachine struct {
	GroupID uint64
	Shards  *ShardOwnership
	kvstore *KVStoreMachine

	trans *Transferer
}

// NewShardKVMachine creates a new sharkv machine.
// Fails if creating KVStoreMachine fails.
func NewShardKVMachine(groupID uint64, kvstore *KVStoreMachine, trans *Transferer) (*ShardKVMachine, error) {
	shards := NewShardOwnership()
	machine := ShardKVMachine{
		GroupID: groupID,
		Shards:  shards,
		kvstore: kvstore,
		trans:   trans,
	}
	return &machine, nil
}

// Close closes the current machine
func (machine *ShardKVMachine) Close() {
	machine.kvstore.Close()
}

// GetState returns the state of the state machine as an interface{}, which can
// be converted to the expected type using type assertions.
func (machine *ShardKVMachine) GetState() interface{} {
	return nil
}

// ApplyCommand applies the given state machine command to the KVStoreMachine, and
// returns a message and an error if there is one.
func (machine *ShardKVMachine) ApplyCommand(command uint64, data []byte) ([]byte, error) {
	switch command {
	case KVStoreCommand:
		return machine.handleKVStoreCommand(command, data)
	case ShardOut:
		machine.handleShardOut(data)
	case ShardIn:
		machine.handleShardIn(data)
	default:
		return nil, fmt.Errorf("ShardKVMachine: unknown command %v", command)
	}
	return nil, nil
}

func (machine *ShardKVMachine) handleKVStoreCommand(command uint64, data []byte) ([]byte, error) {
	var payload KVStoreCommandPayload
	err := util.DecodeMsgPack(data, &payload)
	if err != nil {
		return nil, errHelp.Wrap(err, "ShardKVMachine: decode data as kvStoreComamndPayload fail\n")
	}

	var kvPair KVPair
	err = util.DecodeMsgPack(payload.Data, &kvPair)
	if err != nil {
		return nil, errHelp.Wrap(err, "ShardKVMachine: decode data as KVPair fail\n")
	}

	shard := util.KeyToShard(kvPair.Key)
	if !machine.Shards.Owning(shard) {
		return nil, fmt.Errorf("ShardKVMachine: current replication group is not responsible for key %v", kvPair.Key)
	}

	return machine.kvstore.applyParsedCommand(payload.Command, kvPair)
}

func (machine *ShardKVMachine) handleShardOut(data []byte) error {
	var outPayload ShardOutPayload
	err := util.DecodeMsgPack(data, &outPayload)
	if err != nil {
		return errHelp.Wrap(err, "ShardKVMachine: decode data as shardOutPayload fail\n")
	}

	shards := outPayload.Shards
	allData := make(map[int][]KVPair)
	for _, shard := range shards {
		if machine.Shards.Owning(shard) {
			allData[shard] = machine.kvstore.GetShard(shard)
			machine.kvstore.RemoveShard(shard)
			machine.Shards.Unown(shard)
		}
	}
	inPayload := ShardInPayload{
		ConfigVersion: outPayload.ConfigVersion,
		Data:          allData,
	}
	// ignore error here, assuming that majority of each replication group always work
	machine.trans.Transfer(outPayload.DestAddrs, inPayload)
	return nil
}

func (machine *ShardKVMachine) handleShardIn(data []byte) error {
	var payload ShardInPayload
	err := util.DecodeMsgPack(data, &payload)
	if err != nil {
		return errHelp.Wrap(err, "ShardKVMachine: decode data as shardInPayload fail\n")
	}

	for shard, pairs := range payload.Data {
		if !machine.Shards.Owning(shard) {
			for _, kv := range pairs {
				machine.kvstore.applyParsedCommand(KVStorePut, kv)
			}
			machine.Shards.Own(shard)
		}
	}

	return nil
}
