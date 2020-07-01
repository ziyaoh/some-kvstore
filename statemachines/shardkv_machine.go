package statemachines

import errHelp "github.com/pkg/errors"

// ShardKVMachine implements the raft.StateMachine interface.
// It manages one or more logical shards of the entire dataset.
type ShardKVMachine struct {
	shards  map[uint64]bool
	kvstore *KVStoreMachine
}

// TODO: to implement

// NewShardKVMachine creates a new sharkv machine.
// Fails if creating KVStoreMachine fails.
func NewShardKVMachine(path string) (*ShardKVMachine, error) {
	kvstore, err := NewKVStoreMachine(path)
	if err != nil {
		return nil, errHelp.Wrap(err, "NewShardKVMachine: creating KVStoreMachine fail\n")
	}

	shards := make(map[uint64]bool)
	return &ShardKVMachine{shards: shards, kvstore: kvstore}, nil
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
	return nil, nil
}
