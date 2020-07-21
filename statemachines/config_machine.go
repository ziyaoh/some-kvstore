package statemachines

import (
	"errors"
	"fmt"

	errHelp "github.com/pkg/errors"
	"github.com/ziyaoh/some-kvstore/util"
)

const (
	ConfigJoin uint64 = iota
	ConfigLeave
	ConfigMove
	ConfigQuery
	ConfigInternalQuery
)

type ConfigJoinPayload struct {
	GroupID uint64
	Addrs   []string
}

type ConfigLeavePayload struct {
	GroupID uint64
}

type ConfigMovePayload struct {
	Shard     int
	DestGroup uint64
}

type ConfigQueryPayload struct {
	ShardVersion int64
}

type ConfigInternalQueryPayload struct {
	ShardVersion int64
	SrcGroupID   uint64
	Addrs        []string
}

// ConfigMachine implements the raft.StateMachine interface.
// It manages sharding configuration.
// Serves as the state machine of a shard orchestrator node.
type ConfigMachine struct {
	numShards     int
	configHistory []util.Configuration
	currentConfig util.Configuration

	shardingKicker *Transferer
}

// NewConfigMachine creates a new starting ConfigMachine
func NewConfigMachine(numShards int, kicker *Transferer) *ConfigMachine {
	machine := ConfigMachine{
		numShards:      numShards,
		configHistory:  make([]util.Configuration, 0),
		currentConfig:  util.NewConfiguration(numShards),
		shardingKicker: kicker,
	}
	return &machine
}

// Close closes the current machine
func (machine *ConfigMachine) Close() {}

// state of a ConfigMachine, returned by the GetState function, useful for testing
type configMachineState struct {
	historySize    int
	currentVersion uint64
	numChanges     int
	maxShards      int
	minShards      int
}

// GetState returns encoded slice of all configurations as the state of the state machine
func (machine *ConfigMachine) GetState() interface{} {
	for _, config := range machine.configHistory {
		if err := config.Validate(); err != nil {
			return errHelp.Wrapf(err, "ConfigMachine: validate past config fail: %v\n", config)
		}
	}
	if err := machine.currentConfig.Validate(); err != nil {
		return errHelp.Wrapf(err, "ConfigMachine: validate current config fail: %v\n", machine.currentConfig)
	}

	numChanges := -1
	if len(machine.configHistory) > 0 {
		numChanges = 0
		old := machine.configHistory[len(machine.configHistory)-1]
		for i := range old.Location {
			if old.Location[i] != machine.currentConfig.Location[i] {
				numChanges++
			}
		}
	}
	hist := make(map[uint64]int)
	for _, groupID := range machine.currentConfig.Location {
		if _, exist := hist[groupID]; !exist {
			hist[groupID] = 0
		}
		hist[groupID]++
	}
	max, min := 0, machine.numShards
	for _, shards := range hist {
		if shards > max {
			max = shards
		}
		if shards < min {
			min = shards
		}
	}

	state := configMachineState{
		historySize:    len(machine.configHistory),
		currentVersion: machine.currentConfig.Version,
		numChanges:     numChanges,
		maxShards:      max,
		minShards:      min,
	}
	return state
}

// ApplyCommand applies the given state machine command to the KVStoreMachine, and
// returns a message and an error if there is one.
func (machine *ConfigMachine) ApplyCommand(command uint64, data []byte) ([]byte, error) {
	switch command {
	case ConfigJoin:
		return machine.handleJoin(data)
	case ConfigLeave:
		return machine.handleLeave(data)
	case ConfigMove:
		return machine.handleMove(data)
	case ConfigQuery:
		return machine.handleQuery(data)
	case ConfigInternalQuery:
		return machine.handleInternalQuery(data)
	default:
		return nil, errors.New("unknown command type")
	}
}

func (machine *ConfigMachine) handleJoin(data []byte) ([]byte, error) {
	var payload ConfigJoinPayload
	err := util.DecodeMsgPack(data, &payload)
	if err != nil {
		return nil, errHelp.Wrap(err, "ConfigMachine: decoding configJoinPayload fail\n")
	}

	if payload.GroupID == 0 {
		return nil, fmt.Errorf("ConfigMachine: joining group ID cannot be 0")
	}

	if _, exist := machine.currentConfig.Groups[payload.GroupID]; exist {
		return nil, fmt.Errorf("ConfigMachine: joining with an existing group ID: %d", payload.GroupID)
	}

	newConfig := machine.currentConfig.NextConfig()
	newConfig.Groups[payload.GroupID] = payload.Addrs

	if len(newConfig.Groups) == 1 {
		for i := range newConfig.Location {
			newConfig.Location[i] = payload.GroupID
		}
	} else {
		hist := getHist(newConfig.Location)
		hist[payload.GroupID] = make([]int, 0)
		hist = balance(hist)
		reassign(&newConfig, hist)
	}

	oldConfig := machine.currentConfig
	machine.configHistory = append(machine.configHistory, oldConfig)
	machine.currentConfig = newConfig

	if oldConfig.IsEmpty() {
		go machine.kickSharding(payload.Addrs)
	}

	return nil, nil
}

func (machine *ConfigMachine) kickSharding(destAddrs []string) {
	mapping := make(map[int][]KVPair)
	for shard := 0; shard < machine.numShards; shard++ {
		mapping[shard] = nil
	}
	payload := ShardInPayload{
		ConfigVersion: machine.currentConfig.Version,
		Data:          mapping,
	}
	machine.shardingKicker.Transfer(destAddrs, payload)
}

func (machine *ConfigMachine) handleLeave(data []byte) ([]byte, error) {
	var payload ConfigLeavePayload
	err := util.DecodeMsgPack(data, &payload)
	if err != nil {
		return nil, errHelp.Wrap(err, "ConfigMachine: decoding configLeavePayload fail\n")
	}

	if _, exist := machine.currentConfig.Groups[payload.GroupID]; !exist {
		return nil, fmt.Errorf("ConfigMachine: leaving group ID does not exist: %d", payload.GroupID)
	}

	newConfig := machine.currentConfig.NextConfig()
	delete(newConfig.Groups, payload.GroupID)

	if len(newConfig.Groups) == 0 {
		for i := range newConfig.Location {
			newConfig.Location[i] = uint64(0)
		}
	} else {
		hist := getHist(newConfig.Location)
		for groupID := range newConfig.Groups {
			if _, exist := hist[groupID]; !exist {
				hist[groupID] = make([]int, 0)
			}
		}
		for anyGroupID := range newConfig.Groups {
			hist[anyGroupID] = append(hist[anyGroupID], hist[payload.GroupID]...)
			break
		}
		delete(hist, payload.GroupID)
		hist = balance(hist)
		reassign(&newConfig, hist)
	}

	machine.configHistory = append(machine.configHistory, machine.currentConfig)
	machine.currentConfig = newConfig
	return nil, nil
}

func (machine *ConfigMachine) handleMove(data []byte) ([]byte, error) {
	var payload ConfigMovePayload
	err := util.DecodeMsgPack(data, &payload)
	if err != nil {
		return nil, errHelp.Wrap(err, "ConfigMachine: decoding configMovePayload fail\n")
	}
	if payload.Shard < 0 || payload.Shard >= machine.numShards {
		return nil, fmt.Errorf("ConfigMachine: moving non-existing shard: %d", payload.Shard)
	}
	if _, exist := machine.currentConfig.Groups[payload.DestGroup]; !exist {
		return nil, fmt.Errorf("ConfigMachine: moving shard to non-existing group ID: %d", payload.DestGroup)
	}

	if machine.currentConfig.Location[payload.Shard] != payload.DestGroup {
		newConfig := machine.currentConfig.NextConfig()
		newConfig.Location[payload.Shard] = payload.DestGroup

		machine.configHistory = append(machine.configHistory, machine.currentConfig)
		machine.currentConfig = newConfig
	}
	return nil, nil
}

func (machine *ConfigMachine) handleQuery(data []byte) ([]byte, error) {
	var payload ConfigQueryPayload
	err := util.DecodeMsgPack(data, &payload)
	if err != nil {
		return nil, errHelp.Wrap(err, "ConfigMachine: decoding configQueryPayload fail\n")
	}

	var config util.Configuration
	if payload.ShardVersion < 0 || uint64(payload.ShardVersion) >= machine.currentConfig.Version {
		config = machine.currentConfig
	} else {
		config = machine.configHistory[payload.ShardVersion]
	}

	bytes, err := util.EncodeMsgPack(config)
	if err != nil {
		return nil, errHelp.Wrap(err, "ConfigMachine: handleQuery encoding configuration fail\n")
	}
	return bytes, nil
}

func (machine *ConfigMachine) handleInternalQuery(data []byte) ([]byte, error) {
	var payload ConfigInternalQueryPayload
	err := util.DecodeMsgPack(data, &payload)
	if err != nil {
		return nil, errHelp.Wrap(err, "ConfigMachine: decoding configInternalQueryPayload fail\n")
	}
	// if _, exist := machine.currentConfig.Groups[payload.SrcGroupID]; !exist {
	// 	return nil, fmt.Errorf("ConfigMachine: src group ID does not exist: %d", payload.SrcGroupID)
	// }
	if len(payload.Addrs) == 0 {
		return nil, fmt.Errorf("ConfigMachine: addrs of src group should not be empty")
	}

	if _, exist := machine.currentConfig.Groups[payload.SrcGroupID]; exist {
		machine.currentConfig.Groups[payload.SrcGroupID] = payload.Addrs
	}

	var config util.Configuration
	if payload.ShardVersion < 0 || uint64(payload.ShardVersion) >= machine.currentConfig.Version {
		config = machine.currentConfig
	} else {
		config = machine.configHistory[payload.ShardVersion]
	}

	bytes, err := util.EncodeMsgPack(config)
	if err != nil {
		return nil, errHelp.Wrap(err, "ConfigMachine: handleInternalQuery encoding configuration fail\n")
	}
	return bytes, nil
}

func getHist(locations []uint64) map[uint64][]int {
	hist := make(map[uint64][]int)
	for shard, groupID := range locations {
		if _, exist := hist[groupID]; !exist {
			hist[groupID] = make([]int, 0)
		}
		hist[groupID] = append(hist[groupID], shard)
	}
	return hist
}

func balance(hist map[uint64][]int) map[uint64][]int {
	// rand.Seed(time.Now().Unix())
	for {
		max, min := uint64(0), uint64(0)
		for groupID, shards := range hist {
			if max == 0 || len(shards) > len(hist[max]) {
				max = groupID
			}
			if min == 0 || len(shards) < len(hist[min]) {
				min = groupID
			}
		}
		if len(hist[max])-len(hist[min]) <= 1 {
			break
		}
		// randi := rand.Intn(len(hist[max]))
		// hist[max][randi], hist[max][len(hist[max])-1] = hist[max][len(hist[max])-1], hist[max][randi]
		moveShard := hist[max][len(hist[max])-1]
		hist[max] = hist[max][:len(hist[max])-1]
		hist[min] = append(hist[min], moveShard)
	}
	return hist
}

func reassign(config *util.Configuration, hist map[uint64][]int) {
	for groupID, shards := range hist {
		for _, shard := range shards {
			config.Location[shard] = groupID
		}
	}
}
