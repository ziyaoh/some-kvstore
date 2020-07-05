package statemachines

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ziyaoh/some-kvstore/util"
)

func TestConfigMachineInitialization(t *testing.T) {
	machine := NewConfigMachine(util.NumShards)
	defer machine.Close()

	state := machine.GetState().(configMachineState)

	expectedState := configMachineState{
		historySize:    0,
		currentVersion: uint64(0),
		numChanges:     -1,
		maxShards:      util.NumShards,
		minShards:      util.NumShards,
	}

	assert.Equal(t, expectedState, state)
}

func TestConfigMachineGetStateSimple(t *testing.T) {
	initConfig := util.NewConfiguration(util.NumShards)
	machine := &ConfigMachine{
		numShards:     util.NumShards,
		configHistory: []util.Configuration{initConfig},
		currentConfig: initConfig.NextConfig(),
	}

	expectedState := configMachineState{
		historySize:    1,
		currentVersion: uint64(1),
		numChanges:     0,
		maxShards:      util.NumShards,
		minShards:      util.NumShards,
	}
	state := machine.GetState()
	switch state.(type) {
	case configMachineState:
		if !reflect.DeepEqual(state, expectedState) {
			t.Errorf("Expected state to be %v, got %v\n", expectedState, state)
		}
	case error:
		t.Error(state)
	default:
		t.Errorf("ConfigMachine GetState returns unknown type: %v", state)
	}
}

func TestConfigMachineHandleUnknownCommand(t *testing.T) {
	machine := NewConfigMachine(util.NumShards)
	defer machine.Close()

	res, err := machine.ApplyCommand(10, []byte{})

	assert.NotNil(t, err, "expecting KVStore return error on unknown command")
	assert.Nilf(t, res, "expecting result on unknown command to be nil but got %v", res)
}

func TestConfigMachineHandleJoin(t *testing.T) {
	cases := []struct {
		name string
		data []struct {
			payload    ConfigJoinPayload
			expectFail bool
		}
		expected configMachineState
	}{
		{
			name: "first join",
			data: []struct {
				payload    ConfigJoinPayload
				expectFail bool
			}{
				{
					payload: ConfigJoinPayload{
						GroupID: uint64(1),
						Addrs:   []string{"0.0.0.0"},
					},
					expectFail: false,
				},
			},
			expected: configMachineState{
				historySize:    1,
				currentVersion: uint64(1),
				numChanges:     util.NumShards,
				maxShards:      util.NumShards,
				minShards:      util.NumShards,
			},
		},
		{
			name: "normal join",
			data: []struct {
				payload    ConfigJoinPayload
				expectFail bool
			}{
				{
					payload: ConfigJoinPayload{
						GroupID: uint64(1),
						Addrs:   []string{"0.0.0.0"},
					},
					expectFail: false,
				},
				{
					payload: ConfigJoinPayload{
						GroupID: uint64(2),
						Addrs:   []string{"0.0.0.0"},
					},
					expectFail: false,
				},
			},
			expected: configMachineState{
				historySize:    2,
				currentVersion: uint64(2),
				numChanges:     util.NumShards / 2,
				maxShards:      util.NumShards / 2,
				minShards:      util.NumShards / 2,
			},
		},
		{
			name: "duplicate join",
			data: []struct {
				payload    ConfigJoinPayload
				expectFail bool
			}{
				{
					payload: ConfigJoinPayload{
						GroupID: uint64(1),
						Addrs:   []string{"0.0.0.0"},
					},
					expectFail: false,
				},
				{
					payload: ConfigJoinPayload{
						GroupID: uint64(1),
						Addrs:   []string{"0.0.0.0"},
					},
					expectFail: true,
				},
			},
			expected: configMachineState{
				historySize:    1,
				currentVersion: uint64(1),
				numChanges:     util.NumShards,
				maxShards:      util.NumShards,
				minShards:      util.NumShards,
			},
		},
		{
			name: "join with invalid groupID",
			data: []struct {
				payload    ConfigJoinPayload
				expectFail bool
			}{
				{
					payload: ConfigJoinPayload{
						GroupID: uint64(1),
						Addrs:   []string{"0.0.0.0"},
					},
					expectFail: false,
				},
				{
					payload: ConfigJoinPayload{
						GroupID: uint64(0),
						Addrs:   []string{"0.0.0.0"},
					},
					expectFail: true,
				},
				{
					payload: ConfigJoinPayload{
						GroupID: uint64(3),
						Addrs:   []string{"0.0.0.0"},
					},
					expectFail: false,
				},
			},
			expected: configMachineState{
				historySize:    2,
				currentVersion: uint64(2),
				numChanges:     util.NumShards / 2,
				maxShards:      util.NumShards / 2,
				minShards:      util.NumShards / 2,
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			machine := NewConfigMachine(util.NumShards)
			defer machine.Close()

			for _, step := range testCase.data {
				payloadBytes, err := util.EncodeMsgPack(step.payload)
				assert.Nil(t, err)

				_, err = machine.ApplyCommand(ConfigJoin, payloadBytes)
				if step.expectFail {
					assert.NotNil(t, err)
				} else {
					assert.Nil(t, err)
				}
			}

			state := machine.GetState()
			switch state.(type) {
			case configMachineState:
				if !reflect.DeepEqual(state, testCase.expected) {
					t.Errorf("Expected state to be %v, got %v\n", testCase.expected, state)
				}
			case error:
				t.Error(state)
			default:
				t.Errorf("ConfigMachine GetState returns unknown type: %v", state)
			}
		})
	}
}

func TestConfigMachineHandleLeave(t *testing.T) {
	singleGroups := map[uint64][]string{
		uint64(1): []string{"0.0.0.0"},
	}
	singleGroupLocation := make([]uint64, util.NumShards)
	for i := range singleGroupLocation {
		singleGroupLocation[i] = uint64(1)
	}

	normalGroup := map[uint64][]string{
		uint64(1): []string{"0.0.0.0"},
		uint64(2): []string{"0.0.0.0"},
		uint64(3): []string{"0.0.0.0"},
	}
	normalLocation := make([]uint64, util.NumShards)
	for i := range normalLocation {
		if i < util.NumShards/2 {
			normalLocation[i] = uint64(1)
		} else {
			normalLocation[i] = uint64(2)
		}
	}

	cases := []struct {
		name     string
		starting struct {
			groups    map[uint64][]string
			locations []uint64
		}
		data struct {
			payload    ConfigLeavePayload
			expectFail bool
		}
		expected configMachineState
	}{
		{
			name: "last leave",
			starting: struct {
				groups    map[uint64][]string
				locations []uint64
			}{
				groups:    singleGroups,
				locations: singleGroupLocation,
			},
			data: struct {
				payload    ConfigLeavePayload
				expectFail bool
			}{
				payload:    ConfigLeavePayload{GroupID: uint64(1)},
				expectFail: false,
			},
			expected: configMachineState{
				historySize:    1,
				currentVersion: uint64(1),
				numChanges:     util.NumShards,
				maxShards:      util.NumShards,
				minShards:      util.NumShards,
			},
		},
		{
			name: "normal leave",
			starting: struct {
				groups    map[uint64][]string
				locations []uint64
			}{
				groups:    normalGroup,
				locations: normalLocation,
			},
			data: struct {
				payload    ConfigLeavePayload
				expectFail bool
			}{
				payload:    ConfigLeavePayload{GroupID: uint64(1)},
				expectFail: false,
			},
			expected: configMachineState{
				historySize:    1,
				currentVersion: uint64(1),
				numChanges:     util.NumShards / 2,
				maxShards:      util.NumShards - util.NumShards/2,
				minShards:      util.NumShards / 2,
			},
		},
		{
			name: "non-existing group leave",
			starting: struct {
				groups    map[uint64][]string
				locations []uint64
			}{
				groups:    normalGroup,
				locations: normalLocation,
			},
			data: struct {
				payload    ConfigLeavePayload
				expectFail bool
			}{
				payload:    ConfigLeavePayload{GroupID: uint64(10)},
				expectFail: true,
			},
			expected: configMachineState{
				historySize:    0,
				currentVersion: uint64(0),
				numChanges:     -1,
				maxShards:      util.NumShards - util.NumShards/2,
				minShards:      util.NumShards / 2,
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			machine := ConfigMachine{
				numShards:     util.NumShards,
				configHistory: []util.Configuration{},
				currentConfig: util.Configuration{
					Version:   uint64(0),
					NumShards: util.NumShards,
					Groups:    testCase.starting.groups,
					Location:  testCase.starting.locations,
				},
			}
			defer machine.Close()

			payloadBytes, err := util.EncodeMsgPack(testCase.data.payload)
			assert.Nil(t, err)

			_, err = machine.ApplyCommand(ConfigLeave, payloadBytes)
			if testCase.data.expectFail {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
			}

			state := machine.GetState()
			switch state.(type) {
			case configMachineState:
				if !reflect.DeepEqual(state, testCase.expected) {
					t.Errorf("Expected state to be %v, got %v\n", testCase.expected, state)
				}
			case error:
				t.Error(state)
			default:
				t.Errorf("ConfigMachine GetState returns unknown type: %v", state)
			}
		})
	}
}

func TestConfigMachineHandleMove(t *testing.T) {
	normalGroup := map[uint64][]string{
		uint64(1): []string{"0.0.0.0"},
		uint64(2): []string{"0.0.0.0"},
		uint64(3): []string{"0.0.0.0"},
	}
	normalLocation := make([]uint64, util.NumShards)
	for i := range normalLocation {
		if i < util.NumShards/2 {
			normalLocation[i] = uint64(1)
		} else {
			normalLocation[i] = uint64(2)
		}
	}

	cases := []struct {
		name     string
		starting struct {
			groups    map[uint64][]string
			locations []uint64
		}
		data struct {
			payload    ConfigMovePayload
			expectFail bool
		}
		expected configMachineState
	}{
		{
			name: "simple move",
			starting: struct {
				groups    map[uint64][]string
				locations []uint64
			}{
				groups:    normalGroup,
				locations: normalLocation,
			},
			data: struct {
				payload    ConfigMovePayload
				expectFail bool
			}{
				payload: ConfigMovePayload{
					Shard:     0,
					DestGroup: uint64(3),
				},
				expectFail: false,
			},
			expected: configMachineState{
				historySize:    1,
				currentVersion: uint64(1),
				numChanges:     1,
				maxShards:      util.NumShards / 2,
				minShards:      1,
			},
		},
		{
			name: "move to self no effect",
			starting: struct {
				groups    map[uint64][]string
				locations []uint64
			}{
				groups:    normalGroup,
				locations: normalLocation,
			},
			data: struct {
				payload    ConfigMovePayload
				expectFail bool
			}{
				payload: ConfigMovePayload{
					Shard:     0,
					DestGroup: uint64(1),
				},
				expectFail: false,
			},
			expected: configMachineState{
				historySize:    0,
				currentVersion: uint64(0),
				numChanges:     -1,
				maxShards:      util.NumShards - util.NumShards/2,
				minShards:      util.NumShards / 2,
			},
		},
		{
			name: "move to non-existing gropu",
			starting: struct {
				groups    map[uint64][]string
				locations []uint64
			}{
				groups:    normalGroup,
				locations: normalLocation,
			},
			data: struct {
				payload    ConfigMovePayload
				expectFail bool
			}{
				payload: ConfigMovePayload{
					Shard:     0,
					DestGroup: uint64(10),
				},
				expectFail: true,
			},
			expected: configMachineState{
				historySize:    0,
				currentVersion: uint64(0),
				numChanges:     -1,
				maxShards:      util.NumShards - util.NumShards/2,
				minShards:      util.NumShards / 2,
			},
		},
		{
			name: "move non-existing shard",
			starting: struct {
				groups    map[uint64][]string
				locations []uint64
			}{
				groups:    normalGroup,
				locations: normalLocation,
			},
			data: struct {
				payload    ConfigMovePayload
				expectFail bool
			}{
				payload: ConfigMovePayload{
					Shard:     util.NumShards,
					DestGroup: uint64(3),
				},
				expectFail: true,
			},
			expected: configMachineState{
				historySize:    0,
				currentVersion: uint64(0),
				numChanges:     -1,
				maxShards:      util.NumShards - util.NumShards/2,
				minShards:      util.NumShards / 2,
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			machine := ConfigMachine{
				numShards:     util.NumShards,
				configHistory: []util.Configuration{},
				currentConfig: util.Configuration{
					Version:   uint64(0),
					NumShards: util.NumShards,
					Groups:    testCase.starting.groups,
					Location:  testCase.starting.locations,
				},
			}
			defer machine.Close()

			payloadBytes, err := util.EncodeMsgPack(testCase.data.payload)
			assert.Nil(t, err)

			_, err = machine.ApplyCommand(ConfigMove, payloadBytes)
			if testCase.data.expectFail {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
			}

			state := machine.GetState()
			switch state.(type) {
			case configMachineState:
				if !reflect.DeepEqual(state, testCase.expected) {
					t.Errorf("Expected state to be %v, got %v\n", testCase.expected, state)
				}
			case error:
				t.Error(state)
			default:
				t.Errorf("ConfigMachine GetState returns unknown type: %v", state)
			}
		})
	}
}

func TestConfigMachineHandleQuery(t *testing.T) {
	normalGroup := map[uint64][]string{
		uint64(1): []string{"0.0.0.0"},
		uint64(2): []string{"0.0.0.0"},
		uint64(3): []string{"0.0.0.0"},
	}
	normalLocation := make([]uint64, util.NumShards)
	for i := range normalLocation {
		if i < util.NumShards/2 {
			normalLocation[i] = uint64(1)
		} else {
			normalLocation[i] = uint64(2)
		}
	}

	machine := ConfigMachine{
		numShards:     util.NumShards,
		configHistory: []util.Configuration{},
		currentConfig: util.Configuration{
			Version:   uint64(0),
			NumShards: util.NumShards,
			Groups:    normalGroup,
			Location:  normalLocation,
		},
	}
	defer machine.Close()
	moveBytes, err := util.EncodeMsgPack(ConfigMovePayload{Shard: 0, DestGroup: uint64(3)})
	assert.Nil(t, err)
	_, err = machine.ApplyCommand(ConfigMove, moveBytes)
	assert.Nil(t, err)

	cases := []struct {
		name            string
		query           ConfigQueryPayload
		expectedVersion uint64
	}{
		{
			name:            "simple query",
			query:           ConfigQueryPayload{ShardVersion: int64(0)},
			expectedVersion: uint64(0),
		},
		{
			name:            "latest query",
			query:           ConfigQueryPayload{ShardVersion: int64(1)},
			expectedVersion: uint64(1),
		},
		{
			name:            "query negative version",
			query:           ConfigQueryPayload{ShardVersion: int64(-1)},
			expectedVersion: uint64(1),
		},
		{
			name:            "query big version",
			query:           ConfigQueryPayload{ShardVersion: int64(2)},
			expectedVersion: uint64(1),
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {

			payloadBytes, err := util.EncodeMsgPack(testCase.query)
			assert.Nil(t, err)

			result, err := machine.ApplyCommand(ConfigQuery, payloadBytes)
			assert.Nil(t, err)

			var config util.Configuration
			err = util.DecodeMsgPack(result, &config)
			assert.Nil(t, err)
			assert.Equal(t, testCase.expectedVersion, config.Version)
		})
	}
}

func TestConfigMachineHandleInternalQuery(t *testing.T) {
	normalGroup := map[uint64][]string{
		uint64(1): []string{"0.0.0.1"},
		uint64(2): []string{"0.0.0.2"},
		uint64(3): []string{"0.0.0.3"},
	}
	normalLocation := make([]uint64, util.NumShards)
	for i := range normalLocation {
		if i < util.NumShards/2 {
			normalLocation[i] = uint64(1)
		} else {
			normalLocation[i] = uint64(2)
		}
	}

	cases := []struct {
		name            string
		query           ConfigInternalQueryPayload
		expectFail      bool
		expectedVersion uint64
		expectedGroups  map[uint64][]string
	}{
		{
			name: "simple query",
			query: ConfigInternalQueryPayload{
				ShardVersion: int64(-1),
				SrcGroupID:   uint64(1),
				Addrs:        []string{"0.0.0.1"},
			},
			expectFail:      false,
			expectedVersion: uint64(1),
			expectedGroups: map[uint64][]string{
				uint64(1): []string{"0.0.0.1"},
				uint64(2): []string{"0.0.0.2"},
				uint64(3): []string{"0.0.0.3"},
			},
		},
		{
			name: "internal query changing group addrs",
			query: ConfigInternalQueryPayload{
				ShardVersion: int64(-1),
				SrcGroupID:   uint64(1),
				Addrs:        []string{"0.0.0.10", "0.0.0.11"},
			},
			expectFail:      false,
			expectedVersion: uint64(1),
			expectedGroups: map[uint64][]string{
				uint64(1): []string{"0.0.0.10", "0.0.0.11"},
				uint64(2): []string{"0.0.0.2"},
				uint64(3): []string{"0.0.0.3"},
			},
		},
		{
			name: "internal query with empty addrs",
			query: ConfigInternalQueryPayload{
				ShardVersion: int64(-1),
				SrcGroupID:   uint64(1),
				Addrs:        []string{},
			},
			expectFail: true,
		},
		{
			name: "internal query with non-existing group ID",
			query: ConfigInternalQueryPayload{
				ShardVersion: int64(-1),
				SrcGroupID:   uint64(10),
				Addrs:        []string{"0.0.0.0"},
			},
			expectFail:      false,
			expectedVersion: uint64(1),
			expectedGroups: map[uint64][]string{
				uint64(1): []string{"0.0.0.1"},
				uint64(2): []string{"0.0.0.2"},
				uint64(3): []string{"0.0.0.3"},
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			machine := ConfigMachine{
				numShards:     util.NumShards,
				configHistory: []util.Configuration{},
				currentConfig: util.Configuration{
					Version:   uint64(0),
					NumShards: util.NumShards,
					Groups:    normalGroup,
					Location:  normalLocation,
				},
			}
			defer machine.Close()
			moveBytes, err := util.EncodeMsgPack(ConfigMovePayload{Shard: 0, DestGroup: uint64(3)})
			assert.Nil(t, err)
			_, err = machine.ApplyCommand(ConfigMove, moveBytes)
			assert.Nil(t, err)

			payloadBytes, err := util.EncodeMsgPack(testCase.query)
			assert.Nil(t, err)

			result, err := machine.ApplyCommand(ConfigInternalQuery, payloadBytes)
			if testCase.expectFail {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)

				var config util.Configuration
				err = util.DecodeMsgPack(result, &config)
				assert.Nil(t, err)
				assert.Equal(t, testCase.expectedVersion, config.Version)
				if !reflect.DeepEqual(testCase.expectedGroups, config.Groups) {
					t.Errorf("Expected groups to be %v, got %v\n", testCase.expectedGroups, config.Groups)
				}
			}
		})
	}
}
