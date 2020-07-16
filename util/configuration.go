package util

import "fmt"

// NumShards is the number of shards keys are mapped to
const NumShards = 10

// Configuration defines a mapping from each shard to a replication group
// as well as a list of host addresses in each group
type Configuration struct {
	Version   uint64
	NumShards int
	Groups    map[uint64][]string
	Location  []uint64
}

// NewConfiguration creates an empty default configuration
func NewConfiguration(numShards int) Configuration {
	config := Configuration{
		NumShards: numShards,
		Groups:    make(map[uint64][]string),
		Location:  make([]uint64, numShards),
	}
	return config
}

// NextConfig generates and returns a new Configureation
// with version number incremented and everything else the same
func (config Configuration) NextConfig() Configuration {
	newConfig := Configuration{
		Version:   config.Version + 1,
		NumShards: config.NumShards,
		Groups:    make(map[uint64][]string),
		Location:  make([]uint64, config.NumShards),
	}
	for groupID, addrs := range config.Groups {
		copiedAddrs := make([]string, len(addrs))
		copy(copiedAddrs, addrs)
		newConfig.Groups[groupID] = copiedAddrs
	}
	for i, groupID := range config.Location {
		newConfig.Location[i] = groupID
	}
	return newConfig
}

// IsEmpty checks if current configuration is empty
func (config Configuration) IsEmpty() bool {
	return len(config.Groups) == 0
}

// Validate validates the config, making sure it's legal
func (config Configuration) Validate() error {
	if config.NumShards <= 0 {
		return fmt.Errorf("Configuration: expect configuration NumShards to be positive but got: %d", config.NumShards)
	}

	if _, exist := config.Groups[uint64(0)]; exist {
		return fmt.Errorf("Configuration: invalid replication group ID: %d, addr: %v", uint64(0), config.Groups[uint64(0)])
	}

	if len(config.Groups) == 0 {
		for _, group := range config.Location {
			if group != 0 {
				return fmt.Errorf("Configuration: expect all shards assigned to group 0 when no group exist but got: %v", config.Location)
			}
		}
	} else {
		for shard, group := range config.Location {
			if _, exist := config.Groups[group]; !exist {
				return fmt.Errorf("Configuration: shard %d is assigned to non-existing group: %d", shard, group)
			}
		}
	}
	return nil
}
