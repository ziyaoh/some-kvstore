package shardorchestrator

import (
	"fmt"

	"github.com/ziyaoh/some-kvstore/raft/raft"
)

// Returns the leader in a raft cluster, and an error otherwise.
func findLeader(nodes []*Node) (*Node, error) {
	leaders := make([]*Node, 0)
	for _, node := range nodes {
		if node.raft.State == raft.LeaderState {
			leaders = append(leaders, node)
		}
	}

	if len(leaders) == 0 {
		return nil, fmt.Errorf("No leader found in slice of nodes")
	} else if len(leaders) == 1 {
		return leaders[0], nil
	} else {
		return nil, fmt.Errorf("Found too many leaders in slice of nodes: %v", len(leaders))
	}
}

func findFollower(nodes []*Node) (*Node, error) {
	for _, node := range nodes {
		if node.raft.State == raft.FollowerState {
			return node, nil
		}
	}
	return nil, fmt.Errorf("No Follower found in slice of nodes")
}

func findAllFollowers(nodes []*Node) ([]*Node, error) {
	followers := make([]*Node, 0)
	for _, node := range nodes {
		if node.raft.State == raft.FollowerState {
			followers = append(followers, node)
		}
	}

	if len(followers) == 0 {
		return nil, fmt.Errorf("No follower found in slice of nodes")
	}
	return followers, nil
}
