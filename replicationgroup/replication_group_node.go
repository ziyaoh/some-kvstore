package replicationgroup

import (
	"net"
	"strconv"
	"time"

	"github.com/ziyaoh/some-kvstore/raft/raft"
	"github.com/ziyaoh/some-kvstore/rpc"
	"github.com/ziyaoh/some-kvstore/shardorchestrator"
	"github.com/ziyaoh/some-kvstore/statemachines"
	"github.com/ziyaoh/some-kvstore/util"
	grpc "google.golang.org/grpc"
)

// Node defines an individual replication group node.
type Node struct {
	GroupID uint64
	raft    *raft.Node
	Self    *rpc.RemoteNode
	port    int
	server  *grpc.Server

	shardOwnership *statemachines.ShardOwnership
	configQuery    *shardorchestrator.InternalClient
}

// CreateNode is used to construct a new ReplicationGroup node. It takes a configuration,
// as well as implementations of various interfaces that are required.
// If we have any old state, such as snapshots, logs, Peers, etc, all those will be restored when creating the Raft node.
// Use port=0 for auto selection
func CreateNode(groupID uint64, listener net.Listener, connect *rpc.RemoteNode, config *raft.Config, shardKVMachine *statemachines.ShardKVMachine, stableStore raft.StableStore, configQuery *shardorchestrator.InternalClient) (*Node, error) {
	node := new(Node)

	node.GroupID = groupID
	// Set remote self based on listener address
	node.Self = &rpc.RemoteNode{
		Id:   util.AddrToID(listener.Addr().String(), config.NodeIDSize),
		Addr: listener.Addr().String(),
	}
	// passed in port may be 0
	_, realPort, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		return nil, err
	}
	node.port, err = strconv.Atoi(realPort)
	if err != nil {
		return nil, err
	}

	// Start RPC server
	node.server = grpc.NewServer()
	rpc.RegisterReplicationGroupRPCServer(node.server, node)

	node.raft, err = raft.CreateNode(listener, node.server, connect, config, shardKVMachine, stableStore)
	if err != nil {
		return nil, err
	}

	node.shardOwnership = shardKVMachine.Shards
	node.configQuery = configQuery

	go node.tick()

	return node, nil
}

func (node *Node) tick() {
	seq := uint64(0)
	for {
		// TODO: make this configurable
		time.Sleep(1 * time.Second)
		switch node.raft.State {
		case raft.ExitState:
			return
		case raft.LeaderState:
			peers := make([]string, 0)
			for _, peer := range node.raft.Peers {
				peers = append(peers, peer.Addr)
			}
			config, err := node.configQuery.InternalQuery(int64(-1), node.GroupID, peers)
			if err != nil {
				continue
			}

			owningShards := node.shardOwnership.ViewOwnership()
			todo := make(map[uint64][]int)
			for _, shard := range owningShards {
				target := config.Location[shard]
				if target != node.GroupID {
					if _, exist := todo[target]; !exist {
						todo[target] = make([]int, 0)
					}
					todo[target] = append(todo[target], shard)
				}
			}
			for target, shards := range todo {
				payload := statemachines.ShardOutPayload{
					Shards:        shards,
					ConfigVersion: config.Version,
					DestAddrs:     config.Groups[target],
				}
				data, err := util.EncodeMsgPack(payload)
				if err != nil {
					node.Error("fail to encode ShardOutPayload %v: %v", payload, err)
					continue
				}
				request := rpc.ClientRequest{
					ClientId:        node.configQuery.GetID(),
					SequenceNum:     seq,
					StateMachineCmd: statemachines.ShardOut,
					Data:            data,
				}
				if seq > uint64(0) {
					request.AckSeqs = []uint64{seq - uint64(1)}
				}
				seq++
				node.ClientRequest(&request)
			}
		}
	}
}

// ClientRequest is invoked on us by a client, and sends the request and a
// reply channel to the stateFunction. If the cluster hasn't started yet, it
// returns the corresponding ClientReply.
func (node *Node) ClientRequest(req *rpc.ClientRequest) rpc.ClientReply {
	return node.raft.ClientRequest(req)
}

// Exit abruptly shuts down the current node's process, including the GRPC server.
func (node *Node) Exit() {
	node.raft.Exit()
}

// GracefulExit sends a signal down the gracefulExit channel, in order to enable
// a safe exit from the cluster, handled by the current stateFunction.
func (node *Node) GracefulExit() {
	node.raft.GracefulExit()
}
