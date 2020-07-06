package shardorchestrator

import (
	"net"
	"strconv"

	"github.com/ziyaoh/some-kvstore/raft/raft"
	"github.com/ziyaoh/some-kvstore/rpc"
	"github.com/ziyaoh/some-kvstore/statemachines"
	"github.com/ziyaoh/some-kvstore/util"
	"google.golang.org/grpc"
)

// Node defines an individual shard orchestrator node.
type Node struct {
	raft   *raft.Node
	Self   *rpc.RemoteNode
	port   int
	server *grpc.Server
}

// CreateNode is used to construct a new ShardOrchestrator node. It takes a configuration,
// as well as implementations of various interfaces that are required.
// If we have any old state, such as snapshots, logs, Peers, etc, all those will be restored when creating the Raft node.
// Use port=0 for auto selection
func CreateNode(listener net.Listener, connect *rpc.RemoteNode, config *raft.Config, configMachine *statemachines.ConfigMachine, stableStore raft.StableStore) (*Node, error) {
	node := new(Node)

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
	rpc.RegisterShardOrchestratorRPCServer(node.server, node)

	node.raft, err = raft.CreateNode(listener, node.server, connect, config, configMachine, stableStore)
	if err != nil {
		return nil, err
	}

	return node, nil
}

// RegisterClient registers a client and generates an unique client id
func (node *Node) RegisterClient(req *rpc.RegisterClientRequest) rpc.RegisterClientReply {
	return node.raft.RegisterClient(req)
}

// ClientRequest does some processing on the passed in data and pass it on to underlying raft
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
