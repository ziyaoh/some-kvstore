package replicationgroup

import (
	"net"
	"strconv"

	"github.com/ziyaoh/some-kvstore/raft/raft"
	"github.com/ziyaoh/some-kvstore/rpc"
	"github.com/ziyaoh/some-kvstore/util"
	grpc "google.golang.org/grpc"
)

// Node defines an individual replication group node.
type Node struct {
	raft   *raft.Node
	Self   *rpc.RemoteNode
	port   int
	server *grpc.Server
}

// CreateNode is used to construct a new ReplicationGroup node. It takes a configuration,
// as well as implementations of various interfaces that are required.
// If we have any old state, such as snapshots, logs, Peers, etc, all those will be restored when creating the Raft node.
// Use port=0 for auto selection
func CreateNode(listener net.Listener, connect *rpc.RemoteNode, config *raft.Config, stateMachine raft.StateMachine, stableStore raft.StableStore) (*Node, error) {
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
	rpc.RegisterReplicationGroupRPCServer(node.server, node)

	node.raft, err = raft.CreateNode(node.Self, node.server, connect, config, stateMachine, stableStore)
	if err != nil {
		return nil, err
	}

	// go node.server.Serve(listener)
	// node.raft.Out("Started node")

	// if connect != nil {
	// 	err := connect.JoinRPC(node.Self)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// } else {
	// 	node.raft.Out("Waiting to start cluster until all have joined")
	// 	go node.raft.StartCluster()
	// }

	return node, nil
}

// ClientRequest is invoked on us by a client, and sends the request and a
// reply channel to the stateFunction. If the cluster hasn't started yet, it
// returns the corresponding ClientReply.
func (node *Node) ClientRequest(req *rpc.ClientRequest) rpc.ClientReply {
	reply := make(chan rpc.ClientReply)
	reqMsg := rpc.ClientRequestMsg{
		Request: req,
		Reply:   reply,
	}
	return node.raft.ClientRequest(&reqMsg)
}
