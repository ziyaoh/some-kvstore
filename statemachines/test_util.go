package statemachines

import (
	"context"
	"reflect"

	"github.com/ziyaoh/some-kvstore/raft/raft"
	"github.com/ziyaoh/some-kvstore/rpc"
	"github.com/ziyaoh/some-kvstore/util"
	"google.golang.org/grpc"
)

// MockRGNode mocks a replication group node with predefined behavior on client request
type MockRGNode struct {
	// implements ReplicationGroupRPCServer
	Self          *rpc.RemoteNode
	Leader        *rpc.RemoteNode
	server        *grpc.Server
	ClientRequest func(ctx context.Context, node *MockRGNode, req *rpc.ClientRequest) (*rpc.ClientReply, error)

	// optional element, useful for asserting called in client request
	match    chan bool
	expected *ShardInPayload
}

func (m *MockRGNode) ClientRequestCaller(ctx context.Context, req *rpc.ClientRequest) (*rpc.ClientReply, error) {
	if m.expected != nil {
		var payload ShardInPayload
		util.DecodeMsgPack(req.Data, &payload)
		m.match <- reflect.DeepEqual(*m.expected, payload)
	}
	return m.ClientRequest(ctx, m, req)
}

func (node *MockRGNode) GracefulExit() {
	node.server.GracefulStop()
}

func templateMockRGNode(caller func(ctx context.Context, node *MockRGNode, req *rpc.ClientRequest) (*rpc.ClientReply, error)) *MockRGNode {
	listener := util.OpenPort(0)
	node := &MockRGNode{
		Self: &rpc.RemoteNode{
			Id:   util.AddrToID(listener.Addr().String(), raft.DefaultConfig().NodeIDSize),
			Addr: listener.Addr().String(),
		},
		server:        grpc.NewServer(),
		ClientRequest: caller,
		match:         make(chan bool, 1),
	}
	rpc.RegisterReplicationGroupRPCServer(node.server, node)
	go node.server.Serve(listener)
	return node
}

func getMockRG() *MockRGNode {
	caller := func(ctx context.Context, node *MockRGNode, req *rpc.ClientRequest) (*rpc.ClientReply, error) {
		return &rpc.ClientReply{
			Status:     rpc.ClientStatus_OK,
			Response:   []byte{},
			LeaderHint: node.Self,
		}, nil
	}
	group := templateMockRGNode(caller)

	return group
}
