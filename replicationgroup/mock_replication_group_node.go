package replicationgroup

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ziyaoh/some-kvstore/raft/raft"
	"github.com/ziyaoh/some-kvstore/rpc"
	"github.com/ziyaoh/some-kvstore/util"
	"golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

var MockError error = fmt.Errorf("Error by Mock Replication Group")

func defaultClientRequestCaller(ctx context.Context, node *MockRGNode, req *rpc.ClientRequest) (*rpc.ClientReply, error) {
	return &rpc.ClientReply{
		Status:     rpc.ClientStatus_OK,
		Response:   []byte("value"),
		LeaderHint: node.Leader,
	}, nil
}

func failClientRequestCaller(ctx context.Context, node *MockRGNode, req *rpc.ClientRequest) (*rpc.ClientReply, error) {
	return &rpc.ClientReply{
		Status:     rpc.ClientStatus_REQ_FAILED,
		LeaderHint: node.Leader,
	}, nil
}

func followerClientRequestCaller(ctx context.Context, node *MockRGNode, req *rpc.ClientRequest) (*rpc.ClientReply, error) {
	return &rpc.ClientReply{
		Status:     rpc.ClientStatus_NOT_LEADER,
		LeaderHint: node.Leader,
	}, nil
}

func candidateClientRequestCaller(ctx context.Context, node *MockRGNode, req *rpc.ClientRequest) (*rpc.ClientReply, error) {
	if node.called {
		return &rpc.ClientReply{
			Status:     rpc.ClientStatus_OK,
			Response:   []byte("value"),
			LeaderHint: node.Leader,
		}, nil
	}
	node.called = true
	return &rpc.ClientReply{
		Status:     rpc.ClientStatus_ELECTION_IN_PROGRESS,
		LeaderHint: node.Leader,
	}, nil
}

func startingClientRequestCaller(ctx context.Context, node *MockRGNode, req *rpc.ClientRequest) (*rpc.ClientReply, error) {
	return &rpc.ClientReply{
		Status:     rpc.ClientStatus_CLUSTER_NOT_STARTED,
		LeaderHint: node.Leader,
	}, nil
}

func errorClientRequestCaller(ctx context.Context, node *MockRGNode, req *rpc.ClientRequest) (*rpc.ClientReply, error) {
	return nil, MockError
}

// MockRGNode mocks a replication group node with predefined behavior on client request
type MockRGNode struct {
	// implements ReplicationGroupRPCServer
	Self          *rpc.RemoteNode
	Leader        *rpc.RemoteNode
	server        *grpc.Server
	called        bool
	ClientRequest func(ctx context.Context, node *MockRGNode, req *rpc.ClientRequest) (*rpc.ClientReply, error)

	cacheReq map[string]bool

	// optional element, useful for asserting called in client request
	t        *testing.T
	expected *rpc.ClientRequest
}

func (m *MockRGNode) ClientRequestCaller(ctx context.Context, req *rpc.ClientRequest) (*rpc.ClientReply, error) {
	if m.t != nil && m.expected != nil {
		expectMap := make(map[uint64]bool)
		actualMap := make(map[uint64]bool)
		for _, seq := range m.expected.AckSeqs {
			expectMap[seq] = true
		}
		for _, seq := range req.AckSeqs {
			actualMap[seq] = true
		}
		assert.Equal(m.t, expectMap, actualMap, "client request ack seq doesn't match")
	}
	return m.ClientRequest(ctx, m, req)
}

func (node *MockRGNode) GracefulExit() {
	node.server.GracefulStop()
}

func CreateDefaultMockRGNode() *MockRGNode {
	node := templateMockRGNode(defaultClientRequestCaller)
	node.Leader = node.Self
	return node
}

func CreateFailMockRGNode() *MockRGNode {
	node := templateMockRGNode(failClientRequestCaller)
	node.Leader = node.Self
	return node
}

func CreateFollowerMockRGNode(leader *rpc.RemoteNode) *MockRGNode {
	node := templateMockRGNode(followerClientRequestCaller)
	node.Leader = leader
	return node
}

func CreateCandidateMockRGNode() *MockRGNode {
	node := templateMockRGNode(candidateClientRequestCaller)
	node.Leader = node.Self
	return node
}

func CreateStartingMockRGNode() *MockRGNode {
	node := templateMockRGNode(startingClientRequestCaller)
	return node
}

func CreateErrorMockRGNode() *MockRGNode {
	node := templateMockRGNode(errorClientRequestCaller)
	return node
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
		cacheReq:      map[string]bool{},
	}
	rpc.RegisterReplicationGroupRPCServer(node.server, node)
	go node.server.Serve(listener)
	return node
}
