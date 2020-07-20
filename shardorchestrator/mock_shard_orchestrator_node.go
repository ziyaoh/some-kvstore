package shardorchestrator

import (
	"fmt"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/ziyaoh/some-kvstore/raft/raft"
	"github.com/ziyaoh/some-kvstore/rpc"
	"github.com/ziyaoh/some-kvstore/util"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var MockError error = fmt.Errorf("Error by Mock Shard Orchestrator")

func defaultRegisterClientCaller(ctx context.Context, node *MockSONode, req *rpc.RegisterClientRequest) (*rpc.RegisterClientReply, error) {
	node.clientID++
	return &rpc.RegisterClientReply{
		Status:     rpc.ClientStatus_OK,
		ClientId:   node.clientID,
		LeaderHint: node.Leader,
	}, nil
}

func defaultClientRequestCaller(ctx context.Context, node *MockSONode, req *rpc.ClientRequest) (*rpc.ClientReply, error) {
	config := node.CurrentConfig
	bytes, err := util.EncodeMsgPack(config)
	if err != nil {
		return nil, errors.Wrap(err, "MockShardOrchestrator: default client request caller encoding config fail\n")
	}
	return &rpc.ClientReply{
		Status:     rpc.ClientStatus_OK,
		Response:   bytes,
		LeaderHint: node.Leader,
	}, nil
}

func failRegisterClientCaller(ctx context.Context, node *MockSONode, req *rpc.RegisterClientRequest) (*rpc.RegisterClientReply, error) {
	return &rpc.RegisterClientReply{
		Status:     rpc.ClientStatus_REQ_FAILED,
		LeaderHint: node.Leader,
	}, nil
}

func failClientRequestCaller(ctx context.Context, node *MockSONode, req *rpc.ClientRequest) (*rpc.ClientReply, error) {
	return &rpc.ClientReply{
		Status:     rpc.ClientStatus_REQ_FAILED,
		LeaderHint: node.Leader,
	}, nil
}

func followerRegisterClientCaller(ctx context.Context, node *MockSONode, req *rpc.RegisterClientRequest) (*rpc.RegisterClientReply, error) {
	return &rpc.RegisterClientReply{
		Status:     rpc.ClientStatus_NOT_LEADER,
		LeaderHint: node.Leader,
	}, nil
}

func followerClientRequestCaller(ctx context.Context, node *MockSONode, req *rpc.ClientRequest) (*rpc.ClientReply, error) {
	return &rpc.ClientReply{
		Status:     rpc.ClientStatus_NOT_LEADER,
		LeaderHint: node.Leader,
	}, nil
}

func candidateRegisterClientCaller(ctx context.Context, node *MockSONode, req *rpc.RegisterClientRequest) (*rpc.RegisterClientReply, error) {
	if node.called {
		// return &rpc.RegisterClientReply{
		// 	Status:     rpc.ClientStatus_OK,
		// 	ClientId:   uint64(123),
		// 	LeaderHint: node.Leader,
		// }, nil
		return defaultRegisterClientCaller(ctx, node, req)
	}
	node.called = true
	return &rpc.RegisterClientReply{
		Status:     rpc.ClientStatus_ELECTION_IN_PROGRESS,
		LeaderHint: node.Leader,
	}, nil
}

func candidateClientRequestCaller(ctx context.Context, node *MockSONode, req *rpc.ClientRequest) (*rpc.ClientReply, error) {
	if node.called {
		// return &rpc.ClientReply{
		// 	Status:     rpc.ClientStatus_OK,
		// 	Response:   []byte("value"),
		// 	LeaderHint: node.Leader,
		// }, nil
		node.called = false
		return defaultClientRequestCaller(ctx, node, req)
	}
	node.called = true
	return &rpc.ClientReply{
		Status:     rpc.ClientStatus_ELECTION_IN_PROGRESS,
		LeaderHint: node.Leader,
	}, nil
}

func startingRegisterClientCaller(ctx context.Context, node *MockSONode, req *rpc.RegisterClientRequest) (*rpc.RegisterClientReply, error) {
	return &rpc.RegisterClientReply{
		Status:     rpc.ClientStatus_CLUSTER_NOT_STARTED,
		LeaderHint: node.Leader,
	}, nil
}

func startingClientRequestCaller(ctx context.Context, node *MockSONode, req *rpc.ClientRequest) (*rpc.ClientReply, error) {
	return &rpc.ClientReply{
		Status:     rpc.ClientStatus_CLUSTER_NOT_STARTED,
		LeaderHint: node.Leader,
	}, nil
}

func errorRegisterClientCaller(ctx context.Context, node *MockSONode, req *rpc.RegisterClientRequest) (*rpc.RegisterClientReply, error) {
	return nil, MockError
}

func errorClientRequestCaller(ctx context.Context, node *MockSONode, req *rpc.ClientRequest) (*rpc.ClientReply, error) {
	return nil, MockError
}

// MockSONode mocks a shard orchestrator node with predefined behavior on client registration and request
type MockSONode struct {
	// implements ReplicationGroupRPCServer
	Self           *rpc.RemoteNode
	Leader         *rpc.RemoteNode
	server         *grpc.Server
	called         bool
	CurrentConfig  util.Configuration
	RegisterClient func(ctx context.Context, node *MockSONode, req *rpc.RegisterClientRequest) (*rpc.RegisterClientReply, error)
	ClientRequest  func(ctx context.Context, node *MockSONode, req *rpc.ClientRequest) (*rpc.ClientReply, error)

	clientID uint64

	// optional element, useful for asserting called in client request
	t        *testing.T
	expected *rpc.ClientRequest
}

func (m *MockSONode) RegisterClientCaller(ctx context.Context, req *rpc.RegisterClientRequest) (*rpc.RegisterClientReply, error) {
	return m.RegisterClient(ctx, m, req)
}

func (m *MockSONode) ClientRequestCaller(ctx context.Context, req *rpc.ClientRequest) (*rpc.ClientReply, error) {
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

func (node *MockSONode) GracefulExit() {
	node.server.GracefulStop()
}

func CreateDefaultMockSONode() *MockSONode {
	node := templateMockSONode(defaultRegisterClientCaller, defaultClientRequestCaller)
	node.Leader = node.Self
	return node
}

func CreateFailMockSONode() *MockSONode {
	node := templateMockSONode(failRegisterClientCaller, failClientRequestCaller)
	node.Leader = node.Self
	return node
}

func CreateFollowerMockSONode(leader *rpc.RemoteNode) *MockSONode {
	node := templateMockSONode(followerRegisterClientCaller, followerClientRequestCaller)
	node.Leader = leader
	return node
}

func CreateCandidateMockSONode() *MockSONode {
	node := templateMockSONode(candidateRegisterClientCaller, candidateClientRequestCaller)
	node.Leader = node.Self
	return node
}

func CreateStartingMockSONode() *MockSONode {
	node := templateMockSONode(startingRegisterClientCaller, startingClientRequestCaller)
	return node
}

func CreateErrorMockSONode() *MockSONode {
	node := templateMockSONode(errorRegisterClientCaller, errorClientRequestCaller)
	return node
}

func templateMockSONode(
	registerCaller func(ctx context.Context, node *MockSONode, req *rpc.RegisterClientRequest) (*rpc.RegisterClientReply, error),
	requestCaller func(ctx context.Context, node *MockSONode, req *rpc.ClientRequest) (*rpc.ClientReply, error),
) *MockSONode {
	listener := util.OpenPort(0)
	node := &MockSONode{
		Self: &rpc.RemoteNode{
			Id:   util.AddrToID(listener.Addr().String(), raft.DefaultConfig().NodeIDSize),
			Addr: listener.Addr().String(),
		},
		server:         grpc.NewServer(),
		CurrentConfig:  util.NewConfiguration(util.NumShards),
		RegisterClient: registerCaller,
		ClientRequest:  requestCaller,
	}
	rpc.RegisterShardOrchestratorRPCServer(node.server, node)
	go node.server.Serve(listener)
	return node
}
