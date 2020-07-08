package shardorchestrator

import (
	"github.com/ziyaoh/some-kvstore/rpc"
	"golang.org/x/net/context"
)

// RegisterClientCaller is called through GRPC to respond to a client
// registration request.
func (local *Node) RegisterClientCaller(ctx context.Context, req *rpc.RegisterClientRequest) (*rpc.RegisterClientReply, error) {
	reply := local.RegisterClient(req)

	return &reply, nil
}

// ClientRequestCaller is called through GRPC to respond to a client request.
func (local *Node) ClientRequestCaller(ctx context.Context, req *rpc.ClientRequest) (*rpc.ClientReply, error) {
	reply := local.ClientRequest(req)

	return &reply, nil
}
