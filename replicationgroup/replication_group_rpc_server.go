package replicationgroup

import (
	"github.com/ziyaoh/some-kvstore/rpc"
	"golang.org/x/net/context"
)

// ClientRequestCaller is called through GRPC to respond to a client request.
func (local *Node) ClientRequestCaller(ctx context.Context, req *rpc.ClientRequest) (*rpc.ClientReply, error) {
	reply := local.ClientRequest(req)

	return &reply, nil
}
