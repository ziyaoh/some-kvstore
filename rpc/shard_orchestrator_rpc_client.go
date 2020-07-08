package rpc

import "golang.org/x/net/context"

// ShardOrchestratorRPCClientConn creates or returns a cached RPC client for the given remote node
func (remote *RemoteNode) ShardOrchestratorRPCClientConn() (ShardOrchestratorRPCClient, error) {
	connMapLock.RLock()
	if cc, ok := connMap[remote.Addr]; ok {
		connMapLock.RUnlock()
		return NewShardOrchestratorRPCClient(cc), nil
	}
	connMapLock.RUnlock()

	cc, err := makeClientConn(remote)
	if err != nil {
		return nil, err
	}
	connMapLock.Lock()
	connMap[remote.Addr] = cc
	connMapLock.Unlock()

	return NewShardOrchestratorRPCClient(cc), err
}

// RegisterClientRPC is called by a new client trying to register itself with
// the given Raft node in the cluster.
func (remote *RemoteNode) RegisterClientRPC() (*RegisterClientReply, error) {
	cc, err := remote.ShardOrchestratorRPCClientConn()
	if err != nil {
		return nil, err
	}

	request := RegisterClientRequest{}

	reply, err := cc.RegisterClientCaller(context.Background(), &request)
	return reply, remote.connCheck(err)
}

// ShardOrchestratorClientRequestRPC is executed by a client trying to make a request to the
// given Raft node in the cluster.
func (remote *RemoteNode) ShardOrchestratorClientRequestRPC(request *ClientRequest) (*ClientReply, error) {
	cc, err := remote.ShardOrchestratorRPCClientConn()
	if err != nil {
		return nil, err
	}

	reply, err := cc.ClientRequestCaller(context.Background(), request)
	return reply, remote.connCheck(err)
}
