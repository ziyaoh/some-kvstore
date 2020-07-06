package rpc

import (
	"golang.org/x/net/context"
)

// ReplicationGroupRPCClientConn creates or returns a cached RPC client for the given remote node
func (remote *RemoteNode) ReplicationGroupRPCClientConn() (ReplicationGroupRPCClient, error) {
	connMapLock.RLock()
	if cc, ok := connMap[remote.Addr]; ok {
		connMapLock.RUnlock()
		return NewReplicationGroupRPCClient(cc), nil
	}
	connMapLock.RUnlock()

	cc, err := makeClientConn(remote)
	if err != nil {
		return nil, err
	}
	connMapLock.Lock()
	connMap[remote.Addr] = cc
	connMapLock.Unlock()

	return NewReplicationGroupRPCClient(cc), err
}

// ReplicationGroupClientRequestRPC is executed by a client trying to make a request to the
// given Raft node in the cluster.
func (remote *RemoteNode) ReplicationGroupClientRequestRPC(request *ClientRequest) (*ClientReply, error) {
	cc, err := remote.ReplicationGroupRPCClientConn()
	if err != nil {
		return nil, err
	}

	reply, err := cc.ClientRequestCaller(context.Background(), request)
	return reply, remote.connCheck(err)
}
