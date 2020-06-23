package raft

import (
	"github.com/ziyaoh/some-kvstore/rpc"
)

// joinRPC tells the given remote node that we (a new Raft node) want to join the cluster
func (local *Node) joinRPC(remote *rpc.RemoteNode) error {
	// Get a client connection for the remote node
	if local.NetworkPolicy.IsDenied(*local.Self, *remote) {
		return rpc.ErrorNetworkPolicyDenied
	}
	return remote.JoinRPC(local.Self)
}

// startNodeRPC tells the remote node to start execution with the given list
// of RemoteNodes as the list of all the nodes in the cluster.
func (local *Node) startNodeRPC(remote *rpc.RemoteNode, nodeList []*rpc.RemoteNode) error {
	if local.NetworkPolicy.IsDenied(*local.Self, *remote) {
		return rpc.ErrorNetworkPolicyDenied
	}
	return remote.StartNodeRPC(local.Self, nodeList)
}

// appendEntriesRPC is called by a leader in the cluster attempting to append
// entries to one of its followers. Be sure to pass in a pointer to the Node
// making the request.
func (local *Node) appendEntriesRPC(remote *rpc.RemoteNode, request *rpc.AppendEntriesRequest) (*rpc.AppendEntriesReply, error) {
	if local.NetworkPolicy.IsDenied(*local.Self, *remote) {
		return nil, rpc.ErrorNetworkPolicyDenied
	}
	return remote.AppendEntriesRPC(local.Self, request)
}

// requestVoteRPC asks the given remote node for a vote, using the provided
// RequestVoteRequest struct as the request. Note that calling nodes should
// pass in a pointer to their own Node struct.
func (local *Node) requestVoteRPC(remote *rpc.RemoteNode, request *rpc.RequestVoteRequest) (*rpc.RequestVoteReply, error) {
	if local.NetworkPolicy.IsDenied(*local.Self, *remote) {
		return nil, rpc.ErrorNetworkPolicyDenied
	}
	return remote.RequestVoteRPC(local.Self, request)

}
