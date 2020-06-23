package raft

import (
	"github.com/ziyaoh/some-kvstore/rpc"
	"golang.org/x/net/context"
)

// JoinCaller is called through GRPC to execute a join request.
func (local *Node) JoinCaller(ctx context.Context, r *rpc.RemoteNode) (*rpc.Ok, error) {
	// Check if the network policy prevents incoming requests from the requesting node
	if local.NetworkPolicy.IsDenied(*r, *local.Self) {
		return nil, rpc.ErrorNetworkPolicyDenied
	}

	err := local.Join(r)
	return &rpc.Ok{Ok: err == nil}, err
}

// StartNodeCaller is called through GRPC to execute a start node request.
func (local *Node) StartNodeCaller(ctx context.Context, req *rpc.StartNodeRequest) (*rpc.Ok, error) {
	// Check if the network policy prevents incoming requests from the requesting node
	if local.NetworkPolicy.IsDenied(*req.FromNode, *local.Self) {
		return nil, rpc.ErrorNetworkPolicyDenied
	}

	err := local.StartNode(req)
	return &rpc.Ok{Ok: err == nil}, err
}

// AppendEntriesCaller is called through GRPC to respond to an append entries request.
func (local *Node) AppendEntriesCaller(ctx context.Context, req *rpc.AppendEntriesRequest) (*rpc.AppendEntriesReply, error) {
	// Check if the network policy prevents incoming requests from the requesting node
	if local.NetworkPolicy.IsDenied(*req.Leader, *local.Self) {
		return nil, rpc.ErrorNetworkPolicyDenied
	}

	reply := local.AppendEntries(req)

	return &reply, nil
}

// RequestVoteCaller is called through GRPC to respond to a vote request.
func (local *Node) RequestVoteCaller(ctx context.Context, req *rpc.RequestVoteRequest) (*rpc.RequestVoteReply, error) {
	// Check if the network policy prevents incoming requests from the requesting node
	if local.NetworkPolicy.IsDenied(*req.Candidate, *local.Self) {
		return nil, rpc.ErrorNetworkPolicyDenied
	}

	reply := local.RequestVote(req)

	return &reply, nil
}

// TODO: move to shard master
// RegisterClientCaller is called through GRPC to respond to a client
// registration request.
// func (local *Node) RegisterClientCaller(ctx context.Context, req *RegisterClientRequest) (*RegisterClientReply, error) {
// 	reply := local.RegisterClient(req)

// 	return &reply, nil
// }
