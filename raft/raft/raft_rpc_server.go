package raft

import (
	"golang.org/x/net/context"
)

// JoinCaller is called through GRPC to execute a join request.
func (local *RaftNode) JoinCaller(ctx context.Context, r *RemoteNode) (*Ok, error) {
	// Check if the network policy prevents incoming requests from the requesting node
	if local.NetworkPolicy.IsDenied(*r, *local.Self) {
		return nil, ErrorNetworkPolicyDenied
	}

	err := local.Join(r)
	return &Ok{Ok: err == nil}, err
}

// StartNodeCaller is called through GRPC to execute a start node request.
func (local *RaftNode) StartNodeCaller(ctx context.Context, req *StartNodeRequest) (*Ok, error) {
	// Check if the network policy prevents incoming requests from the requesting node
	if local.NetworkPolicy.IsDenied(*req.FromNode, *local.Self) {
		return nil, ErrorNetworkPolicyDenied
	}

	err := local.StartNode(req)
	return &Ok{Ok: err == nil}, err
}

// AppendEntriesCaller is called through GRPC to respond to an append entries request.
func (local *RaftNode) AppendEntriesCaller(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesReply, error) {
	// Check if the network policy prevents incoming requests from the requesting node
	if local.NetworkPolicy.IsDenied(*req.Leader, *local.Self) {
		return nil, ErrorNetworkPolicyDenied
	}

	reply := local.AppendEntries(req)

	return &reply, nil
}

// RequestVoteCaller is called through GRPC to respond to a vote request.
func (local *RaftNode) RequestVoteCaller(ctx context.Context, req *RequestVoteRequest) (*RequestVoteReply, error) {
	// Check if the network policy prevents incoming requests from the requesting node
	if local.NetworkPolicy.IsDenied(*req.Candidate, *local.Self) {
		return nil, ErrorNetworkPolicyDenied
	}

	reply := local.RequestVote(req)

	return &reply, nil
}

// RegisterClientCaller is called through GRPC to respond to a client
// registration request.
func (local *RaftNode) RegisterClientCaller(ctx context.Context, req *RegisterClientRequest) (*RegisterClientReply, error) {
	reply := local.RegisterClient(req)

	return &reply, nil
}

// ClientRequestCaller is called through GRPC to respond to a client request.
func (local *RaftNode) ClientRequestCaller(ctx context.Context, req *ClientRequest) (*ClientReply, error) {
	reply := local.ClientRequest(req)

	return &reply, nil
}
