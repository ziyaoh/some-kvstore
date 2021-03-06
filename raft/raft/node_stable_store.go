package raft

import (
	"errors"
	"fmt"

	"github.com/ziyaoh/some-kvstore/rpc"
	"github.com/ziyaoh/some-kvstore/util"
)

// ////////////////////////////////////////////////////////////////////////////////
// // High level API for StableStore                                        	 //
// ////////////////////////////////////////////////////////////////////////////////

// initStore stores zero-index log
func (r *Node) initStableStore() {
	r.StoreLog(&rpc.LogEntry{
		Index:  0,
		TermId: 0,
		Type:   rpc.CommandType_NOOP,
		Data:   []byte{},
	})
}

// setCurrentTerm sets the current node's term and writes log to disk
func (r *Node) setCurrentTerm(newTerm uint64) {
	currentTerm := r.GetCurrentTerm()
	if newTerm != currentTerm {
		r.Out("Setting current term from %v -> %v", currentTerm, newTerm)
	}
	err := r.stableStore.SetUint64([]byte("current_term"), newTerm)
	if err != nil {
		r.Error("Unable to flush new term to disk: %v", err)
		panic(err)
	}
}

// GetCurrentTerm returns the current node's term
func (r *Node) GetCurrentTerm() uint64 {
	return r.stableStore.GetUint64([]byte("current_term"))
}

// setVotedFor sets the candidateId for which the current node voted for, and writes log to disk
func (r *Node) setVotedFor(candidateID string) {
	err := r.stableStore.SetBytes([]byte("voted_for"), []byte(candidateID))
	if err != nil {
		r.Error("Unable to flush new votedFor to disk: %v", err)
		panic(err)
	}
}

// GetVotedFor returns the Id of the candidate that the current node voted for
func (r *Node) GetVotedFor() string {
	return string(r.stableStore.GetBytes([]byte("voted_for")))
}

// CacheClientRegistration caches the given register client reply with the provided ID.
func (r *Node) CacheClientRegistration(idempotencyID string, reply rpc.RegisterClientReply) error {
	key := []byte(fmt.Sprintf("registrationID:%v", idempotencyID))
	if value := r.stableStore.GetBytes(key); value != nil {
		return errors.New("registration with the same idempotencyID already exists")
	}

	bytes, err := util.EncodeMsgPack(reply)
	if err != nil {
		return err
	}
	err = r.stableStore.SetBytes(key, bytes)
	if err != nil {
		r.Error("Unable to flush new client registration to disk: %v", err)
		panic(err)
	}
	return nil
}

// GetCachedRegistration checks if the given register client request has a cached response.
// It returns the cached response (or nil) and a boolean indicating whether or not
// a cached response existed.
func (r *Node) GetCachedRegistration(registerClientReq rpc.RegisterClientRequest) (*rpc.RegisterClientReply, bool) {
	if !registerClientReq.Idempotent {
		return nil, false
	}
	key := []byte(fmt.Sprintf("registrationID:%v", string(registerClientReq.IdempotencyID)))

	if value := r.stableStore.GetBytes(key); value != nil {
		var reply rpc.RegisterClientReply
		util.DecodeMsgPack(value, &reply)
		return &reply, true
	}
	return nil, false
}

// CacheClientReply caches the given client response with the provided cache ID.
func (r *Node) CacheClientReply(cacheID string, reply rpc.ClientReply) error {
	key := []byte("cacheID:" + cacheID)
	if value := r.stableStore.GetBytes(key); value != nil {
		return errors.New("request with the same clientId and seqNum already exists")
	}

	bytes, err := util.EncodeMsgPack(reply)
	if err != nil {
		return err
	}
	err = r.stableStore.SetBytes(key, bytes)
	if err != nil {
		r.Error("Unable to flush new client request to disk: %v", err)
		panic(err)
	}
	return nil
}

// GetCachedReply checks if the given client request has a cached response.
// It returns the cached response (or nil) and a boolean indicating whether or not
// a cached response existed.
func (r *Node) GetCachedReply(clientReq rpc.ClientRequest) (*rpc.ClientReply, bool) {
	cacheID := createCacheID(clientReq.ClientId, clientReq.SequenceNum)
	key := []byte("cacheID:" + cacheID)

	if value := r.stableStore.GetBytes(key); value != nil {
		var reply rpc.ClientReply
		util.DecodeMsgPack(value, &reply)
		return &reply, true
	}
	return nil, false
}

// RemoveCachedReply remove the cached client reply according to the passed in cacheID
// It does nothing if cacheID does not exist
func (r *Node) RemoveCachedReply(cacheID string) error {
	key := []byte("cacheID:" + cacheID)

	return r.stableStore.RemoveBytes(key)
}

// LastLogIndex returns index of last log. If no log exists, it returns 0.
func (r *Node) LastLogIndex() uint64 {
	return r.stableStore.LastLogIndex()
}

// StoreLog appends log to log entry. Should always succeed
func (r *Node) StoreLog(log *rpc.LogEntry) {
	err := r.stableStore.StoreLog(log)
	if err != nil {
		panic(err)
	}
}

// GetLog gets a log at a specific index. If log does not exist, GetLog returns nil
func (r *Node) GetLog(index uint64) *rpc.LogEntry {
	return r.stableStore.GetLog(index)
}

// TruncateLog deletes logs from index to end of logs. Should always succeed
func (r *Node) TruncateLog(index uint64) {
	err := r.stableStore.TruncateLog(index)
	if err != nil {
		panic(err)
	}
}

// RemoveLogs removes data from stableStore
func (r *Node) RemoveLogs() {
	r.stableStore.Remove()
}
