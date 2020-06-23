package raft

import (
	"math"

	"github.com/ziyaoh/some-kvstore/rpc"
)

// doFollower implements the logic for a Raft node in the follower state.
func (r *Node) doFollower() stateFunction {
	r.Out("Transitioning to FollowerState")
	r.State = FollowerState

	clientReply := rpc.ClientReply{
		Status:     rpc.ClientStatus_NOT_LEADER,
		Response:   nil,
		LeaderHint: r.Leader,
	}

	registerReply := rpc.RegisterClientReply{
		Status:     rpc.ClientStatus_NOT_LEADER,
		ClientId:   0,
		LeaderHint: r.Leader,
	}

	if r.Leader == nil {
		clientReply.LeaderHint = r.Self
		registerReply.LeaderHint = r.Self
	}

	r.requestsMutex.Lock()
	for _, replyToClient := range r.requestsByCacheID {
		replyToClient <- clientReply
	}
	r.requestsByCacheID = make(map[string]chan rpc.ClientReply)
	r.requestsMutex.Unlock()

	r.registrationsMutex.Lock()
	for _, replyToClient := range r.registrationsByLogIndex {
		replyToClient <- registerReply
	}
	r.registrationsByLogIndex = make(map[uint64]chan rpc.RegisterClientReply)
	r.registrationsMutex.Unlock()

	timeout := randomTimeout(r.config.ElectionTimeout)

	for {
		select {
		case shutdown := <-r.gracefulExit:
			if shutdown {
				return nil
			}
		case clientMsg := <-r.clientRequest:
			clientReply.LeaderHint = r.Leader
			clientMsg.reply <- clientReply

		case registerMsg := <-r.registerClient:
			registerReply.LeaderHint = r.Leader
			registerMsg.reply <- registerReply

		case voteMsg := <-r.requestVote:
			if r.handleRequestVote(voteMsg) {
				timeout = randomTimeout(r.config.ElectionTimeout)
			}

		case appendMsg := <-r.appendEntries:
			reset, _ := r.handleAppendEntries(appendMsg)
			if reset {
				timeout = randomTimeout(r.config.ElectionTimeout)
			}

		case <-timeout:
			r.Leader = nil
			return r.doCandidate
		}
	}
}

// handleAppendEntries handles an incoming AppendEntriesMsg. It is called by a
// node in a follower, candidate, or leader state. It returns two booleans:
// - resetTimeout is true if the follower node should reset the election timeout
// - fallback is true if the node should become a follower again
func (r *Node) handleAppendEntries(msg AppendEntriesMsg) (resetTimeout, fallback bool) {
	request := msg.request
	reply := msg.reply
	// If a server receives a request with a stale term number, it rejects the request (&5.1)
	if r.GetCurrentTerm() > request.GetTerm() {
		reply <- rpc.AppendEntriesReply{Term: r.GetCurrentTerm(), Success: false}
		return false, false
	}

	r.Leader = request.Leader
	if r.GetCurrentTerm() < request.GetTerm() {
		r.setCurrentTerm(request.GetTerm())
		r.setVotedFor("")
	}
	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if request.PrevLogIndex > 0 && (r.GetLog(request.PrevLogIndex) == nil || r.GetLog(request.PrevLogIndex).GetTermId() != request.GetPrevLogTerm()) {
		reply <- rpc.AppendEntriesReply{Term: r.GetCurrentTerm(), Success: false}
		return true, true
	}
	// Found a log entry whose term and index are matched with prevLogIndex and preLogTerm
	r.leaderMutex.Lock()
	for _, leaderLog := range request.GetEntries() {
		if leaderLog.GetIndex() > r.LastLogIndex() {
			// Append any new entries not already in the log
			r.StoreLog(leaderLog)
		} else if r.GetLog(leaderLog.GetIndex()) == nil || (leaderLog.GetTermId() != r.GetLog(leaderLog.GetIndex()).GetTermId()) {
			// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
			r.TruncateLog(leaderLog.GetIndex())
			// Append any new entries not already in the log
			r.StoreLog(leaderLog)
		}
	}
	r.leaderMutex.Unlock()
	if request.GetLeaderCommit() > r.commitIndex {
		r.commitIndex = uint64(math.Min(float64(request.GetLeaderCommit()), float64(r.LastLogIndex())))
		for r.commitIndex > r.lastApplied {
			r.lastApplied++
			r.processLogEntry(*r.GetLog(r.lastApplied))
		}
	}
	reply <- rpc.AppendEntriesReply{Term: r.GetCurrentTerm(), Success: true}
	return true, true
}

func (r *Node) handleRequestVote(msg RequestVoteMsg) (resetTimeout bool) {
	request := msg.request
	reply := msg.reply
	// If a server receives a request with a stale term number, it rejects the request (&5.1)
	if r.GetCurrentTerm() > request.GetTerm() {
		reply <- rpc.RequestVoteReply{Term: r.GetCurrentTerm(), VoteGranted: false}
		return false
	} else if r.GetCurrentTerm() < request.GetTerm() {
		r.setCurrentTerm(request.GetTerm())
		// If follower and candidate are in different term. Reset the follower's vote for
		r.setVotedFor("")
	}
	// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	lastTerm := r.GetLog(r.LastLogIndex()).GetTermId()
	if (r.GetVotedFor() == "" || r.GetVotedFor() == request.GetCandidate().GetId()) &&
		(lastTerm < request.GetLastLogTerm() ||
			(lastTerm == request.GetLastLogTerm() && r.LastLogIndex() <= request.GetLastLogIndex())) {
		r.setVotedFor(request.GetCandidate().GetId())
		reply <- rpc.RequestVoteReply{Term: r.GetCurrentTerm(), VoteGranted: true}
		return true
	}
	reply <- rpc.RequestVoteReply{Term: r.GetCurrentTerm(), VoteGranted: false}
	return false
}
