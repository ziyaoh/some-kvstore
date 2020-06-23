package raft

import "github.com/ziyaoh/some-kvstore/rpc"

// doCandidate implements the logic for a Raft node in the candidate state.
func (r *Node) doCandidate() stateFunction {
	r.Out("Transitioning to CandidateState")
	r.State = CandidateState

	// Foollowing &5.2
	// Increment currentTerm
	r.setCurrentTerm(r.GetCurrentTerm() + 1)
	// Vote for self
	r.setVotedFor(r.Self.GetId())
	// Reset election timer
	timeout := randomTimeout(r.config.ElectionTimeout)
	electionResults := make(chan bool)
	fallbackChan := make(chan bool)
	go r.requestVotes(electionResults, fallbackChan, r.GetCurrentTerm())
	for {
		select {
		case shutdown := <-r.gracefulExit:
			if shutdown {
				return nil
			}

		case clientMsg := <-r.clientRequest:
			clientMsg.reply <- rpc.ClientReply{
				Status:     rpc.ClientStatus_ELECTION_IN_PROGRESS,
				Response:   nil,
				LeaderHint: r.Self,
			}

		case registerMsg := <-r.registerClient:
			registerMsg.reply <- rpc.RegisterClientReply{
				Status:     rpc.ClientStatus_ELECTION_IN_PROGRESS,
				ClientId:   0,
				LeaderHint: r.Self,
			}

		case voteMsg := <-r.requestVote:
			if r.handleCompetingRequestVote(voteMsg) {
				return r.doFollower
			}

		case appendMsg := <-r.appendEntries:
			_, toFollower := r.handleAppendEntries(appendMsg)
			if toFollower {
				return r.doFollower
			}

		case elected := <-electionResults:
			if elected {
				return r.doLeader
			}

		case toFollower := <-fallbackChan:
			if toFollower {
				return r.doFollower
			}

		case <-timeout:
			return r.doCandidate
		}
	}
}

type RequestVoteResult string

const (
	RequestVoteSuccess  RequestVoteResult = "success"
	RequestVoteFail                       = "fail"
	RequestVoteFallback                   = "fallback"
)

func (r *Node) requestPeerVote(peer *rpc.RemoteNode, msg *rpc.RequestVoteRequest, resultChan chan RequestVoteResult) {
	// reply, err := peer.RequestVoteRPC(r.Self, msg)
	reply, err := r.requestVoteRPC(peer, msg)

	if err != nil {
		r.Error("Error in requesting a vote from %v", peer.GetId())
		resultChan <- RequestVoteFail
	} else {
		if reply.GetVoteGranted() {
			resultChan <- RequestVoteSuccess
		} else if reply.GetTerm() > r.GetCurrentTerm() {
			r.setCurrentTerm(reply.GetTerm())
			r.setVotedFor("")
			resultChan <- RequestVoteFallback
		} else {
			resultChan <- RequestVoteFail
		}
	}
}

// requestVotes is called to request votes from all other nodes. It takes in a
// channel on which the result of the vote should be sent over: true for a
// successful election, false otherwise.
func (r *Node) requestVotes(electionResults chan bool, fallbackChan chan bool, currTerm uint64) {
	// Votes received
	remaining := 0
	resultChan := make(chan RequestVoteResult)
	for _, peer := range r.Peers {
		if r.Self.GetId() == peer.GetId() {
			continue
		}
		msg := rpc.RequestVoteRequest{
			Term:         currTerm,
			Candidate:    r.Self,
			LastLogIndex: r.LastLogIndex(),
			LastLogTerm:  r.GetLog(r.LastLogIndex()).GetTermId(),
		}
		remaining++
		go r.requestPeerVote(peer, &msg, resultChan)
	}

	vote := 1
	reject := 0
	majority := r.config.ClusterSize/2 + 1
	if vote >= majority {
		electionResults <- true
		return
	}
	for remaining > 0 {
		requestVoteResult := <-resultChan
		remaining--
		if requestVoteResult == RequestVoteFallback {
			fallbackChan <- true
			return
		}
		if requestVoteResult == RequestVoteSuccess {
			vote++
			if vote >= majority {
				electionResults <- true
				return
			}
		} else {
			reject++
			if reject >= majority {
				electionResults <- false
				return
			}
		}
	}
}

// handleCompetingRequestVote handles an incoming vote request when the current
// node is in the candidate or leader state. It returns true if the caller
// should fall back to the follower state, false otherwise.
func (r *Node) handleCompetingRequestVote(msg RequestVoteMsg) (fallback bool) {
	request := msg.request
	reply := msg.reply
	// If a server receives a request with a stale term number, it rejects the request (&5.1)
	if r.GetCurrentTerm() >= request.GetTerm() {
		reply <- rpc.RequestVoteReply{Term: r.GetCurrentTerm(), VoteGranted: false}
		return false
	}
	r.setCurrentTerm(request.GetTerm())
	// If follower and candidate are in different term. Reset the follower's vote for
	r.setVotedFor("")
	// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	lastTerm := r.GetLog(r.LastLogIndex()).GetTermId()
	if lastTerm < request.GetLastLogTerm() ||
		(lastTerm == request.GetLastLogTerm() && r.LastLogIndex() <= request.GetLastLogIndex()) {
		r.setVotedFor(request.GetCandidate().GetId())
		reply <- rpc.RequestVoteReply{Term: r.GetCurrentTerm(), VoteGranted: true}
		return true
	}
	reply <- rpc.RequestVoteReply{Term: r.GetCurrentTerm(), VoteGranted: false}
	return true
}
