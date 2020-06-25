package raft

import (
	"sort"
	"time"

	"github.com/ziyaoh/some-kvstore/rpc"
)

// doLeader implements the logic for a Raft node in the leader state.
func (r *Node) doLeader() stateFunction {
	r.Out("Transitioning to LeaderState")
	r.State = LeaderState

	r.Leader = r.Self
	// When a leader first comes to power, it initializes all nextIndex values to the index just after the last one in its log (&5.3)
	r.leaderMutex.Lock()
	r.nextIndex = make(map[string]uint64)
	r.matchIndex = make(map[string]uint64)
	for _, peer := range r.Peers {
		r.nextIndex[peer.GetId()] = r.LastLogIndex() + uint64(1)
		r.matchIndex[peer.GetId()] = 0
	}
	noopLog := rpc.LogEntry{
		Index:  r.LastLogIndex() + 1,
		TermId: r.GetCurrentTerm(),
		Type:   rpc.CommandType_NOOP,
	}
	r.StoreLog(&noopLog)
	r.leaderMutex.Unlock()

	fallbackChan := make(chan bool)
	go func() {
		fallback, _ := r.sendHeartbeats()
		if fallback {
			fallbackChan <- true
		}
	}()

	ticker := time.NewTicker(r.config.HeartbeatTimeout)
	for {
		select {
		case shutdown := <-r.gracefulExit:
			if shutdown {
				return nil
			}
		case clientMsg := <-r.clientRequest:
			request := clientMsg.request
			reply := clientMsg.reply
			r.handleClientRequest(request, reply)

		case registerMsg := <-r.registerClient:
			reply := registerMsg.reply

			r.handleRegisterClient(reply)

		case voteMsg := <-r.requestVote:
			if r.handleCompetingRequestVote(voteMsg) {
				return r.doFollower
			}

		case appendMsg := <-r.appendEntries:
			_, fallback := r.handleAppendEntries(appendMsg)
			if fallback {
				return r.doFollower
			}

		case fallbackFromHB := <-fallbackChan:
			if fallbackFromHB {
				return r.doFollower
			}

		case <-ticker.C:
			// send heartbeat in go routine
			go func() {
				fallback, _ := r.sendHeartbeats()
				if fallback {
					fallbackChan <- true
				}
			}()
		}
	}
}

type HeartbeatResult string

const (
	HeartbeatSuccess  HeartbeatResult = "success"
	HeartbeatFail                     = "fail"
	HeartbeatFallback                 = "fallback"
)

func (r *Node) sendHeartbeat(peer *rpc.RemoteNode, msg *rpc.AppendEntriesRequest, resultChan chan HeartbeatResult) {
	// reply, err := peer.AppendEntriesRPC(r.Self, msg)
	reply, err := r.appendEntriesRPC(peer, msg)

	if err == nil {
		if reply.GetSuccess() {
			resultChan <- HeartbeatSuccess

			r.leaderMutex.Lock()
			numEntries := uint64(len(msg.Entries))
			r.nextIndex[peer.GetId()] += numEntries
			r.matchIndex[peer.GetId()] = r.nextIndex[peer.GetId()] - 1
			r.leaderMutex.Unlock()
			r.tryCommit()
		} else {
			if reply.GetTerm() > r.GetCurrentTerm() {
				r.setCurrentTerm(reply.GetTerm())
				resultChan <- HeartbeatFallback
				return
			}
			resultChan <- HeartbeatFail
			// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
			r.leaderMutex.Lock()
			r.nextIndex[peer.GetId()] -= uint64(1)
			r.leaderMutex.Unlock()
		}
	} else {
		resultChan <- HeartbeatFail
	}
}

func (r *Node) tryCommit() {
	r.leaderMutex.Lock()
	values := make(SortableUint64Slice, 0)
	for _, node := range r.Peers {
		values = append(values, r.matchIndex[node.GetId()])
	}

	sort.Sort(values)
	commitableIndex := values[len(values)/2]
	if commitableIndex > r.commitIndex && r.GetCurrentTerm() == r.GetLog(commitableIndex).GetTermId() {
		// commit and process log
		start := r.lastApplied
		for index := start + 1; index <= commitableIndex; index++ {
			r.processLogEntry(*r.GetLog(index))
			r.lastApplied = index
		}
		r.commitIndex = commitableIndex
	}
	r.leaderMutex.Unlock()
}

// sendHeartbeats is used by the leader to send out heartbeats to each of
// the other nodes. It returns true if the leader should fall back to the
// follower state. (This happens if we discover that we are in an old term.)
//
// If another node isn't up-to-date, then the leader should attempt to
// update them, and, if an index has made it to a quorum of nodes, commit
// up to that index. Once committed to that index, the replicated state
// machine should be given the new log entries via processLogEntry.
func (r *Node) sendHeartbeats() (fallback, sentToMajority bool) {
	r.leaderMutex.Lock()
	leaderCommit := r.commitIndex
	r.leaderMutex.Unlock()
	allLogs := r.stableStore.AllLogs()
	resultChan := make(chan HeartbeatResult)
	total := 0
	for _, peer := range r.Peers {
		if r.Self.GetId() == peer.GetId() {
			r.leaderMutex.Lock()
			r.nextIndex[peer.GetId()] = r.LastLogIndex() + uint64(1)
			r.matchIndex[peer.GetId()] = r.LastLogIndex()
			r.leaderMutex.Unlock()
		} else {
			r.leaderMutex.Lock()
			nextInd := r.nextIndex[peer.GetId()]
			r.leaderMutex.Unlock()
			msg := rpc.AppendEntriesRequest{
				Term:         r.GetCurrentTerm(),
				Leader:       r.Self,
				PrevLogIndex: nextInd - 1,
				PrevLogTerm:  r.GetLog(nextInd - 1).GetTermId(),
				Entries:      allLogs[nextInd:],
				LeaderCommit: leaderCommit,
			}
			total++
			go r.sendHeartbeat(peer, &msg, resultChan)
		}
	}

	if total == 0 {
		r.tryCommit()
		return
	}

	successCount := 0
	remaining := total
	for remaining > 0 {
		heartbeatResult := <-resultChan
		remaining--
		if heartbeatResult == HeartbeatFallback {
			return true, false
		}
		if heartbeatResult == HeartbeatSuccess {
			successCount++
		}
	}

	return false, successCount >= total/2
}

func (r *Node) handleClientRequest(request *rpc.ClientRequest, replyChannel chan rpc.ClientReply) {
	cacheID := createCacheID(request.ClientId, request.SequenceNum)
	r.requestsMutex.Lock()
	oldChannel, exist := r.requestsByCacheID[cacheID]
	if exist {
		// duplicate request: result under processing
		ch := make(chan rpc.ClientReply)
		r.requestsByCacheID[cacheID] = ch
		go func() {
			response := <-ch
			oldChannel <- response
			replyChannel <- response
		}()
		r.requestsMutex.Unlock()
		return
	}
	r.requestsByCacheID[cacheID] = replyChannel
	r.requestsMutex.Unlock()

	// new request
	r.leaderMutex.Lock()
	ackCache := make([]string, 0)
	for _, seq := range request.AckSeqs {
		ackID := createCacheID(request.ClientId, seq)
		ackCache = append(ackCache, ackID)
	}
	logEntry := rpc.LogEntry{
		Index:       r.LastLogIndex() + 1,
		TermId:      r.GetCurrentTerm(),
		Type:        rpc.CommandType_STATE_MACHINE_COMMAND,
		Command:     request.StateMachineCmd,
		Data:        request.Data,
		CacheId:     cacheID,
		AckCacheIds: ackCache,
	}
	r.StoreLog(&logEntry)
	r.leaderMutex.Unlock()
}

func (r *Node) handleRegisterClient(replyChannel chan rpc.RegisterClientReply) {
	r.leaderMutex.Lock()
	logEntry := rpc.LogEntry{
		Index:  r.LastLogIndex() + 1,
		TermId: r.GetCurrentTerm(),
		Type:   rpc.CommandType_CLIENT_REGISTRATION,
	}

	r.registrationsMutex.Lock()
	_, exist := r.registrationsByLogIndex[logEntry.Index]
	if exist {
		panic("should not exist")
	}
	r.registrationsByLogIndex[logEntry.Index] = replyChannel
	r.registrationsMutex.Unlock()

	r.StoreLog(&logEntry)
	r.leaderMutex.Unlock()
}

// processLogEntry applies a single log entry to the finite state machine. It is
// called once a log entry has been replicated to a majority and committed by
// the leader. Once the entry has been applied, the leader responds to the client
// with the result, and also caches the response.
func (r *Node) processLogEntry(entry rpc.LogEntry) {
	r.Out("Processing log entry: %v", entry)

	for _, cacheID := range entry.AckCacheIds {
		r.RemoveCachedReply(cacheID)
	}

	if entry.Type == rpc.CommandType_CLIENT_REGISTRATION {
		registerReply := rpc.RegisterClientReply{
			Status:     rpc.ClientStatus_OK,
			ClientId:   entry.Index,
			LeaderHint: r.Self,
		}
		r.registrationsMutex.Lock()
		replyChan, exists := r.registrationsByLogIndex[entry.Index]
		if exists {
			replyChan <- registerReply
			delete(r.registrationsByLogIndex, entry.Index)
		}
		r.registrationsMutex.Unlock()
		return
	}

	status := rpc.ClientStatus_OK
	response := []byte{}
	var err error

	// Apply command on state machine
	if entry.Type == rpc.CommandType_STATE_MACHINE_COMMAND {
		response, err = r.stateMachine.ApplyCommand(entry.Command, entry.Data)
		if err != nil {
			status = rpc.ClientStatus_REQ_FAILED
			response = []byte(err.Error())
		}
	}

	// Construct reply
	reply := rpc.ClientReply{
		Status:     status,
		Response:   response,
		LeaderHint: r.Self,
	}

	// Add reply to cache
	if entry.CacheId != "" {
		r.CacheClientReply(entry.CacheId, reply)
	}

	// Send reply to client
	r.requestsMutex.Lock()
	replyChan, exists := r.requestsByCacheID[entry.CacheId]
	if exists {
		replyChan <- reply
		delete(r.requestsByCacheID, entry.CacheId)
	}
	r.requestsMutex.Unlock()
}
