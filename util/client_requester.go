package util

import (
	"errors"
	"fmt"
	"time"

	"github.com/ziyaoh/some-kvstore/rpc"
)

// MaxRetries is the maximum times the Client will retry a request after
// receiving a REQ_FAILED reply from the Raft cluster
const MaxRetries = 3

const (
	ShardOrchestratorRequester uint64 = iota
	ReplicationGroupRequester
)

type Requester struct {
	ID          uint64          // Client ID, determined by the Raft node
	Mode        uint64          // Distinguish requster for replication group vs shard orchestrator
	Leader      *rpc.RemoteNode // Raft node we're connected to (also last known leader)
	SequenceNum uint64          // Sequence number of the latest request sent by the client
	ackSeqs     map[uint64]bool // Sequence numbers of finished requests to be acked
	// TODO: add state mutex to support concurrent request to replication group
	// stateMutex  sync.Mutex
}

// ConnectReplicationGroup creates a new Requester for a ReplicationGroup
func ConnectReplicationGroup(id uint64, addr string) (*Requester, error) {
	requester := new(Requester)

	// Note: we don't yet know the ID of the remoteNode, so just set it to an
	// empty string.
	remoteNode := &rpc.RemoteNode{Id: "", Addr: addr}

	// We've registered with the leader!
	requester.ID = id
	requester.Mode = ReplicationGroupRequester
	requester.Leader = remoteNode
	requester.ackSeqs = make(map[uint64]bool)

	return requester, nil
}

// ConnectShardOrchestrator creates a new Requester for ShardOrchestrator and registers with the Raft node at the given address.
func ConnectShardOrchestrator(addr string, idempotent bool, idempotencyID uint64) (requester *Requester, err error) {
	requester = new(Requester)

	// Note: we don't yet know the ID of the remoteNode, so just set it to an
	// empty string.
	remoteNode := &rpc.RemoteNode{Id: "", Addr: addr}

	var reply *rpc.RegisterClientReply
	retries := 0

	request := rpc.RegisterClientRequest{}
	if idempotent {
		request.Idempotent = idempotent
		request.IdempotencyID = idempotencyID
	}

	for retries < MaxRetries {
		reply, err = remoteNode.RegisterClientRPC(&request)
		if err != nil {
			return nil, err
		}

		switch reply.Status {
		case rpc.ClientStatus_OK:
			Out.Output(2, fmt.Sprintf("%v is the leader\n", requester.Leader))
			Out.Output(2, fmt.Sprintf("Register returned ID \"%v\"\n", reply.ClientId))
			requester.ID = reply.ClientId
			requester.Mode = ShardOrchestratorRequester
			requester.Leader = remoteNode
			requester.ackSeqs = make(map[uint64]bool)
			return requester, nil
		case rpc.ClientStatus_REQ_FAILED:
			Out.Output(2, fmt.Sprintf("Request failed\n"))
			Out.Output(2, "Retrying...\n")
			retries++
		case rpc.ClientStatus_NOT_LEADER:
			// The person we've contacted isn't the leader. Use their hint to find
			// the leader.
			if reply.LeaderHint.Addr == remoteNode.Addr {
				time.Sleep(200 * time.Millisecond)
			}
			remoteNode = reply.LeaderHint
		case rpc.ClientStatus_ELECTION_IN_PROGRESS:
			// An election is in progress. Accept the hint and wait an appropriate
			// amount of time, so the election can finish.
			remoteNode = reply.LeaderHint
			time.Sleep(time.Millisecond * 200)
		case rpc.ClientStatus_CLUSTER_NOT_STARTED:
			return nil, errors.New("cluster hasn't started")
		}
	}

	return nil, errors.New("request failed")
}

// SendRequest sends a command the associated data to the last known leader of
// the Raft cluster, and handles responses.
func (requester *Requester) SendRequest(command uint64, data []byte) ([]byte, error) {
	ackSeqs := make([]uint64, 0)
	for seq := range requester.ackSeqs {
		ackSeqs = append(ackSeqs, seq)
	}
	curSeq := requester.SequenceNum
	requester.SequenceNum++
	request := rpc.ClientRequest{
		ClientId:        requester.ID,
		SequenceNum:     curSeq,
		StateMachineCmd: command,
		Data:            data,
		AckSeqs:         ackSeqs,
	}

	var reply *rpc.ClientReply
	var err error
	retries := 0

	for retries < MaxRetries {
		reply, err = requester.request(request)
		if err != nil {
			return nil, err
		}

		switch reply.Status {
		case rpc.ClientStatus_OK:
			Out.Output(2, fmt.Sprintf("%v is the leader\n", requester.Leader))
			Out.Output(2, fmt.Sprintf("Request returned \"%v\"\n", reply.Response))

			for _, seq := range ackSeqs {
				delete(requester.ackSeqs, seq)
			}
			requester.ackSeqs[curSeq] = true

			return reply.GetResponse(), nil
		case rpc.ClientStatus_REQ_FAILED:
			Out.Output(2, fmt.Sprintf("Request failed: %v\n", reply.Response))
			Out.Output(2, "Retrying...\n")
			retries++
		case rpc.ClientStatus_NOT_LEADER:
			// The person we've contacted isn't the leader. Use their hint to find
			// the leader.
			if reply.LeaderHint.Addr == requester.Leader.Addr {
				time.Sleep(200 * time.Millisecond)
			}
			requester.Leader = reply.LeaderHint
		case rpc.ClientStatus_ELECTION_IN_PROGRESS:
			// An election is in progress. Accept the hint and wait an appropriate
			// amount of time, so the election can finish.
			requester.Leader = reply.LeaderHint
			time.Sleep(time.Millisecond * 200)
		case rpc.ClientStatus_CLUSTER_NOT_STARTED:
			return nil, errors.New("cluster hasn't started")
		}
	}

	return nil, fmt.Errorf("Requester: request failed: %s", string(reply.GetResponse()))
}

func (requester *Requester) request(request rpc.ClientRequest) (*rpc.ClientReply, error) {
	switch requester.Mode {
	case ReplicationGroupRequester:
		return requester.Leader.ReplicationGroupClientRequestRPC(&request)
	case ShardOrchestratorRequester:
		return requester.Leader.ShardOrchestratorClientRequestRPC(&request)
	default:
		return nil, fmt.Errorf("Requester: unknown requester mode: %v", requester.Mode)
	}
}
