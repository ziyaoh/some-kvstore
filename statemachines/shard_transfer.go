package statemachines

import (
	"errors"
	"fmt"
	"time"

	errHelp "github.com/pkg/errors"
	"github.com/ziyaoh/some-kvstore/rpc"
	"github.com/ziyaoh/some-kvstore/util"
)

// Transferer represents a client that's responsible for transfering shards of kv pairs to another replication group
type Transferer struct {
	ID  uint64
	seq uint64
}

// NewTransferer creates a new Transferer
func NewTransferer(id uint64) (cp *Transferer, err error) {
	cp = new(Transferer)
	cp.ID = id

	return cp, nil
}

// Transfer hands off a shard to another replication group
func (trans *Transferer) Transfer(destAddrs []string, payload ShardInPayload) error {
	reqSeq := trans.seq
	trans.seq++

	data, err := util.EncodeMsgPack(payload)
	if err != nil {
		return errHelp.Wrapf(err, "Transferer: encoding shardInPayload fail\n")
	}
	for _, addr := range destAddrs {
		remoteNode := &rpc.RemoteNode{Id: "", Addr: addr}

		request := rpc.ClientRequest{
			ClientId:        trans.ID,
			SequenceNum:     reqSeq,
			StateMachineCmd: ShardIn,
			Data:            data,
		}

		var reply *rpc.ClientReply
		var err error
		retries := 0

		for retries < util.MaxRetries {
			reply, err = remoteNode.ReplicationGroupClientRequestRPC(&request)
			if err != nil {
				return err
			}

			switch reply.Status {
			case rpc.ClientStatus_OK:
				util.Out.Output(2, fmt.Sprintf("%v is the leader\n", remoteNode))
				util.Out.Output(2, fmt.Sprintf("Request returned \"%v\"\n", reply.Response))

				return nil
			case rpc.ClientStatus_REQ_FAILED:
				util.Out.Output(2, fmt.Sprintf("Request failed: %v\n", reply.Response))
				util.Out.Output(2, "Retrying...\n")
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
				return errors.New("cluster hasn't started")
			}
		}
	}
	return fmt.Errorf("Shard Transferer: shard request failed on all dest addr")
}
