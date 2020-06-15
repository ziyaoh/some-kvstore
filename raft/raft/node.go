package raft

import (
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/ziyaoh/some-kvstore/rpc"
	"github.com/ziyaoh/some-kvstore/util"
	"google.golang.org/grpc"
)

// NodeState represents one of four possible states a Raft node can be in.
type NodeState int

// Enum for potential node states
const (
	FollowerState NodeState = iota
	CandidateState
	LeaderState
	JoinState
	ExitState
)

// Node defines an individual Raft node.
type Node struct {
	State  NodeState
	Self   *rpc.RemoteNode // move (maybe, or both)
	Leader *rpc.RemoteNode
	Peers  []*rpc.RemoteNode
	config *Config
	// port      int // move (maybe, or both)
	nodeMutex sync.Mutex

	server        *grpc.Server // move (maybe, or both)
	NetworkPolicy *rpc.NetworkPolicy

	// Stable store (written to disk, use helper methods)
	stableStore StableStore

	// Replicated state machine (e.g. hash machine, kv-store etc.)
	stateMachine StateMachine

	// Leader specific volatile state
	commitIndex uint64
	lastApplied uint64
	nextIndex   map[string]uint64
	matchIndex  map[string]uint64
	leaderMutex sync.Mutex

	// Channels to send / receive various RPC messages (used in state functions)
	appendEntries chan AppendEntriesMsg
	requestVote   chan RequestVoteMsg
	// registerClient chan RegisterClientMsg
	clientRequest chan rpc.ClientRequestMsg
	gracefulExit  chan bool

	// Client request map (used to store channels to respond through once a
	// request has been processed)
	requestsByCacheID map[string]chan rpc.ClientReply
	requestsMutex     sync.Mutex
}

// CreateNode is used to construct a new Raft node. It takes a configuration,
// as well as implementations of various interfaces that are required.
// If we have any old state, such as snapshots, logs, Peers, etc, all those will be restored when creating the Raft node.
// Use port=0 for auto selection
func CreateNode(listener net.Listener, server *grpc.Server, connect *rpc.RemoteNode, config *Config, stateMachine StateMachine, stableStore StableStore) (*Node, error) {
	r := new(Node)

	// Set remote self based on listener address
	r.Self = &rpc.RemoteNode{
		Id:   util.AddrToID(listener.Addr().String(), config.NodeIDSize),
		Addr: listener.Addr().String(),
	}
	r.config = config
	r.Peers = []*rpc.RemoteNode{}
	// passed in port may be 0
	// _, realPort, err := net.SplitHostPort(listener.Addr().String())
	// if err != nil {
	// 	return nil, err
	// }
	// r.port, err = strconv.Atoi(realPort)
	// if err != nil {
	// 	return nil, err
	// }

	// Initialize network policy
	r.NetworkPolicy = rpc.NewNetworkPolicy()
	r.NetworkPolicy.PauseWorld(false)

	// Initialize leader specific state
	r.commitIndex = 0
	r.lastApplied = 0
	r.nextIndex = make(map[string]uint64)
	r.matchIndex = make(map[string]uint64)

	// Initialize RPC channels
	r.appendEntries = make(chan AppendEntriesMsg)
	r.requestVote = make(chan RequestVoteMsg)
	// r.registerClient = make(chan RegisterClientMsg)
	r.clientRequest = make(chan rpc.ClientRequestMsg)
	r.gracefulExit = make(chan bool)

	// Initialize state machine (in Puddlestore, you'll switch this with your
	// own state machine)
	r.stateMachine = stateMachine

	// Initialize stable store with Bolt store
	r.stableStore = stableStore
	r.initStableStore()

	// Initialize client request cache
	r.requestsByCacheID = make(map[string]chan rpc.ClientReply)

	// Start RPC server
	r.server = server
	rpc.RegisterRaftRPCServer(r.server, r)
	go r.server.Serve(listener)
	r.Out("Started node")

	r.State = JoinState
	if connect != nil {
		// err := connect.JoinRPC(r.Self)
		err := r.joinRPC(connect)
		if err != nil {
			return nil, err
		}
	} else {
		r.Out("Waiting to start cluster until all have joined")
		go r.startCluster()
	}

	return r, nil
}

// stateFunction is a function defined on a Raft node, that while executing,
// handles the logic of the current state. When the time comes to transition to
// another state, the function returns the next function to execute.
type stateFunction func() stateFunction

func (r *Node) run() {
	var curr stateFunction = r.doFollower
	for curr != nil {
		curr = curr()
	}
}

// startCluster puts the current Raft node on hold until the required number of
// Peers join the cluster. Once they do, it starts the Peers via a StartNodeRPC
// call, and then starts the current node in the follower state.
func (r *Node) startCluster() {
	r.nodeMutex.Lock()
	r.Peers = append(r.Peers, r.Self)
	r.nodeMutex.Unlock()

	// Wait for all nodes to join cluster...
	for len(r.Peers) < r.config.ClusterSize {
		time.Sleep(time.Millisecond * 100)
	}

	// Start other nodes
	for _, node := range r.Peers {
		if r.Self.Id != node.Id {
			r.Out("Starting node-%v", node.Id)
			// err := node.StartNodeRPC(r.Self, r.Peers)
			err := r.startNodeRPC(node, r.Peers)
			if err != nil {
				r.Error("Unable to start node: %v", err)
			}
		}
	}

	// Start the current Raft node, initially in follower state
	go r.run()
}

// Join adds the fromNode to the current Raft cluster.
func (r *Node) Join(fromNode *rpc.RemoteNode) error {
	r.nodeMutex.Lock()
	defer r.nodeMutex.Unlock()

	if len(r.Peers) == r.config.ClusterSize {
		for _, node := range r.Peers {
			if node.Id == fromNode.Id {
				// node.StartNodeRPC(r.Self, r.Peers)
				r.startNodeRPC(node, r.Peers)
				return nil
			}
		}

		r.Error("Warning! Unrecognized node tried to join after all other nodes have joined.")
		return fmt.Errorf("all nodes have already joined this Raft cluster")
	}

	r.Peers = append(r.Peers, fromNode)
	return nil
}

// StartNode is invoked on us by a remote node, and starts the current node in follower state.
func (r *Node) StartNode(req *rpc.StartNodeRequest) error {
	r.nodeMutex.Lock()
	defer r.nodeMutex.Unlock()

	r.Peers = req.NodeList
	r.Out(r.FormatNodeListIds("StartNode"))

	// Start the current Raft node, initially in follower state
	go r.run()

	return nil
}

// AppendEntriesMsg is used for notifying candidates of a new leader and transfering logs
type AppendEntriesMsg struct {
	request *rpc.AppendEntriesRequest
	reply   chan rpc.AppendEntriesReply
}

// AppendEntries is invoked on us by a remote node, and sends the request and a
// reply channel to the stateFunction.
func (r *Node) AppendEntries(req *rpc.AppendEntriesRequest) rpc.AppendEntriesReply {
	// r.Debug("AppendEntries request received")
	reply := make(chan rpc.AppendEntriesReply)
	r.appendEntries <- AppendEntriesMsg{req, reply}
	return <-reply
}

// RequestVoteMsg is used for raft elections
type RequestVoteMsg struct {
	request *rpc.RequestVoteRequest
	reply   chan rpc.RequestVoteReply
}

// RequestVote is invoked on us by a remote node, and sends the request and a
// reply channel to the stateFunction.
func (r *Node) RequestVote(req *rpc.RequestVoteRequest) rpc.RequestVoteReply {
	r.Debug("RequestVote request received")
	reply := make(chan rpc.RequestVoteReply)
	r.requestVote <- RequestVoteMsg{req, reply}
	return <-reply
}

// TODO: move to shard master
// RegisterClientMsg is sent from a client to raft leader to register itself. Mainly used for caching purposes
// type RegisterClientMsg struct {
// 	request *RegisterClientRequest
// 	reply   chan RegisterClientReply
// }

// // RegisterClient is invoked on us by a client, and sends the request and a
// // reply channel to the stateFunction. If the cluster hasn't started yet, it
// // returns the corresponding RegisterClientReply.
// func (r *Node) RegisterClient(req *RegisterClientRequest) RegisterClientReply {
// 	r.Debug("RegisterClientRequest received")
// 	reply := make(chan RegisterClientReply)

// 	// If cluster hasn't started yet, return
// 	if r.State == JoinState {
// 		return RegisterClientReply{
// 			Status:     ClientStatus_CLUSTER_NOT_STARTED,
// 			ClientId:   0,
// 			LeaderHint: nil,
// 		}
// 	}

// 	// Send request down channel to be processed by current stateFunction
// 	r.registerClient <- RegisterClientMsg{req, reply}
// 	return <-reply
// }

// ClientRequest is invoked on us by a client, and sends the request and a
// reply channel to the stateFunction. If the cluster hasn't started yet, it
// returns the corresponding rpc.ClientReply.
func (r *Node) ClientRequest(reqMsg *rpc.ClientRequestMsg) rpc.ClientReply {
	r.Debug("ClientRequest request received")

	// If cluster hasn't started yet, return
	if r.State == JoinState {
		return rpc.ClientReply{
			Status:     rpc.ClientStatus_CLUSTER_NOT_STARTED,
			Response:   nil,
			LeaderHint: nil,
		}
	}

	reply := make(chan rpc.ClientReply)
	cr, exists := r.GetCachedReply(*reqMsg.Request)

	if exists {
		// If the request has been cached, reply with existing response
		return *cr
	}

	// Else, send request down channel to be processed by current stateFunction
	// r.clientRequest <- rpc.ClientRequestMsg{req, reply}
	r.clientRequest <- *reqMsg
	return <-reply
}

// Exit abruptly shuts down the current node's process, including the GRPC server.
func (r *Node) Exit() {
	r.Out("Abruptly shutting down node!")
	os.Exit(0)
}

// GracefulExit sends a signal down the gracefulExit channel, in order to enable
// a safe exit from the cluster, handled by the current stateFunction.
func (r *Node) GracefulExit() {
	r.NetworkPolicy.PauseWorld(true)
	r.Out("Gracefully shutting down node!")

	if !(r.State == ExitState || r.State == JoinState) {
		r.gracefulExit <- true
	}

	r.State = ExitState
	r.stableStore.Close()
	r.server.GracefulStop()
}
