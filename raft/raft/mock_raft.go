package raft

import (
	"crypto/sha1"
	"fmt"
	"math/big"
	"math/rand"
	"net"
	"os"
	"os/user"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ziyaoh/some-kvstore/raft/statemachines"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// AddrToId converts a network address to a Raft node ID of specified length.
func AddrToId(addr string, length int) string {
	h := sha1.New()
	h.Write([]byte(addr))
	v := h.Sum(nil)
	keyInt := big.Int{}
	keyInt.SetBytes(v[:length])
	return keyInt.String()
}

var MockError error = fmt.Errorf("Error by Mock Raft")

const WIN_EADDRINUSE = syscall.Errno(10048)

// Default implementation. Does nothing except return true.
func DefaultJoinCaller(ctx context.Context, r *RemoteNode) (*Ok, error) {
	return &Ok{Ok: true}, nil
}

// Default implementation. Does nothing except return true.
func DefaultStartNodeCaller(ctx context.Context, req *StartNodeRequest) (*Ok, error) {
	return &Ok{Ok: true}, nil
}

// Default implementation. Does nothing except return true and echo the term.
func DefaultAppendEntriesCaller(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesReply, error) {
	reply := &AppendEntriesReply{}
	reply.Term = req.Term
	reply.Success = true
	return reply, nil
}

// Default implementation. Echoes the term and always votes positively.
func DefaultRequestVoteCaller(ctx context.Context, req *RequestVoteRequest) (*RequestVoteReply, error) {
	reply := &RequestVoteReply{}
	reply.Term = req.Term
	reply.VoteGranted = true

	return reply, nil
}

// Default implementation. Returns empty, since it's not useful for testing student code.
func DefaultRegisterClientCaller(ctx context.Context, req *RegisterClientRequest) (*RegisterClientReply, error) {
	return &RegisterClientReply{}, nil
}

// Default implementation. Returns empty, since it's not useful for testing student code.
func DefaultClientRequestCaller(ctx context.Context, req *ClientRequest) (*ClientReply, error) {
	return &ClientReply{}, nil
}

// Error implementation. Always returns a MockError.
func ErrorJoinCaller(ctx context.Context, r *RemoteNode) (*Ok, error) {
	return nil, MockError
}

// Error implementation. Always returns a MockError.
func ErrorStartNodeCaller(ctx context.Context, req *StartNodeRequest) (*Ok, error) {
	return nil, MockError
}

// Error implementation. Always returns a MockError.
func ErrorAppendEntriesCaller(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesReply, error) {
	return nil, MockError
}

// Error implementation. Always returns a MockError.
func ErrorRequestVoteCaller(ctx context.Context, req *RequestVoteRequest) (*RequestVoteReply, error) {
	return nil, MockError
}

// Error implementation. Always returns a MockError.
func ErrorRegisterClientCaller(ctx context.Context, req *RegisterClientRequest) (*RegisterClientReply, error) {
	return nil, MockError
}

// Error implementation. Always returns a MockError.
func ErrorClientRequestCaller(ctx context.Context, req *ClientRequest) (*ClientReply, error) {
	return nil, MockError
}

type MockRaft struct {
	// implements RaftRPCServer
	Join           func(ctx context.Context, r *RemoteNode) (*Ok, error)
	StartNode      func(ctx context.Context, req *StartNodeRequest) (*Ok, error)
	AppendEntries  func(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesReply, error)
	RequestVote    func(ctx context.Context, req *RequestVoteRequest) (*RequestVoteReply, error)
	RegisterClient func(ctx context.Context, req *RegisterClientRequest) (*RegisterClientReply, error)
	ClientRequest  func(ctx context.Context, req *ClientRequest) (*ClientReply, error)

	RemoteSelf        *RemoteNode
	server            *grpc.Server
	StudentRaft       *Node
	studentRaftClient RaftRPCClient

	receivedReqs  []*AppendEntriesRequest
	receivedMutex sync.Mutex

	t *testing.T
}

func (m *MockRaft) JoinCaller(ctx context.Context, r *RemoteNode) (*Ok, error) {
	return m.Join(ctx, r)
}

func (m *MockRaft) StartNodeCaller(ctx context.Context, req *StartNodeRequest) (*Ok, error) {
	return m.StartNode(ctx, req)
}

func (m *MockRaft) AppendEntriesCaller(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesReply, error) {
	return m.AppendEntries(ctx, req)
}

func (m *MockRaft) RequestVoteCaller(ctx context.Context, req *RequestVoteRequest) (*RequestVoteReply, error) {
	return m.RequestVote(ctx, req)
}

func (m *MockRaft) RegisterClientCaller(ctx context.Context, req *RegisterClientRequest) (*RegisterClientReply, error) {
	return m.RegisterClient(ctx, req)
}

func (m *MockRaft) ClientRequestCaller(ctx context.Context, req *ClientRequest) (*ClientReply, error) {
	return m.ClientRequest(ctx, req)
}

// Stop the grpc server at this MockRaft
func (m *MockRaft) Stop() {
	m.server.Stop()
}

// Call JoinRPC on the student node
func (m *MockRaft) JoinCluster() (err error) {
	_, err = m.studentRaftClient.JoinCaller(context.Background(), m.RemoteSelf)
	return
}

// Create a cluster from one student raft and ClusterSize-1 mock rafts, connect the mock rafts to the student one.
func MockCluster(joinThem bool, config *Config, t *testing.T) (studentRaft *Node, mockRafts []*MockRaft, err error) {
	if config == nil {
		config = DefaultConfig()
	}

	// Use a path in /tmp so we use local disk and not NFS
	if runtime.GOOS != "windows" {
		curUser, _ := user.Current()
		config.LogPath = "/tmp/" + curUser.Username + "_raftlogs"
	}

	var stableStore StableStore
	if config.InMemory {
		stableStore = NewMemoryStore()
	} else {
		// stableStore = NewBoltStore(filepath.Join(config.LogPath, fmt.Sprintf("raft%d", rand.Int())))
		stableStore = nil
	}
	studentRaft, err = CreateNode(OpenPort(0), nil, config, new(hashmachine.HashMachine), stableStore)
	if err != nil {
		return
	}
	mockRafts = make([]*MockRaft, config.ClusterSize-1)

	for i, _ := range mockRafts {
		var mr *MockRaft
		mr, err = NewDefaultMockRaft(studentRaft)
		if err != nil {
			return
		}
		mockRafts[i] = mr
		mr.setTestingT(t)
		if joinThem {
			err = mr.JoinCluster()
			if err != nil {
				return
			}
		}
	}

	if joinThem {
		time.Sleep(110 * time.Millisecond)
	}
	return
}

func (m *MockRaft) init(config *Config) error {
	m.server = grpc.NewServer()
	RegisterRaftRPCServer(m.server, m)
	conn, port, err := OpenListener()
	if err != nil {
		return err
	}
	hostname, _ := os.Hostname()
	addr := fmt.Sprintf("%v:%v", hostname, port)
	id := AddrToId(addr, 2)
	m.RemoteSelf = &RemoteNode{Addr: addr, Id: id}

	cc, err := m.StudentRaft.Self.ClientConn()
	if err != nil {
		return err
	}

	m.studentRaftClient = cc

	go m.server.Serve(conn)
	return nil
}

func OpenListener() (net.Listener, int, error) {
	rand.Seed(time.Now().UTC().UnixNano())
	port := rand.Intn(61000-32768) + 32768
	hostname, err := os.Hostname()
	if err != nil {
		return nil, -1, err
	}

	addr := fmt.Sprintf("%v:%v", hostname, port)
	conn, err := net.Listen("tcp4", addr)
	if err != nil {
		if addrInUse(err) {
			time.Sleep(100 * time.Millisecond)
			return OpenListener()
		} else {
			return nil, -1, err
		}
	}
	return conn, port, err
}

func addrInUse(err error) bool {
	if opErr, ok := err.(*net.OpError); ok {
		if osErr, ok := opErr.Err.(*os.SyscallError); ok {
			return osErr.Err == syscall.EADDRINUSE || osErr.Err == WIN_EADDRINUSE
		}
	}
	return false
}

// Create a new mockraft with the default config
func NewDefaultMockRaft(studentRaft *Node) (m *MockRaft, err error) {
	m = &MockRaft{
		Join:           DefaultJoinCaller,
		StartNode:      DefaultStartNodeCaller,
		AppendEntries:  DefaultAppendEntriesCaller,
		RequestVote:    DefaultRequestVoteCaller,
		RegisterClient: DefaultRegisterClientCaller,
		ClientRequest:  DefaultClientRequestCaller,

		StudentRaft: studentRaft,
	}

	err = m.init(DefaultConfig())

	return
}

// set the testing.T associated with this MockRaft
func (m *MockRaft) setTestingT(t *testing.T) {
	m.t = t
}

func (m *MockRaft) validateAppendEntries(req *AppendEntriesRequest) {
	// outline
	//     type AppendEntriesRequest struct {
	//     // The leader's term
	//     Term uint64 `protobuf:"varint,1,opt,name=term" json:"term,omitempty"`
	//     // Address of the leader sending this request
	//     Leader *RemoteNode `protobuf:"bytes,2,opt,name=leader" json:"leader,omitempty"`
	//     // The index of the log entry immediately preceding the new ones
	//     PrevLogIndex uint64 `protobuf:"varint,3,opt,name=prevLogIndex" json:"prevLogIndex,omitempty"`
	//     // The term of the log entry at prevLogIndex
	//     PrevLogTerm uint64 `protobuf:"varint,4,opt,name=prevLogTerm" json:"prevLogTerm,omitempty"`
	//     // The log entries the follower needs to store. Empty for heartbeat messages.
	//     Entries []*LogEntry `protobuf:"bytes,5,rep,name=entries" json:"entries,omitempty"`
	//     // The leader's commitIndex
	//     LeaderCommit uint64 `protobuf:"varint,6,opt,name=leaderCommit" json:"leaderCommit,omitempty"`
	// }
	m.addRequest(req)

	if req == nil {
		m.t.Fatal("student sent nil AppendEntriesRequest!")
	}

	// if currTerm := m.StudentRaft.GetCurrentTerm(); req.Term != currTerm {
	// 	m.t.Fatalf("term in student request was %v, but student term is %v", req.Term, currTerm)
	// }

	if studentRemote := m.StudentRaft.Self; studentRemote.Id != req.Leader.Id {
		m.t.Fatalf("leader in student request was %v, but student is %v", req.Leader, studentRemote)
	}

	// m.StudentRaft.leaderMutex.Lock()
	// if m.StudentRaft.commitIndex != req.LeaderCommit {
	// 	m.StudentRaft.leaderMutex.Unlock()
	// 	m.t.Fatalf("commit in student request was %v, but student commitIndex is %v", req.LeaderCommit, m.StudentRaft.commitIndex)
	// }
	// m.StudentRaft.leaderMutex.Unlock()
}

func (m *MockRaft) addRequest(req *AppendEntriesRequest) {
	m.receivedMutex.Lock()
	defer m.receivedMutex.Unlock()
	m.receivedReqs = append(m.receivedReqs, req)
}

// Stops all the grpc servers, removes raftlogs, and closes client connections
func cleanupMockCluster(student *Node, mockRafts []*MockRaft) {
	student.server.Stop()
	go func(student *Node) {
		student.GracefulExit()
		student.RemoveLogs()
	}(student)

	time.Sleep(2 * time.Second)

	for _, n := range mockRafts {
		n.Stop()
		n.RemoteSelf.RemoveClientConn()
	}

	student.Self.RemoveClientConn()
}
