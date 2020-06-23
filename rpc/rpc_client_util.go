package rpc

import (
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// RPCTimeout is default timeout for rpc client calls
const RPCTimeout = 2 * time.Second

var dialOptions []grpc.DialOption

func init() {
	dialOptions = []grpc.DialOption{grpc.WithInsecure(), grpc.FailOnNonTempDialError(true), grpc.WithTimeout(2 * time.Second), grpc.WithUnaryInterceptor(clientUnaryInterceptor)}
}

// clientUnaryInterceptor is a client unary interceptor that injects a default timeout
func clientUnaryInterceptor(
	ctx context.Context,
	method string,
	req, reply interface{},
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	ctx, cancel := context.WithTimeout(ctx, RPCTimeout)
	defer cancel()

	return invoker(ctx, method, req, reply, cc, opts...)
}

// RPC Invocation functions

var connMap = make(map[string]*grpc.ClientConn)
var connMapLock = &sync.RWMutex{}

// makeClientConn creates a new client connection to the given remote node
func makeClientConn(remote *RemoteNode) (*grpc.ClientConn, error) {
	return grpc.Dial(remote.Addr, dialOptions...)
}

// RemoveClientConn removes the client connection to the given node, if present
func (remote *RemoteNode) RemoveClientConn() {
	connMapLock.Lock()
	defer connMapLock.Unlock()
	if cc, ok := connMap[remote.Addr]; ok {
		cc.Close()
		delete(connMap, remote.Addr)
	}
}

// connCheck checks the given error and removes the client connection if it's not nil
func (remote *RemoteNode) connCheck(err error) error {
	if err != nil {
		time.Sleep(RPCTimeout)
		remote.RemoveClientConn()
	}
	return err
}
