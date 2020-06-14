package rpc

// ClientRequestMsg is sent from a client to raft leader to make changes to the state machine
type ClientRequestMsg struct {
	Request *ClientRequest
	Reply   chan ClientReply
}
