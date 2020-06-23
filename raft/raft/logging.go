package raft

import (
	"bytes"
	"fmt"

	"github.com/fatih/color"
	"github.com/ziyaoh/some-kvstore/util"
)

// Out prints to standard output, prefaced with time and filename
func (r *Node) Out(formatString string, args ...interface{}) {
	util.Out.Output(2, fmt.Sprintf("(%v/%v) %v\n", r.Self, r.State, fmt.Sprintf(formatString, args...)))
}

// Debug prints to standard output if SetDebug was called with enabled=true, prefaced with time and filename
func (r *Node) Debug(formatString string, args ...interface{}) {
	util.Debug.Output(2, fmt.Sprintf("(%v/%v) %v\n", r.Self, r.State, fmt.Sprintf(formatString, args...)))
}

// Error prints to standard error, prefaced with "ERROR: ", time, and filename
func (r *Node) Error(formatString string, args ...interface{}) {
	color.Set(color.FgRed)
	util.Error.Output(2, fmt.Sprintf("(%v/%v) %v\n", r.Self, r.State, fmt.Sprintf(formatString, args...)))
	color.Unset()
}

func (s NodeState) String() string {
	switch s {
	case FollowerState:
		return "follower"
	case CandidateState:
		return "candidate"
	case LeaderState:
		return "leader"
	case JoinState:
		return "joining"
	default:
		return "unknown"
	}
}

func (r *Node) String() string {
	return fmt.Sprintf("Node{Self: %v, State: %v}", r.Self, r.State)
}

// FormatState returns a string representation of the Raft node's state
func (r *Node) FormatState() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("Current node (%v): state:\n", r.Self))

	for i, node := range r.Peers {
		buffer.WriteString(fmt.Sprintf("%v - %v", i, node))
		local := r.Self

		if local.Addr == node.Addr {
			buffer.WriteString(" (local node)")
		}

		if r.Leader != nil && r.Leader.Addr == node.Addr {
			buffer.WriteString(" (leader node)")
		}
		buffer.WriteString("\n")
	}

	buffer.WriteString(fmt.Sprintf("Current term: %v\n", r.GetCurrentTerm()))
	buffer.WriteString(fmt.Sprintf("Current state: %v\n", r.State))
	buffer.WriteString(fmt.Sprintf("Current commit index: %v\n", r.commitIndex))
	buffer.WriteString(fmt.Sprintf("Current next index: %v\n", r.nextIndex))
	buffer.WriteString(fmt.Sprintf("Current match index: %v\n", r.matchIndex))

	return buffer.String()
}

// FormatLogCache returns a string representation of the Raft node's log cache
func (r *Node) FormatLogCache() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("Node %v LogCache:\n", r.Self))

	for i := uint64(0); i <= r.LastLogIndex(); i++ {
		log := r.GetLog(i)
		if log != nil {
			buffer.WriteString(fmt.Sprintf(" idx:%v, term:%v\n", log.Index, log.TermId))
		}
	}

	return buffer.String()
}

// FormatNodeListIds returns a string representation of IDs the list of nodes in the cluster
func (r *Node) FormatNodeListIds(ctx string) string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("%v (%v) r.NodeList = [", ctx, r.Self))

	nodeList := r.Peers
	for i, node := range nodeList {
		buffer.WriteString(fmt.Sprintf("%v", node.Id))
		if i < len(nodeList)-1 {
			buffer.WriteString(",")
		}
	}

	buffer.WriteString("]\n")
	return buffer.String()
}
