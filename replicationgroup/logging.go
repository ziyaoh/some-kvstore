package replicationgroup

import (
	"github.com/fatih/color"
)

// Out prints to standard output, prefaced with time and filename
func (r *Node) Out(formatString string, args ...interface{}) {
	r.raft.Out(formatString, args...)
}

// Debug prints to standard output if SetDebug was called with enabled=true, prefaced with time and filename
func (r *Node) Debug(formatString string, args ...interface{}) {
	r.raft.Debug(formatString, args...)
}

// Error prints to standard error, prefaced with "ERROR: ", time, and filename
func (r *Node) Error(formatString string, args ...interface{}) {
	color.Set(color.FgRed)
	r.raft.Error(formatString, args...)
	color.Unset()
}
