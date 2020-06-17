package util

import (
	"io/ioutil"
	"log"
	"os"

	"google.golang.org/grpc/grpclog"
)

// Debug is medium-risk logger
var Debug *log.Logger

// Out is low-risk logger
var Out *log.Logger

// Error is high-risk logger
var Error *log.Logger

// Initialize the loggers
func init() {
	Debug = log.New(ioutil.Discard, "", log.Ltime|log.Lshortfile)
	Out = log.New(os.Stdout, "DEBUG: ", log.Ltime|log.Lshortfile)
	Error = log.New(os.Stderr, "ERROR: ", log.Ltime|log.Lshortfile)

	grpclog.SetLogger(log.New(ioutil.Discard, "", 0))
}

// SetDebug turns printing debug strings on or off
func SetDebug(enabled bool) {
	if enabled {
		Debug.SetOutput(os.Stdout)
	} else {
		Debug.SetOutput(ioutil.Discard)
	}
}

// SuppressLoggers turns off all logging
func SuppressLoggers() {
	Out.SetOutput(ioutil.Discard)
	Error.SetOutput(ioutil.Discard)
	Debug.SetOutput(ioutil.Discard)
	grpclog.SetLogger(Out)
}
