package raft

import (
	"math/rand"
	"time"
)

const (
	TICK_INTERVAL 		= 10 * time.Millisecond		// frequency followers check latest heartbeat
	HEARTBEAT_INTERVAL 	= 100 * time.Millisecond	// frequency leader sends out heartbeats
	ELECTION_LB_TIMEOUT	= 400 * time.Millisecond	// lower bound election timeout
	ELECTION_UB_TIMEOUT	= 800 * time.Millisecond	// upper bound election timeout
)

func min(args ...int) int {
	ret := args[0]
	for i := 1; i < len(args); i++ { if args[i] < ret { ret = args[i] } }
	return ret
}

func RandElectionTimeout() time.Duration {
	return ELECTION_LB_TIMEOUT + time.Duration(rand.Int63n(int64(ELECTION_UB_TIMEOUT - ELECTION_LB_TIMEOUT + 1)))
}