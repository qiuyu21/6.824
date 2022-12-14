package raft

import "time"

const (
	LEADER ServerState = iota
	FOLLOWER
	CANDIDATE
)

const (
	TICK_INTERVAL 		= 10 * time.Millisecond		// frequency followers check latest heartbeat
	HEARTBEAT_INTERVAL 	= 100 * time.Millisecond	// frequency leader sends out heartbeats
	ELECTION_LB_TIMEOUT	= 150 * time.Millisecond	// lower bound election timeout
	ELECTION_UB_TIMEOUT	= 300 * time.Millisecond	// upper bound election timeout
)

func min(args ...int) int {
	ret := args[0]
	for i := 1; i < len(args); i++ { if args[i] < ret { ret = args[i] } }
	return ret
}