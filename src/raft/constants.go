package raft

import "time"

const (
	LEADER ServerState = iota
	FOLLOWER
	CANDIDATE
)

const (
	TICK_INTERVAL 		= 20 * time.Millisecond		// frequency followers check latest heartbeat
	HEARTBEAT_INTERVAL 	= 100 * time.Millisecond	// frequency leader sends out heartbeats
	ELECTION_LB_TIMEOUT = 200 * time.Millisecond	// lower bound election timeout
	ELECTION_UB_TIMEOUT = 350 * time.Millisecond	// upper bound election timeout
)