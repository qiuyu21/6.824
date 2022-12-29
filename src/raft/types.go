package raft

import (
	"sync"
	"time"

	"6.824/labrpc"
)

type ServerState uint8

type Raft struct {
	mu        	sync.Mutex
	peers     	[]*labrpc.ClientEnd // RPC end points of all peers
	persister 	*Persister          // Object to hold this peer's persisted state
	me        	int                 // this peer's index into peers[]
	dead      	int32               // set by Kill()
	state 		ServerState			// leader?follower?candidate?
	term 		int 				// current term
	vote 		int 				// the peer voted for in this term
	leader		int					// leader id
	lastHB		time.Time			// latest heartbeat
	timeout 	time.Duration		// election timeout for this term
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type RPCRequestVoteArgs struct {
	Term, CandidateId int
}

type RPCRequestVoteReply struct {
	Term int
	VoteGranted bool
}

type RPCAppendEntriesArgs struct {
	Term, LeaderId int
}

type RPCAppendEntriesReply struct {
	Term int
}

type RPCInstallSnapshotArgs struct {}

type RPCInstallSnapshotReply struct {}