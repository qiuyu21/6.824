package raft

import (
	"sync"
	"time"

	"6.824/labrpc"
)

type ServerState uint8

type Raft struct {
	mu        			sync.Mutex
	applyCh				chan ApplyMsg
	peers     			[]*labrpc.ClientEnd // RPC end points of all peers
	persister 			*Persister          // Object to hold this peer's persisted state
	me        			int                 // this peer's index into peers[]
	dead      			int32               // set by Kill()
	isLeader 			bool
	term 				int 				// current term
	vote 				int 				// the peer voted for in this term
	lastHB				time.Time			// latest heartbeat
	timeout 			time.Duration		// election timeout for this term

	logs 				map[int]*LogEntry	
	commitIndex 		int
	lastLogIndex		int

	nextIndex 			[]int				// index of the next log entry to send
	matchIndex			[]int				// index of highest log entry known to be replicated on server

	snapshot 			[]byte
	snapshotLastIndex 	int
	snapshotLastTerm 	int
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

type LogEntry struct {
	Index, Term int
	Command interface{}
}

type RPCRequestVoteArgs struct {
	Term, CandidateId, LastLogIndex, LastLogTerm int
}

type RPCRequestVoteReply struct {
	Term int
	VoteGranted bool
}

type RPCAppendEntriesArgs struct {
	Term, LeaderId, PrevLogIndex, PrevLogTerm, LeaderCommitIndex int
	Entries []LogEntry
}

type RPCAppendEntriesReply struct {
	Term, Index int
	Success bool
}

type RPCInstallSnapshotArgs struct {
	Term, LeaderId, LastIncludedIndex, LastIncludedTerm int
	Data []byte
}

type RPCInstallSnapshotReply struct {
	Term int
}