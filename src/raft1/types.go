package raft1

import (
	"sync"
	"time"

	"6.824/labrpc"
)

type ApplyMsg struct {
    CommandValid bool
    Command      interface{}
    CommandIndex int

    SnapshotValid bool
    Snapshot      []byte
    SnapshotTerm  int
    SnapshotIndex int
}

type Raft struct {
    mu        			sync.Mutex
    applyCh 			chan ApplyMsg
    peers     			[]*labrpc.ClientEnd
    persister 			*Persister
    me        			int
    dead      			int32

    isLeader 			bool
    term 				int
    vote	 			int
    lastHB				time.Time
    timeout 			time.Duration

    snapshot 			[]byte
    snapshotLastIndex	int
    snapshotLastTerm 	int

    notifyCond			*sync.Cond
    commitCond 			*sync.Cond

    logManager          LogManager
    nextIndex 			[]int
    matchIndex 			[]int
    commitIndex 		int
    nextCommitIndex 	int
}

type LogEntry struct {
    Term int
    Command interface{}
}

type RPCRequestVoteArgs struct {
    CandidateId, Term, LastLogTerm, LastLogIndex int
}

type RPCRequestVoteReply struct {
    Term int
    VoteGranted bool
}

type RPCAppendEntriesArgs struct {
    Term, LeaderId, PrevLogTerm, PrevLogIndex, LeaderCommitIndex int
    Entries []*LogEntry
}

type RPCAppendEntriesReply struct {
    Term, Index int
    Success bool
}

type RPCInstallSnapshotArgs struct {}

type RPCInstallSnapshotReply struct {}