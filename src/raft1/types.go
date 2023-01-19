package raft1

import (
	"sync"

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
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int
	dead      int32
}

type RequestVoteArgs struct {}

type RequestVoteReply struct {}

type AppendEntriesArgs struct {}

type AppendEntriesReply struct {}

type InstallSnapshotArgs struct {}

type InstallSnapshotReply struct {}