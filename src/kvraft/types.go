package kvraft

import (
	"errors"
	"sync"

	"6.824/labrpc"
	"6.824/raft"
)

var (
	ErrNoKey 		= errors.New("No Key")
	ErrWrongLeader 	= errors.New("Wrong Leader")
)

type Op struct {
	Op,Key,Val string
}

type KVServer struct {
	mu      		sync.Mutex
	me      		int
	rf      		*raft.Raft
	applyCh 		chan raft.ApplyMsg
	applyCond		*sync.Cond
	dead    		int32
	maxraftstate 	int
	store 			KeyValueStore
	committed		map[int]int	// index -> term
	closeCh 		chan struct{}
}

type Clerk struct {
	servers []*labrpc.ClientEnd
}

type RPCPutAppendArgs struct {
	Key   string
	Value string
	Op    string
}

type RPCPutAppendReply struct {
	Err int
}

type RPCGetArgs struct {
	Key string
}

type RPCGetReply struct {
	Err   int
	Value string
}