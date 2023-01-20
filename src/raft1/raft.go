package raft1

import (
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.term, rf.isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool { return true }

func (rf *Raft) persistState() {}

func (rf *Raft) persistStateAndSnapshot() {}

func (rf *Raft) readPersistState(data []byte) {
	if data == nil || len(data) < 1 { return }
}

func (rf *Raft) serializeState() []byte { return nil }

func (rf *Raft) Snapshot(index int, snapshot []byte) {}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	return index, term, isLeader
}

func (rf *Raft) ticker() {
	n := len(rf.peers)
	for !rf.killed() {
		rf.mu.Lock()
		if rf.isLeader {
			rf.mu.Unlock()
			for i := 0; i < n; i++ { if i != rf.me { go rf.heartbeat(i) } }
			time.Sleep(HEARTBEAT_INTERVAL)
		} else {
			if rf.lastHB.Add(rf.timeout).After(time.Now()) {
				rf.mu.Unlock()
				time.Sleep(TICK_INTERVAL)
				continue
			}
			rf.term++
			rf.vote = rf.me
			rf.persistState()
			newterm := rf.term
			rf.mu.Unlock()
			mu := sync.Mutex{}
			cd := sync.NewCond(&mu)
			re := []RPCRequestVoteReply{}
			re = append(re, RPCRequestVoteReply{VoteGranted: true})
			requestVote := func(peer int) {
				var args RPCRequestVoteArgs
				var repl RPCRequestVoteReply
				rf.mu.Lock()
				rf.setupRequestVoteArgs(&args)
				rf.mu.Unlock()
				rf.peers[peer].Call("Raft.RPCRequestVote", &args, &repl)
				mu.Lock()
				re = append(re, repl)
				cd.Signal()
				mu.Unlock()
			}
			for i := 0; i < n; i++ { if i != rf.me { go requestVote(i) } }
			granted, majority := 0, n/2 + 1
			for i := 0; granted < majority && granted + n - i >= majority; i++ {
				mu.Lock()
				for len(re) == 0 { cd.Wait() }
				if re[0].VoteGranted {
					granted++
				} else if re[0].Term > newterm {
					rf.mu.Lock()
					if re[0].Term > rf.term {
						rf.term = re[0].Term
						rf.vote = -1
						rf.persistState()
					}
					rf.mu.Unlock()
					mu.Unlock()
					break
				}
				re = re[1:]
				mu.Unlock()
			}
			rf.mu.Lock()
			if granted == majority {
				rf.isLeader = true
				rf.nextIndex = make([]int, n)
				rf.matchIndex = make([]int, n)
				for i := 0; i < n; i++ {
					rf.nextIndex[i]  = rf.lastLogIndex + 1
					rf.matchIndex[i] = 0
				}
			}
			rf.lastHB = time.Now()
			rf.timeout = RandElectionTimeout()
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) heartbeat(peer int) {
	var args RPCAppendEntriesArgs
	var repl RPCAppendEntriesReply
	rf.mu.Lock()
	if !rf.isLeader {
		rf.mu.Unlock()
		return
	}
	args.LeaderId = rf.me
	args.Term = rf.term
	rf.mu.Unlock()
	if rf.peers[peer].Call("Raft.RPCAppendEntries", &args, &repl) {
		rf.mu.Lock()
		if rf.isLeader {
			if repl.Term > args.Term {
				if repl.Term > rf.term {
					rf.isLeader = false
					rf.term = repl.Term
					rf.vote = -1
					rf.lastHB = time.Now()
					rf.persistState()
				}
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) setupRequestVoteArgs(args *RPCRequestVoteArgs) {
	args.CandidateId = rf.me
	args.Term = rf.term
	args.LastLogIndex = rf.lastLogIndex
	if args.LastLogIndex > 0 {
		if args.LastLogIndex == rf.snapshotLastIndex {
			args.LastLogTerm = rf.snapshotLastTerm
		} else {
			args.LastLogTerm = rf.logs[args.LastLogIndex].Term
		}
	}
}

func (rf *Raft) setupAppendEntriesArgs(args *RPCAppendEntriesArgs) {}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	
	rf.peers 			= peers
	rf.persister 		= persister
	rf.me 				= me
	rf.dead 			= 0

	rf.isLeader 		= false
	rf.term 			= 0
	rf.vote 			= -1

	rf.logs 			= make([]LogEntry, DEFAULT_LOG_SIZE)
	rf.firstLogIndex 	= 0
	rf.lastLogIndex 	= 0
	rf.commitIndex 		= 0
	rf.nextCommitIndex 	= 0

	rf.readPersistState(persister.ReadRaftState())

	rf.lastHB 			= time.Now()
	rf.timeout 			= RandElectionTimeout()

	go rf.ticker()
	return rf
}

func (rf *Raft) RPCRequestVote(args *RPCRequestVoteArgs, reply *RPCRequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	cmp_uptodate := func() bool {
		return true
	}
	if args.Term < rf.term || (args.Term == rf.term && rf.vote != args.CandidateId) {
		reply.Term = args.Term
		reply.VoteGranted = false
	} else {
		shouldPersist := false
		if args.Term > rf.term {
			rf.isLeader = false
			rf.term = args.Term
			rf.vote = -1
			shouldPersist = true
		}
		if !cmp_uptodate() {
			reply.VoteGranted = false
		} else {
			rf.vote = args.CandidateId
			reply.VoteGranted = true
			shouldPersist = true
		}
		if shouldPersist { rf.persistState() }
	}
}

func (rf *Raft) RPCAppendEntries(args *RPCAppendEntriesArgs, reply *RPCAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.term {
		reply.Term = rf.term
	} else {
		shouldPersist := false
		if args.Term > rf.term {
			rf.term = args.Term
			rf.vote = args.LeaderId
			shouldPersist = true
		}
		rf.isLeader = false
		rf.lastHB = time.Now()
		reply.Term = args.Term
		reply.Success = true
		if shouldPersist { rf.persistState() }
	}
}

func (rf *Raft) RPCInstallSnapshot(args *RPCInstallSnapshotArgs, reply *RPCInstallSnapshotReply) {}