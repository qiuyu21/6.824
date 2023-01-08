package raft

import (
	"bytes"
	"log"
	"sort"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.term, rf.isLeader
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool { return true }

func (rf *Raft) killed() bool { return atomic.LoadInt32(&rf.dead) == 1 }

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if !rf.isLeader { return -1, -1, false }
	rf.lastLogIndex++
	rf.logs[rf.lastLogIndex] = &LogEntry{
		Index: rf.lastLogIndex,
		Term: rf.term,
		Command: command,
	}
	rf.persistState()
	rf.matchIndex[rf.me] = rf.lastLogIndex
	
	return rf.lastLogIndex, rf.term, true
}

func (rf *Raft) ticker() {
	n := len(rf.peers)
	for !rf.killed() {
		rf.mu.Lock()
		if rf.isLeader {
			rf.mu.Unlock()
			f := func(peer int) {
				var args RPCAppendEntriesArgs
				var repl RPCAppendEntriesReply
				if !rf.setupargs(&args, peer) { return }
				if rf.peers[peer].Call("Raft.RPCAppendEntries", &args, &repl) {
					rf.mu.Lock()
					if rf.isLeader {
						if repl.Term > args.Term {
							if repl.Term > rf.term {
								rf.isLeader = false
								rf.term = repl.Term
								rf.vote = -1
								rf.persistState()
							}
						} else if !repl.Success {
							rf.nextIndex[peer] = repl.Index
						} else if m := len(args.Entries); m > 0 {
							i := args.Entries[m-1].Index
							if i + 1 > rf.nextIndex[peer] { 
								rf.nextIndex[peer] = i + 1
								rf.matchIndex[peer] = i
							}
							sorted := make([]int, len(rf.matchIndex))
							copy(sorted, rf.matchIndex)
							sort.Ints(sorted)
							j := sorted[n-n/2-1]
							if j > rf.commitIndex && rf.logs[j].Term == rf.term {
								k := rf.commitIndex + 1
								rf.commitIndex = j
								rf.mu.Unlock()
								for ; k <= j; k++ {
									rf.mu.Lock()
									command := rf.logs[k].Command
									rf.mu.Unlock()
									rf.applyCh <- ApplyMsg{
										CommandValid: true,
										CommandIndex: k,
										Command: command,
									}
								}
								return
							}
						}
					}
					rf.mu.Unlock()
				}
			}
			for i := 0; i < n; i++ { if i != rf.me { go f(i) } }
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
			c := make(chan RPCRequestVoteReply, n)
			c <- RPCRequestVoteReply{VoteGranted: true}
			f := func(peer int) {
				rf.mu.Lock()
				var args RPCRequestVoteArgs
				var repl RPCRequestVoteReply
				args.CandidateId = rf.me
				args.Term = newterm
				args.LastLogIndex = rf.lastLogIndex
				if args.LastLogIndex > 0 {
					if args.LastLogIndex == rf.snapshotLastIndex {
						args.LastLogTerm = rf.snapshotLastTerm
					} else {
						args.LastLogTerm = rf.logs[rf.lastLogIndex].Term
					}
				}
				rf.mu.Unlock()
				rf.peers[peer].Call("Raft.RPCRequestVote", &args, &repl)
				c <- repl
			}
			for i := 0; i < n; i++ { if i != rf.me { go f(i) } }
			granted := 0
			majority := n / 2 + 1
			for i := 0; granted < majority && granted + n - i >= majority; i++ {
				res := <- c
				if res.VoteGranted {
					granted++
				} else if res.Term > newterm {
					rf.mu.Lock()
					if res.Term > rf.term {
						rf.term = res.Term
						rf.vote = -1
						rf.persistState()
					}
					rf.mu.Unlock()
					break
				}
			}
			rf.mu.Lock()
			if granted == majority && rf.term == newterm {
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

func (rf *Raft) setupargs(args *RPCAppendEntriesArgs, peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.isLeader { return false }
	args.LeaderId = rf.me
	args.Term = rf.term
	args.LeaderCommitIndex = min(rf.commitIndex, rf.matchIndex[peer]);
	if rf.snapshotLastIndex > 0 && rf.nextIndex[peer] <= rf.snapshotLastIndex {
		go rf.sendSnapshot(peer)
		return false
	} else {
		if rf.lastLogIndex >= rf.nextIndex[peer] {
			for i := rf.nextIndex[peer]; i <= rf.lastLogIndex; i++ {
				args.Entries = append(args.Entries, LogEntry{
					Index: i,
					Term: rf.logs[i].Term,
					Command: rf.logs[i].Command,
				})
			}
			args.PrevLogIndex = rf.nextIndex[peer] - 1
		} else {
			args.PrevLogIndex = rf.lastLogIndex
		}
		if args.PrevLogIndex > 0 {
			if args.PrevLogIndex == rf.snapshotLastIndex {
				args.PrevLogTerm = rf.snapshotLastTerm
			} else {
				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
			}
		}
		return true
	}
}

func (rf *Raft) sendSnapshot(peer int) {
	rf.mu.Lock()
	if !rf.isLeader {
		rf.mu.Unlock()
	} else {
		var args RPCInstallSnapshotArgs
		var repl RPCInstallSnapshotReply
		args.LeaderId = rf.me
		args.Term = rf.term
		args.LastIncludedIndex = rf.snapshotLastIndex
		args.LastIncludedTerm = rf.snapshotLastTerm
		args.Data = make([]byte, len(rf.snapshot))
		copy(args.Data, rf.snapshot)
		rf.mu.Unlock()
		if rf.peers[peer].Call("Raft.RPCInstallSnapshot", &args, &repl) {
			rf.mu.Lock()
			if rf.isLeader {
				if repl.Term > args.Term {
					if repl.Term > rf.term {
						rf.isLeader = false
						rf.term = repl.Term
						rf.vote = -1
						rf.persistState()
					}
				} else if args.LastIncludedIndex + 1 > rf.nextIndex[peer] { 
					rf.nextIndex[peer] = args.LastIncludedIndex + 1
					rf.matchIndex[peer] = args.LastIncludedIndex
				}
			}
			rf.mu.Unlock()
		} 
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.applyCh = applyCh
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.isLeader = false
	rf.term = 0
	rf.vote = -1
	rf.logs = make(map[int]*LogEntry)
	rf.commitIndex = 0
	rf.lastLogIndex = 0
	rf.readPersist(rf.persister.ReadRaftState())
	rf.snapshot = rf.persister.ReadSnapshot()
	rf.lastHB = time.Now()
	rf.timeout = RandElectionTimeout()
	go rf.ticker()
	return rf
}

func (rf *Raft) persistState() {
	rf.persister.SaveRaftState(rf.serializeState())
}

func (rf *Raft) persistStateAndSnapshot() {
	rf.persister.SaveStateAndSnapshot(rf.serializeState(), rf.snapshot)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { return }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.term) != nil ||
		d.Decode(&rf.vote) != nil ||
		d.Decode(&rf.logs) != nil ||
		d.Decode(&rf.snapshotLastIndex) != nil ||
		d.Decode(&rf.snapshotLastTerm) != nil {
			log.Fatalln("Decode has failed")
		}
	if len(rf.logs) > 0 {
		for index := range rf.logs { if index > rf.lastLogIndex { rf.lastLogIndex = index } }
	} else {
		rf.lastLogIndex = rf.snapshotLastIndex
	}
	rf.commitIndex = rf.snapshotLastIndex
}

func (rf *Raft) serializeState() []byte {
	w := &bytes.Buffer{}
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.vote)
	e.Encode(rf.logs)
	e.Encode(rf.snapshotLastIndex)
	e.Encode(rf.snapshotLastTerm)
	return w.Bytes()
}

func (rf *Raft) RPCRequestVote(args *RPCRequestVoteArgs, reply *RPCRequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	cmp_uptodate := func() bool {
		if rf.lastLogIndex > 0 {
			if len(rf.logs) == 0 {
				if args.LastLogTerm < rf.snapshotLastTerm { 
					return false
				} else if args.LastLogTerm == rf.snapshotLastTerm && args.LastLogIndex < rf.lastLogIndex {
					return false
				}
			} else if args.LastLogTerm < rf.logs[rf.lastLogIndex].Term {
				return false
			} else if args.LastLogTerm == rf.logs[rf.lastLogIndex].Term && args.LastLogIndex < rf.lastLogIndex {
				return false
			}
		}
		return true
	}
	if args.Term < rf.term || (args.Term == rf.term && rf.vote != args.CandidateId) {
		reply.Term = rf.term
		reply.VoteGranted = false
	} else {
		shouldPersist := false
		if args.Term > rf.term {
			rf.term = args.Term
			rf.vote = -1
			rf.isLeader = false
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
	if args.Term < rf.term {
		reply.Term = rf.term
		rf.mu.Unlock()
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
		if args.PrevLogIndex > rf.lastLogIndex {
			reply.Index = rf.lastLogIndex + 1
			reply.Success = false
			if shouldPersist { rf.persistState() }
			rf.mu.Unlock()
			return
		} else if args.PrevLogIndex > 0 {
			if args.PrevLogIndex == rf.snapshotLastIndex {
				if args.PrevLogTerm != rf.snapshotLastTerm { log.Fatalln("This should not happen!") }
			} else if args.PrevLogIndex > rf.snapshotLastIndex {
				if t := rf.logs[args.PrevLogIndex].Term; t != args.PrevLogTerm {
					reply.Index = args.PrevLogIndex
					for reply.Index > 1 && reply.Index-1 > rf.snapshotLastIndex && rf.logs[reply.Index-1].Term == t { reply.Index-- }
					reply.Success = false
					if shouldPersist { rf.persistState() }
					rf.mu.Unlock()
					return
				}
			}
		}
		if len(args.Entries) > 0 {
			for i, l_log := range args.Entries {
				if l_log.Index <= rf.snapshotLastIndex { continue }
				if f_log, ok := rf.logs[l_log.Index]; ok {
					if l_log.Term == f_log.Term { continue }
					for j := l_log.Index; j <= rf.lastLogIndex; j++ { delete(rf.logs, j) }
				}
				for j := i; j < len(args.Entries); j++ {
					rf.logs[args.Entries[j].Index] = &LogEntry{
						Index: args.Entries[j].Index,
						Term: args.Entries[j].Term,
						Command: args.Entries[j].Command,
					}
				}
				rf.lastLogIndex = args.Entries[len(args.Entries)-1].Index
				shouldPersist = true
				break
			}
		}
		if shouldPersist { rf.persistState() }
		if args.LeaderCommitIndex > rf.commitIndex {
			i := min(args.LeaderCommitIndex, rf.lastLogIndex)
			j := rf.commitIndex + 1
			rf.commitIndex = i
			rf.mu.Unlock()
			for ; j <= i; j++ {
				rf.mu.Lock()
				command := rf.logs[j].Command
				rf.mu.Unlock()
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					CommandIndex: j,
					Command: command,
				}
			}
			return
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) RPCInstallSnapshot(args *RPCInstallSnapshotArgs, reply *RPCInstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.term {
		reply.Term = rf.term
		return
	} else {
		if args.Term > rf.term {
			rf.term = args.Term
			rf.vote = args.LeaderId
		}
		rf.isLeader = false
		rf.lastHB = time.Now()
		reply.Term = args.Term
		rf.snapshot = args.Data
		rf.snapshotLastIndex = args.LastIncludedIndex
		rf.snapshotLastTerm = args.LastIncludedTerm
		if log, ok := rf.logs[args.LastIncludedIndex]; ok && log.Term == args.LastIncludedTerm {
			for i := args.LastIncludedIndex; i > 0; i-- {
				if _, ok := rf.logs[i]; !ok { break }
				delete(rf.logs, i)
			}
		} else {
			rf.lastLogIndex = rf.snapshotLastIndex
			rf.logs = make(map[int]*LogEntry)
		}
		if rf.snapshotLastIndex > rf.commitIndex { rf.commitIndex = rf.snapshotLastIndex }
		rf.persistStateAndSnapshot()
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot: rf.snapshot,
			SnapshotIndex: rf.snapshotLastIndex,
			SnapshotTerm: rf.snapshotLastTerm,
		}
	}
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.snapshotLastIndex { log.Fatalln("Snapshot: This should not happen") }
	rf.snapshot = snapshot
	rf.snapshotLastIndex = index
	rf.snapshotLastTerm = rf.logs[index].Term
	for i := index; i > 0; i-- {
		if _, ok := rf.logs[i]; !ok { break }
		delete(rf.logs, i)
	}
	rf.persistStateAndSnapshot()
}