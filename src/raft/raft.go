package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"log"
	"math/rand"
	"sort"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.term, rf.state == LEADER
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if rf.state != LEADER { return -1, -1, false }
	rf.lastLogIndex++
	rf.logs[rf.lastLogIndex] = &LogEntry{
		Index: rf.lastLogIndex,
		Term: rf.term,
		Command: command,
	}
	rf.persist()
	rf.matchIndex[rf.me] = rf.lastLogIndex
	return rf.lastLogIndex, rf.term, true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received heartsbeats recently.
func (rf *Raft) ticker() {
	n := len(rf.peers)
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == LEADER {
			rf.mu.Unlock()
			f := func(peer int) {
				var args RPCAppendEntriesArgs
				var repl RPCAppendEntriesReply
				if !rf.setupargs(&args, peer) { return }
				if rf.peers[peer].Call("Raft.RPCAppendEntries", &args, &repl) {
					rf.mu.Lock()
					if rf.state == LEADER {
						if repl.Term > args.Term {
							rf.state = FOLLOWER
							if repl.Term > rf.term {
								rf.term = repl.Term
								rf.vote = -1
								rf.persist()
							}
						} else if !repl.Success {
							rf.nextIndex[peer] = repl.Index
						} else if len(args.Entries) > 0 {
							i := args.Entries[len(args.Entries)-1].Index
							rf.nextIndex[peer] = i + 1
							rf.matchIndex[peer] = i
							// Update commit index
							sorted := make([]int, len(rf.matchIndex))
							copy(sorted, rf.matchIndex)
							sort.Ints(sorted)
							j := sorted[n-n/2-1]
							if j > rf.commitIndex && rf.logs[j].Term == rf.term {
								for k := rf.commitIndex + 1; k <= j; k++ {
									rf.applyCh <- ApplyMsg{
										CommandValid: true,
										CommandIndex: k,
										Command: rf.logs[k].Command,
									}
								}
								rf.commitIndex = j
							}
						}
					}
					rf.mu.Unlock()
				}
			}
			for i := 0; i < n; i++ { if i != rf.me { go f(i) } }
			time.Sleep(HEARTBEAT_INTERVAL)
		} else if rf.state == FOLLOWER {
			if rf.lastHB.Add(rf.timeout).After(time.Now()) {
				rf.mu.Unlock()
				time.Sleep(TICK_INTERVAL)
				continue
			}
			rf.term++
			rf.vote = rf.me
			rf.state = CANDIDATE
			rf.persist()
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
				if args.LastLogIndex > 0 { args.LastLogTerm = rf.logs[rf.lastLogIndex].Term }
				rf.mu.Unlock()
				rf.peers[peer].Call("Raft.RPCRequestVote", &args, &repl)
				c <- repl
			}
			for i := 0; i < n; i++ { if i != rf.me { go f(i) } }
			granted := 0
			m := n / 2 + 1
			for i := 0; granted < m && granted + n - i >= m; i++ {
				res := <- c
				if res.VoteGranted {
					granted++
				} else if res.Term > newterm {
					rf.mu.Lock()
					rf.state = FOLLOWER
					if res.Term > rf.term {
						rf.term = res.Term
						rf.vote = -1
						rf.persist()
					}
					rf.mu.Unlock()
					break
				}
			}
			rf.mu.Lock()
			if granted == m && rf.term == newterm {
				rf.state = LEADER
				rf.nextIndex = make([]int, n)
				rf.matchIndex = make([]int, n)
				for i := 0; i < n; i++ { 
					rf.nextIndex[i]  = rf.lastLogIndex + 1
					rf.matchIndex[i] = 0
				}
			} else {
				rf.state = FOLLOWER
			}
			rf.timeout = RandElectionTimeout()
			rf.mu.Unlock()
		} else {
			log.Fatalln("This should not happen!")
		}
	}
}

func (rf *Raft) setupargs(args *RPCAppendEntriesArgs, peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER { return false }
	args.LeaderId = rf.me
	args.Term = rf.term
	args.LeaderCommitIndex = rf.commitIndex
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
	if args.PrevLogIndex > 0 { args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term }
	return true
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.applyCh = applyCh
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = FOLLOWER
	rf.term = 0
	rf.vote = -1
	rf.lastHB = time.Now()
	rf.timeout = RandElectionTimeout()
	rf.logs = make(map[int]*LogEntry)
	rf.commitIndex = 0
	rf.lastLogIndex = 0
	rf.readPersist(rf.persister.ReadRaftState())
	time.Sleep(rf.timeout)
	go rf.ticker()
	return rf
}

// Caller should hold the lock
func (rf *Raft) persist() {
	w := &bytes.Buffer{}
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.vote)
	e.Encode(rf.logs)
	rf.persister.SaveRaftState(w.Bytes())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { return }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.term) != nil || d.Decode(&rf.vote) != nil || d.Decode(&rf.logs) != nil {
		log.Fatalln("readPersist has failed")
	}
	for index := range rf.logs { if index > rf.lastLogIndex { rf.lastLogIndex = index } }
}

// Invoked by candidates during elections
func (rf *Raft) RPCRequestVote(args *RPCRequestVoteArgs, reply *RPCRequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	shouldPersist := false
	defer func(){ if shouldPersist {rf.persist()} }()
	if args.Term < rf.term || (args.Term == rf.term && rf.vote != args.CandidateId) {
		reply.Term = rf.term
		reply.VoteGranted = false
	} else {
		if args.Term > rf.term {
			rf.term = args.Term
			rf.vote = -1
			rf.state = FOLLOWER
			shouldPersist = true
		}
		if (rf.lastLogIndex > 0 && (args.LastLogTerm < rf.logs[rf.lastLogIndex].Term ||
			(args.LastLogTerm == rf.logs[rf.lastLogIndex].Term && args.LastLogIndex < rf.lastLogIndex))) {
			reply.VoteGranted = false
		} else {
			rf.vote = args.CandidateId
			reply.VoteGranted = true
			shouldPersist = true
		}
	}
}


// Invoked by leaders to replicate log entries and to provide heartbeats
func (rf *Raft) RPCAppendEntries(args *RPCAppendEntriesArgs, reply *RPCAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	shouldPersist := false
	defer func(){ if shouldPersist {rf.persist()} }()
	if args.Term < rf.term {
		reply.Term = rf.term
	} else {
		if args.Term > rf.term {
			rf.term = args.Term
			rf.vote = args.LeaderId
			shouldPersist = true
		}
		rf.state = FOLLOWER
		rf.lastHB = time.Now()
		reply.Term = args.Term
		reply.Success = true
		if args.PrevLogIndex > rf.lastLogIndex {
			reply.Index = rf.lastLogIndex + 1
			reply.Success = false
			return
		} else if args.PrevLogIndex != 0 && args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term {
			t := rf.logs[args.PrevLogIndex].Term
			reply.Index = args.PrevLogIndex
			for reply.Index > 1 && rf.logs[reply.Index-1].Term == t { reply.Index-- }
			reply.Success = false
			return
		}
		if len(args.Entries) > 0 {
			for i, l_log := range args.Entries {
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
		if args.LeaderCommitIndex > rf.commitIndex {
			i := min(args.LeaderCommitIndex, rf.lastLogIndex)
			for j := rf.commitIndex + 1; j <= i; j++ {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					CommandIndex: j,
					Command: rf.logs[j].Command,
				}
			}
			rf.commitIndex = i
		}
	}
}

func (rf *Raft) RPCInstallSnapshot(args *RPCInstallSnapshotArgs, reply *RPCInstallSnapshotReply) {
	// Invoked by leader to send chunk of a snapshot to a follower
}

func RandElectionTimeout() time.Duration {
	return ELECTION_LB_TIMEOUT + time.Duration(rand.Int63n(int64(ELECTION_UB_TIMEOUT - ELECTION_LB_TIMEOUT + 1)))
}