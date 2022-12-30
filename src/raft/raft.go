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
	"log"
	"math/rand"
	"sort"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.term, rf.state == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
	l := &LogEntry{
		Index: rf.lastLogIndex,
		Term: rf.term,
		Command: command,
	}
	rf.logs[rf.lastLogIndex] = l
	rf.matchIndex[rf.me] = rf.lastLogIndex
	return rf.lastLogIndex, rf.term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
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
				rf.setupargs(&args, peer)
				if rf.peers[peer].Call("Raft.RPCAppendEntries", &args, &repl) {
					rf.mu.Lock()
					if rf.state == LEADER {
						if repl.Term > rf.term {
							rf.state = FOLLOWER
							rf.term = repl.Term
							rf.leader = -1
							rf.lastHB = time.Now()
						} else if !repl.Success {
							rf.nextIndex[peer]--	// TODO: implement the optimization in the paper
						} else if len(args.Entries) > 0 {
							i := args.Entries[len(args.Entries)-1].Index
							rf.nextIndex[peer] = i + 1
							rf.matchIndex[peer] = i
							// Update commit index
							sorted := make([]int, len(rf.matchIndex))
							copy(sorted, rf.matchIndex)
							sort.Ints(rf.matchIndex)
							j := sorted[n-n/2-1]
							if j > rf.commitIndex && rf.logs[j].Term == rf.me {
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
			// Start a new election
			rf.term++
			rf.vote = rf.me
			rf.state = CANDIDATE
			rf.timeout = RandElectionTimeout()
			newterm := rf.term
			rf.mu.Unlock()
			c := make(chan RPCRequestVoteReply, n)
			c <- RPCRequestVoteReply{VoteGranted: true}
			f := func(peer int) {
				var args RPCRequestVoteArgs
				var repl RPCRequestVoteReply
				args.CandidateId = rf.me
				args.Term = newterm
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
					rf.term = res.Term
					rf.leader = -1
					rf.lastHB = time.Now()
					rf.mu.Unlock()
					break
				}
			}
			rf.mu.Lock()
			if granted == m {
                rf.state = LEADER
                rf.nextIndex = make([]int, n)
                rf.matchIndex = make([]int, n)
				for i := 0; i < n; i++ { 
					rf.nextIndex[i]  = rf.lastLogIndex + 1
					rf.matchIndex[i] = -1
				}
            } else { 
                rf.state = FOLLOWER
            }
			rf.mu.Unlock()
		} else {
			log.Fatalln("2 - This should not happen!")
		}
	}
}

func (rf *Raft) setupargs(args *RPCAppendEntriesArgs, peer int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
		if rf.nextIndex[peer] == 0 {
			args.PrevLogIndex = -1
		} else {
			args.PrevLogIndex = rf.nextIndex[peer] - 1
			args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		}
	}
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
	rf.commitIndex = -1
	rf.lastLogIndex = -1
	time.Sleep(rf.timeout)
	go rf.ticker()
	return rf
}

// Invoked by candidates during elections
func (rf *Raft) RPCRequestVote(args *RPCRequestVoteArgs, reply *RPCRequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.term ||
        (args.Term == rf.term && rf.vote != -1) ||
        (len(rf.logs) > 0 && (args.LastLogTerm < rf.logs[rf.lastLogIndex].Term ||
        (args.LastLogTerm == rf.logs[rf.lastLogIndex].Term && args.LastLogIndex < rf.lastLogIndex))) {
		reply.Term = rf.term
		reply.VoteGranted = false
	} else {
		rf.term = args.Term
		rf.vote = args.CandidateId
		reply.VoteGranted = true
	}
}

// Invoked by leaders to replicate log entries and to provide heartbeats
func (rf *Raft) RPCAppendEntries(args *RPCAppendEntriesArgs, reply *RPCAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.term {
		reply.Term = rf.term
	} else {
		rf.term = args.Term
		rf.state = FOLLOWER
		rf.leader = args.LeaderId
		rf.lastHB = time.Now()
		reply.Term = args.Term
		reply.Success = true
		if len(args.Entries) > 0 {
			if (args.PrevLogIndex > rf.lastLogIndex) || 
				(args.PrevLogIndex != -1 && args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term) {
				reply.Success = false
			} else {
				for i, l_log := range args.Entries {	// All the logs should be sorted by Index
					if f_log, ok := rf.logs[l_log.Index]; ok {
						if l_log.Term == f_log.Term { continue }
						for j := l_log.Index; j <= rf.lastLogIndex; j++ { delete(rf.logs, j) }
					}
					for j := i; j < len(args.Entries); j++ {
						rf.logs[args.Entries[i].Index] = &LogEntry{
							Index: args.Entries[i].Index,
							Term: args.Entries[i].Term,
							Command: args.Entries[i].Command,
						}
					}
					rf.lastLogIndex = args.Entries[len(args.Entries)-1].Index
					break
				}
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