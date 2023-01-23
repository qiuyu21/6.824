package raft1

import (
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

func Make(peers []*labrpc.ClientEnd, me int,
    persister *Persister, applyCh chan ApplyMsg) *Raft {
    rf := &Raft{}
    rf.peers 			= peers
    rf.applyCh          = applyCh
    rf.persister 		= persister
    rf.me 				= me
    rf.dead 			= 0
    rf.isLeader 		= false
    rf.term 			= 0
    rf.vote 			= -1
    rf.commitIndex 		= 0
    rf.nextCommitIndex 	= 0
    rf.lastHB 			= time.Now()
    rf.timeout 			= RandElectionTimeout()
    rf.commitCond       = sync.NewCond(&rf.mu)
    rf.notifyCond       = sync.NewCond(&rf.mu)
    rf.logManager       = NewHashLogManager()
    rf.readPersistState(persister.ReadRaftState())
    go rf.commit()
    go rf.ticker()
    for i := 0; i < len(rf.peers); i++ {
        if i != rf.me {
            go rf.replicate(i)
        }
    }
    return rf
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if !rf.isLeader { return -1, -1, false }
    rf.matchIndex[rf.me] = rf.logManager.AppendLog(&LogEntry{Term: rf.term, Command: command})
    rf.persistState()
    rf.notifyCond.Broadcast()
    return rf.matchIndex[rf.me], rf.term, rf.isLeader
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool { return true }

func (rf *Raft) GetState() (int, bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    return rf.term, rf.isLeader
}

func (rf *Raft) Kill() {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    rf.dead = 1
    rf.notifyCond.Broadcast()
    rf.commitCond.Signal()
}

func (rf *Raft) killed() bool {
    return atomic.LoadInt32(&rf.dead) == 1
}

func (rf *Raft) persistState() {
}

func (rf *Raft) persistStateAndSnapshot() {
}

func (rf *Raft) readPersistState(data []byte) {
    if data == nil || len(data) < 1 { return }
}

func (rf *Raft) serializeState() []byte { return nil }

func (rf *Raft) Snapshot(index int, snapshot []byte) {
}

func (rf *Raft) ticker() {
    n := len(rf.peers)
    for !rf.killed() {
        rf.mu.Lock()
        if rf.isLeader {
            rf.mu.Unlock()
            for i := 0; i < n; i++ { if i != rf.me { go rf.heartbeat(i, time.Now()) } }
            time.Sleep(HEARTBEAT_INTERVAL)
        } else {
            if rf.lastHB.Add(rf.timeout).After(time.Now()) {
                rf.mu.Unlock()
                time.Sleep(TICK_INTERVAL)
                continue
            }
            rf.election()
        }
    }
}

func (rf *Raft) heartbeat(peer int, t0 time.Time) {
    var args RPCAppendEntriesArgs
    var repl RPCAppendEntriesReply
    for {
        rf.mu.Lock()
        if !rf.isLeader {
            rf.mu.Unlock()
            return
        }
        args.LeaderId = rf.me
        args.Term = rf.term
        args.LeaderCommitIndex = min(rf.commitIndex, rf.matchIndex[peer])
        rf.mu.Unlock()
        if !rf.peers[peer].Call("Raft.RPCAppendEntries", &args, &repl) {
            if time.Now().Sub(t0) > HEARTBEAT_INTERVAL { break }
            time.Sleep(TICK_INTERVAL)
        } else {
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
            break
        }
    }
}

func (rf *Raft) election() {
    n := len(rf.peers)
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
        if len(re) == 0 { cd.Wait() }
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
            rf.nextIndex[i]  = rf.logManager.GetLastLogIndex() + 1
            rf.matchIndex[i] = 0
        }
    }
    rf.lastHB = time.Now()
    rf.timeout = RandElectionTimeout()
    rf.mu.Unlock()
}

func (rf *Raft) replicate(peer int) {
    for {
        rf.mu.Lock()
        if rf.dead == 1 {
            rf.mu.Unlock()
            return
        }
        for !rf.isLeader || rf.nextIndex[peer] > rf.logManager.GetLastLogIndex() {
            rf.notifyCond.Wait()
            if rf.dead == 1 {
                rf.mu.Unlock()
                return
            }
        }
        rf._replicate(peer)
    }
}

func (rf *Raft) _replicate(peer int) {
    n := len(rf.peers)
    for {
        if rf.dead == 1 || !rf.isLeader {
            rf.mu.Unlock()
            return
        } else if rf.snapshotLastIndex > 0 && rf.nextIndex[peer] <= rf.snapshotLastIndex {
            log.Fatalln("NOT IMPLEMENTED")
        } else {
            var args RPCAppendEntriesArgs
            var repl RPCAppendEntriesReply
            rf.setupAppendEntriesArgs(&args, peer)
            if len(args.Entries) == 0 {
                rf.mu.Unlock()
                return
            }
            rf.mu.Unlock()
            if !rf.peers[peer].Call("Raft.RPCAppendEntries", &args, &repl) {
                time.Sleep(1 * time.Millisecond)
                rf.mu.Lock()
                continue
            } else {
                rf.mu.Lock()
                if rf.dead == 1 || !rf.isLeader {
                    rf.mu.Unlock()
                    return
                } else {
                    if repl.Term > args.Term {
                        if repl.Term > rf.term {
                            rf.isLeader = false
                            rf.term = repl.Term
                            rf.vote = -1
                            rf.lastHB = time.Now()
                            rf.persistState()
                        } else {
                            rf.mu.Unlock()
                            time.Sleep(1 * time.Millisecond)
                            rf.mu.Lock()
                            continue
                        }
                    } else if !repl.Success {
                        rf.nextIndex[peer] = repl.Index
                        rf.mu.Unlock()
                        time.Sleep(1 * time.Millisecond)
                        rf.mu.Lock()
                        continue
                    } else {
                        m := len(args.Entries)
                        i := args.PrevLogIndex + m
                        if i + 1 > rf.nextIndex[peer] {
                            rf.nextIndex[peer] = i + 1
                            rf.matchIndex[peer] = i
                            sorted := make([]int, len(rf.matchIndex))
                            copy(sorted, rf.matchIndex)
                            sort.Ints(sorted)
                            j := sorted[n-n/2-1]
                            _, log := rf.logManager.GetLog(j)
                            if j > rf.snapshotLastIndex && j > rf.commitIndex && log.Term == rf.term {
                                rf.nextCommitIndex = j
                                rf.commitCond.Signal()
                            }
                        }
                    }
                    rf.mu.Unlock()
                    return
                }
            }
        }
    }
}

func (rf *Raft) commit() {
    for {
        rf.mu.Lock()
        if rf.dead == 1 {
            rf.mu.Unlock()
            return
        }
        for rf.commitIndex == rf.nextCommitIndex {
            rf.commitCond.Wait()
            if rf.dead == 1 {
                rf.mu.Unlock()
                return
            }
        }
        i, j := rf.commitIndex + 1, rf.nextCommitIndex
        rf.mu.Unlock()
        for ; i <= j; i++ {
            rf.mu.Lock()
            if i <= rf.snapshotLastIndex {
                rf.mu.Unlock()
                continue
            }
            _, log := rf.logManager.GetLog(i)
            rf.mu.Unlock()
            rf.applyCh <- ApplyMsg{
                CommandValid: true,
                CommandIndex: i,
                Command: log.Command,
            }
        }
        rf.mu.Lock()
        rf.commitIndex = j
        rf.mu.Unlock()
    }
}

func (rf *Raft) setupRequestVoteArgs(args *RPCRequestVoteArgs) {
    args.CandidateId = rf.me
    args.Term = rf.term
    args.LastLogIndex = rf.logManager.GetLastLogIndex()
    if args.LastLogIndex > 0 {
        if args.LastLogIndex == rf.snapshotLastIndex {
            args.LastLogTerm = rf.snapshotLastTerm
        } else {
            _, log := rf.logManager.GetLog(args.LastLogIndex)
            args.LastLogTerm = log.Term
        }
    }
}

func (rf *Raft) setupAppendEntriesArgs(args *RPCAppendEntriesArgs, peer int) {
    args.LeaderId = rf.me
    args.Term = rf.term
    args.LeaderCommitIndex = min(rf.matchIndex[peer], rf.commitIndex)
    if lastIndex := rf.logManager.GetLastLogIndex(); lastIndex >= rf.nextIndex[peer] {
        rf.logManager.GetLogs(&args.Entries, rf.nextIndex[peer], lastIndex)
        args.PrevLogIndex = rf.nextIndex[peer] - 1
    } else {
        args.PrevLogIndex = lastIndex
    }
    if args.PrevLogIndex > 0 {
        if args.PrevLogIndex == rf.snapshotLastIndex {
            args.PrevLogTerm = rf.snapshotLastTerm
        } else {
            _, lastLog := rf.logManager.GetLog(args.PrevLogIndex)
            args.PrevLogTerm = lastLog.Term
        }
    }
}

func (rf *Raft) RPCRequestVote(args *RPCRequestVoteArgs, reply *RPCRequestVoteReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    cmp_uptodate := func() bool {
        if lastIndex := rf.logManager.GetLastLogIndex(); lastIndex > 0 {
            if rf.logManager.GetSize() == 0 {
                if args.LastLogTerm < rf.snapshotLastTerm { 
                    return false
                } else if args.LastLogTerm == rf.snapshotLastTerm && args.LastLogIndex < lastIndex {
                    return false
                }
            } else {
                _, log := rf.logManager.GetLog(lastIndex)
                if args.LastLogTerm < log.Term {
                    return false
                } else if args.LastLogTerm == log.Term && args.LastLogIndex < lastIndex {
                    return false
                }
            }
        }
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
        if lastIndex := rf.logManager.GetLastLogIndex(); args.PrevLogIndex > lastIndex {
            reply.Index = lastIndex + 1
            reply.Success = false
            if shouldPersist { rf.persistState() }
            rf.mu.Unlock()
            return
        } else if args.PrevLogIndex > 0 {
            if args.PrevLogIndex == rf.snapshotLastIndex {
                if args.PrevLogTerm != rf.snapshotLastTerm { log.Fatalln("2:This should not happen!") }
            } else if args.PrevLogIndex > rf.snapshotLastIndex {
                _, log := rf.logManager.GetLog(args.PrevLogIndex)
                if t := log.Term; t != args.PrevLogTerm {
                    reply.Index = args.PrevLogIndex
                    for reply.Index > 1 && reply.Index-1 > rf.snapshotLastIndex {
                        _, log1 := rf.logManager.GetLog(reply.Index - 1)
                        if log1.Term != t { break }
                        reply.Index--
                    }
                    reply.Success = false
                    if shouldPersist { rf.persistState() }
                    rf.mu.Unlock()
                    return
                }
            }
        }
        for i, l_log := range args.Entries {
            index := args.PrevLogIndex + i + 1  // log index
            if index <= rf.snapshotLastIndex {
                continue
            } else if index <= rf.logManager.GetLastLogIndex() {
                _, log := rf.logManager.GetLog(index)
                if log.Term == l_log.Term { continue }
                rf.logManager.RemoveLogsFrom(index)
            }
            for j := i; j < len(args.Entries); j++ {
                rf.logManager.AppendLog(&LogEntry{
                    Term: args.Entries[j].Term,
                    Command: args.Entries[j].Command,
                })
            }
            shouldPersist = true
            break
        }
        if shouldPersist { rf.persistState() }
        if args.LeaderCommitIndex > rf.commitIndex {
            rf.nextCommitIndex = min(args.LeaderCommitIndex, rf.logManager.GetLastLogIndex())
            rf.commitCond.Signal()
        }
        rf.mu.Unlock()
    }
}

func (rf *Raft) RPCInstallSnapshot(args *RPCInstallSnapshotArgs, reply *RPCInstallSnapshotReply) {}