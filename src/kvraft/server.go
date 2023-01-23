package kvraft

import (
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	close(kv.closeCh)
}

func (kv *KVServer) killed() bool { return atomic.LoadInt32(&kv.dead) == 1 }

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})
	kv := &KVServer{}
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.applyCond = sync.NewCond(&kv.mu)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.store = NewHashKeyValueStore(nil)
	kv.closeCh = make(chan struct{})
	kv.committed = make(map[int]int)
	go kv.apply()
	return kv
}

func (kv *KVServer) apply() {
	for !kv.killed() {
		select {
		case <- kv.closeCh:
			return
		case applymsg := <- kv.applyCh:
			if applymsg.CommandValid {
				kv.mu.Lock()
				index, term := applymsg.CommandIndex, applymsg.CommandTerm
				cmd := applymsg.Command.(Op)
				kv.store.PutAppend(cmd.Op=="Put", cmd.Key, cmd.Val)
				kv.committed[index] = term
				kv.applyCond.Broadcast()
				kv.mu.Unlock()
			} else {
				panic("NOT IMPLEMENTED")
			}
		}
	}
}

func (kv *KVServer) RPCGet(args *RPCGetArgs, reply *RPCGetReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = 1
	} else {
		reply.Value = kv.store.Get(args.Key)
	}
}

func (kv *KVServer) RPCPutAppend(args *RPCPutAppendArgs, reply *RPCPutAppendReply) {
	for !kv.killed() {
		op := Op{
			Op: args.Op,
			Key: args.Key,
			Val: args.Value,
		}
		index, term, isLeader := kv.rf.Start(op)
		if !isLeader {
			reply.Err = 1
			return
		}
		kv.mu.Lock()
		for !kv.killed() {
			if t, ok := kv.committed[index]; !ok {
				kv.applyCond.Wait()
			} else if term != t {
				break
			} else {
				kv.mu.Unlock()
				reply.Err = 0
				return
			}
		}
		kv.mu.Unlock()
	}
}