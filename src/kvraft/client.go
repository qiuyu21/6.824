package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	return ck
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) Get(key string) string {
	for {
		for _, server := range ck.servers {
			var args RPCGetArgs
			var repl RPCGetReply
			args.Key = key
			if server.Call("KVServer.RPCGet", &args, &repl) {
				if repl.Err == 0 {
					return repl.Value
				}
			}
		}
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	for {
		for _, server := range ck.servers {
			var args RPCPutAppendArgs
			var repl RPCPutAppendReply
			args.Key = key
			args.Value = value
			args.Op = op
			if server.Call("KVServer.RPCPutAppend", &args, &repl) {
				if repl.Err == 0 {
					return
				}
			}
		}
	}
}