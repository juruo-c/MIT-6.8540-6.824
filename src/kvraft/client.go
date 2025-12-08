package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	seqId    int
	clientId int64
	leaderId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.seqId = 0
	ck.clientId = nrand()
	ck.leaderId = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.seqId++
	args := &GetArgs{
		Key:      key,
		SeqId:    ck.seqId,
		ClientId: ck.clientId,
	}
	reply := &GetReply{}

	for {
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", args, reply)
		if ok && reply.Err == OK {
			return reply.Value
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(10 * time.Millisecond)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.seqId++
	args := &PutAppendArgs{
		Key:      key,
		Value:    value,
		SeqId:    ck.seqId,
		ClientId: ck.clientId,
	}
	reply := &PutAppendReply{}

	for {
		ok := ck.servers[ck.leaderId].Call("KVServer."+op, args, reply)
		if ok && reply.Err == OK {
			return
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
