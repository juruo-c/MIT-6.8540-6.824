package shardkv

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     string
	Key      string
	Value    string
	SeqId    int
	ClientId int64
}

type LastReply struct {
	SeqId int
	Reply string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvstore   map[string]string
	waitChs   map[int]chan Op
	lastReply map[int64]LastReply

	persister *raft.Persister

	sc     *shardctrler.Clerk
	config shardctrler.Config
}

func (kv *ShardKV) getWaitCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.waitChs[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.waitChs[index] = ch
	}
	return ch
}

func (kv *ShardKV) removeWaitCh(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.waitChs, index)
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if !kv.rf.IsLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	// check the shard group
	shard := key2shard(args.Key)
	kv.mu.Lock()
	if kv.config.Shards[shard] != kv.gid {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Unlock()

	// check duplicate request
	kv.mu.Lock()
	last, ok := kv.lastReply[args.ClientId]
	if ok && args.SeqId <= last.SeqId {
		reply.Err = OK
		reply.Value = last.Reply
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Type:     GET,
		Key:      args.Key,
		SeqId:    args.SeqId,
		ClientId: args.ClientId,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.getWaitCh(index)

	// check duplicate request again
	kv.mu.Lock()
	last, ok = kv.lastReply[args.ClientId]
	if ok && args.SeqId <= last.SeqId {
		kv.mu.Unlock()
		kv.removeWaitCh(index)
		reply.Err = OK
		return
	}
	kv.mu.Unlock()

	select {
	case appliedOp := <-ch:
		if equalOp(&appliedOp, &op) && kv.rf.IsLeader() {
			kv.mu.Lock()
			reply.Err = OK
			reply.Value = kv.kvstore[args.Key]
			kv.mu.Unlock()
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(800 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
	kv.removeWaitCh(index)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if !kv.rf.IsLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	// check the shard group
	shard := key2shard(args.Key)
	kv.mu.Lock()
	if kv.config.Shards[shard] != kv.gid {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Unlock()

	// check duplicate request
	kv.mu.Lock()
	last, ok := kv.lastReply[args.ClientId]
	if ok && args.SeqId <= last.SeqId {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Key:      args.Key,
		Value:    args.Value,
		SeqId:    args.SeqId,
		ClientId: args.ClientId,
	}
	if args.Op == "Put" {
		op.Type = PUT
	} else {
		op.Type = APPEND
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.getWaitCh(index)

	// check duplicate request again
	kv.mu.Lock()
	last, ok = kv.lastReply[args.ClientId]
	if ok && args.SeqId <= last.SeqId {
		kv.mu.Unlock()
		kv.removeWaitCh(index)
		reply.Err = OK
		return
	}
	kv.mu.Unlock()

	select {
	case appliedOp := <-ch:
		if equalOp(&appliedOp, &op) && kv.rf.IsLeader() {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(800 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
	kv.removeWaitCh(index)
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// should hold lock before calling
func (kv *ShardKV) applyOp(op *Op) {
	switch op.Type {
	case GET:
		kv.lastReply[op.ClientId] = LastReply{
			SeqId: op.SeqId,
			Reply: kv.kvstore[op.Key],
		}
	case PUT:
		kv.kvstore[op.Key] = op.Value
		kv.lastReply[op.ClientId] = LastReply{
			SeqId: op.SeqId,
		}
	case APPEND:
		kv.kvstore[op.Key] += op.Value
		kv.lastReply[op.ClientId] = LastReply{
			SeqId: op.SeqId,
		}
	}
}

func (kv *ShardKV) readApplied() {
	for msg := range kv.applyCh {
		if msg.SnapshotValid {
			if msg.Snapshot != nil {
				kv.decodeSnapshot(msg.Snapshot)
			}
			continue
		}
		kv.mu.Lock()
		op := msg.Command.(Op)

		// duplicate checking
		last, ok := kv.lastReply[op.ClientId]
		if ok && op.SeqId <= last.SeqId {
			kv.mu.Unlock()
			continue
		}

		kv.applyOp(&op)
		ch, ok := kv.waitChs[msg.CommandIndex]
		if ok {
			delete(kv.waitChs, msg.CommandIndex)
		}
		kv.mu.Unlock()

		if ok {
			ch <- op
		}

		kv.snapshotCheck(msg.CommandIndex)
	}
}

// for snapshot
func (kv *ShardKV) snapshotCheck(index int) {
	if kv.maxraftstate == -1 {
		return
	}
	if kv.persister.RaftStateSize() >= kv.maxraftstate {
		snapshot := kv.encodeSnapshot()
		kv.rf.Snapshot(index, snapshot)
	}
}

func (kv *ShardKV) encodeSnapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvstore)
	e.Encode(kv.lastReply)
	return w.Bytes()
}

func (kv *ShardKV) decodeSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.kvstore) != nil ||
		d.Decode(&kv.lastReply) != nil {
		log.Fatalf("Server %v decode snapshot error", kv.me)
	}
}

func (kv *ShardKV) configPuller() {
	for !kv.killed() {
		cfg := kv.sc.Query(-1)
		kv.mu.Lock()
		kv.config = cfg
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Your initialization code here.

	kv.kvstore = make(map[string]string)
	kv.waitChs = make(map[int]chan Op)
	kv.lastReply = make(map[int64]LastReply)

	kv.persister = persister
	kv.decodeSnapshot(kv.persister.ReadSnapshot())

	kv.sc = shardctrler.MakeClerk(kv.ctrlers)
	kv.config = shardctrler.Config{}

	go kv.readApplied()

	go kv.configPuller()

	return kv
}

// helper function
func equalOp(opA *Op, opB *Op) bool {
	return opA.SeqId == opB.SeqId && opA.ClientId == opB.ClientId
}
