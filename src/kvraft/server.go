package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

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
	seqId int
	reply string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvstore   map[string]string
	waitChs   map[int]chan Op
	lastReply map[int64]LastReply
}

func (kv *KVServer) getWaitCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.waitChs[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.waitChs[index] = ch
	}
	return ch
}

func (kv *KVServer) removeWaitCh(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.waitChs, index)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if !kv.rf.IsLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	// check duplicate request
	kv.mu.Lock()
	last, ok := kv.lastReply[args.ClientId]
	if ok && args.SeqId <= last.seqId {
		reply.Err = OK
		reply.Value = last.reply
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
	if ok && args.SeqId <= last.seqId {
		kv.mu.Unlock()
		kv.removeWaitCh(index)
		reply.Err = OK
		return
	}
	kv.mu.Unlock()

	select {
	case appliedOp := <-ch:
		if equalOp(&appliedOp, &op) {
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

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if !kv.rf.IsLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	// check duplicate request
	kv.mu.Lock()
	last, ok := kv.lastReply[args.ClientId]
	if ok && args.SeqId <= last.seqId {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Type:     PUT,
		Key:      args.Key,
		Value:    args.Value,
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
	if ok && args.SeqId <= last.seqId {
		kv.mu.Unlock()
		kv.removeWaitCh(index)
		reply.Err = OK
		return
	}
	kv.mu.Unlock()

	select {
	case appliedOp := <-ch:
		if equalOp(&appliedOp, &op) {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(800 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
	kv.removeWaitCh(index)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if !kv.rf.IsLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	// check duplicate request
	kv.mu.Lock()
	last, ok := kv.lastReply[args.ClientId]
	if ok && args.SeqId <= last.seqId {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Type:     APPEND,
		Key:      args.Key,
		Value:    args.Value,
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
	if ok && args.SeqId <= last.seqId {
		kv.mu.Unlock()
		kv.removeWaitCh(index)
		reply.Err = OK
		return
	}
	kv.mu.Unlock()

	select {
	case appliedOp := <-ch:
		if equalOp(&appliedOp, &op) {
			reply.Err = OK
			return
		} else {
			reply.Err = ErrWrongLeader
			return
		}
	case <-time.After(800 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
	kv.removeWaitCh(index)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// should hold lock before calling
func (kv *KVServer) applyOp(op *Op) {
	switch op.Type {
	case GET:
		kv.lastReply[op.ClientId] = LastReply{
			seqId: op.SeqId,
			reply: kv.kvstore[op.Key],
		}
	case PUT:
		kv.kvstore[op.Key] = op.Value
		kv.lastReply[op.ClientId] = LastReply{
			seqId: op.SeqId,
		}
	case APPEND:
		kv.kvstore[op.Key] += op.Value
		kv.lastReply[op.ClientId] = LastReply{
			seqId: op.SeqId,
		}
	}
}

func (kv *KVServer) readApplied() {
	for msg := range kv.applyCh {
		kv.mu.Lock()
		op := msg.Command.(Op)

		// duplicate checking
		last, ok := kv.lastReply[op.ClientId]
		if ok && op.SeqId <= last.seqId {
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
	}
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvstore = make(map[string]string)
	kv.waitChs = make(map[int]chan Op)
	kv.lastReply = make(map[int64]LastReply)

	go kv.readApplied()

	return kv
}

// helper function
func equalOp(opA *Op, opB *Op) bool {
	return opA.SeqId == opB.SeqId && opA.ClientId == opB.ClientId
}
