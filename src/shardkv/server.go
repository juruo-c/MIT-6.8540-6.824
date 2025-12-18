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
	Type      string
	Key       string
	Value     string
	SeqId     int
	ClientId  int64
	NewConfig shardctrler.Config
	ConfigNum int
	Shard     int
	Data      map[string]string
	LastReply map[int64]LastReply
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

	sc         *shardctrler.Clerk
	config     shardctrler.Config
	prevConfig shardctrler.Config

	shardStatus [shardctrler.NShards]int
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
	if kv.config.Shards[shard] != kv.gid ||
		kv.shardStatus[shard] != ShardServing {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
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

	// check the shard group
	kv.mu.Lock()
	if kv.config.Shards[shard] != kv.gid ||
		kv.shardStatus[shard] != ShardServing {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Unlock()

	ch := kv.getWaitCh(index)

	select {
	case appliedOp := <-ch:
		if equalOp(&appliedOp, &op) && kv.rf.IsLeader() {
			kv.mu.Lock()
			reply.Err = OK
			reply.Value = kv.kvstore[args.Key]
			kv.mu.Unlock()
			log.Printf("Get data from server-%d-%d, k,v={%v,%v}\n", kv.gid, kv.me, args.Key, reply.Value)
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
	if kv.config.Shards[shard] != kv.gid ||
		kv.shardStatus[shard] != ShardServing {
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

	// check the shard group
	kv.mu.Lock()
	if kv.config.Shards[shard] != kv.gid ||
		kv.shardStatus[shard] != ShardServing {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Unlock()

	// check duplicate request again
	kv.mu.Lock()
	last, ok = kv.lastReply[args.ClientId]
	if ok && args.SeqId <= last.SeqId {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	kv.mu.Unlock()

	ch := kv.getWaitCh(index)

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

func (kv *ShardKV) PullShardRPC(args *PullShardArgs, reply *PullShardReply) {
	kv.mu.Lock()
	if kv.config.Num < args.ConfigNum {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	shard := args.Shard
	if kv.shardStatus[shard] != ShardGC &&
		kv.shardStatus[shard] != ShardServing {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Type: PULLSHARD,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if kv.config.Num < args.ConfigNum {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if kv.shardStatus[shard] != ShardGC &&
		kv.shardStatus[shard] != ShardServing {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	ch := kv.getWaitCh(index)

	select {
	case appliedOp := <-ch:
		if equalOp(&appliedOp, &op) && kv.rf.IsLeader() {
			kv.mu.Lock()
			data := make(map[string]string)
			for k, v := range kv.kvstore {
				if key2shard(k) == shard {
					data[k] = v
				}
			}
			reply.Data = data
			reply.LastReply = copyLastReply(kv.lastReply)
			kv.mu.Unlock()
			reply.Err = OK
		} else {
			reply.Err = ErrWrongGroup
		}
	case <-time.After(800 * time.Millisecond):
		reply.Err = ErrWrongGroup
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
		break
	case PUT:
		kv.kvstore[op.Key] = op.Value
		kv.lastReply[op.ClientId] = LastReply{
			SeqId: op.SeqId,
		}
		log.Printf("server-%d-%d apply client(%v) PUT (%d), value={%v}\n", kv.gid, kv.me, op.ClientId, op.SeqId, op.Value)
	case APPEND:
		kv.kvstore[op.Key] += op.Value
		kv.lastReply[op.ClientId] = LastReply{
			SeqId: op.SeqId,
		}
		log.Printf("server-%d-%d apply client(%v) APPEND {%d, %v}, value={%v}\n", kv.gid, kv.me, op.ClientId, op.SeqId, op.Value, kv.kvstore[op.Key])
	case CONFIG:
		if op.NewConfig.Num == kv.config.Num+1 {
			if kv.config.Num == 0 {
				for s := 0; s < shardctrler.NShards; s++ {
					if op.NewConfig.Shards[s] == kv.gid {
						kv.shardStatus[s] = ShardServing
					}
				}
			} else {
				for s := 0; s < shardctrler.NShards; s++ {
					// get new shards
					if kv.config.Shards[s] != kv.gid &&
						op.NewConfig.Shards[s] == kv.gid &&
						kv.shardStatus[s] != ShardInstalling {
						kv.shardStatus[s] = ShardPulling
					}
					// lose old shards
					if kv.config.Shards[s] == kv.gid &&
						op.NewConfig.Shards[s] != kv.gid &&
						kv.shardStatus[s] != ShardInstalling {
						kv.shardStatus[s] = ShardGC
					}
				}
			}
			copyConfig(&kv.prevConfig, &kv.config)
			kv.config = op.NewConfig
		}
	case SHARDINSTALL:
		if op.ConfigNum != kv.config.Num {
			return
		}
		log.Printf("server-%d-%d start install shard %d, shardStatus=%d\n", kv.gid, kv.me, op.Shard, kv.shardStatus[op.Shard])
		if kv.shardStatus[op.Shard] == ShardInstalling ||
			kv.shardStatus[op.Shard] == ShardPulling {
			for k, v := range op.Data {
				kv.kvstore[k] = v
			}
			for cid, new := range op.LastReply {
				if old, ok := kv.lastReply[cid]; !ok || new.SeqId > old.SeqId {
					kv.lastReply[cid] = new
				}
			}
			kv.shardStatus[op.Shard] = ShardServing
			log.Printf("server-%d-%d install shard %d over: kv={%v}\n", kv.gid, kv.me, op.Shard, kv.kvstore)
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

		// check the shard group
		if op.Type == GET || op.Type == PUT || op.Type == APPEND {
			shard := key2shard(op.Key)
			if kv.config.Shards[shard] != kv.gid ||
				kv.shardStatus[shard] != ShardServing {
				kv.mu.Unlock()
				continue
			}
		}

		// duplicate checking
		if op.Type == PUT || op.Type == APPEND {
			last, ok := kv.lastReply[op.ClientId]
			if ok && op.SeqId <= last.SeqId {
				kv.mu.Unlock()
				continue
			}
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
	e.Encode(kv.prevConfig)
	e.Encode(kv.config)
	e.Encode(kv.shardStatus)
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
		d.Decode(&kv.lastReply) != nil ||
		d.Decode(&kv.prevConfig) != nil ||
		d.Decode(&kv.config) != nil ||
		d.Decode(&kv.shardStatus) != nil {
		log.Fatalf("Server %v decode snapshot error", kv.me)
	}
}

func (kv *ShardKV) configPuller() {
	for !kv.killed() {
		kv.mu.Lock()
		nextNum := kv.config.Num + 1
		kv.mu.Unlock()

		cfg := kv.sc.Query(nextNum)
		if cfg.Num == nextNum {
			op := Op{
				Type: CONFIG,
			}
			copyConfig(&op.NewConfig, &cfg)
			kv.rf.Start(op)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) pullOneShard(cfgNum int, shard int, servers []string) {
	for _, sName := range servers {
		server := kv.make_end(sName)
		args := PullShardArgs{
			ConfigNum: cfgNum,
			Shard:     shard,
		}
		var reply PullShardReply
		log.Printf("server-%d-%d pull shard %d from %v\n", kv.gid, kv.me, shard, sName)
		ok := server.Call("ShardKV.PullShardRPC", &args, &reply)
		if ok && reply.Err == OK {
			op := Op{
				Type:      SHARDINSTALL,
				ConfigNum: cfgNum,
				Shard:     shard,
				Data:      copyKVStore(reply.Data),
				LastReply: copyLastReply(reply.LastReply),
			}
			log.Printf("server-%d-%d get shard %d's data:{%v}\n", kv.gid, kv.me, shard, op.Data)
			kv.rf.Start(op)
			return
		}
	}

	kv.mu.Lock()
	if kv.shardStatus[shard] == ShardInstalling {
		kv.shardStatus[shard] = ShardPulling
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) pullShardLoop() {
	for !kv.killed() {
		kv.mu.Lock()
		for s := 0; s < shardctrler.NShards; s++ {
			if kv.shardStatus[s] == ShardPulling {
				cfg := kv.config
				oldGid := kv.prevConfig.Shards[s]
				servers := kv.prevConfig.Groups[oldGid]
				kv.shardStatus[s] = ShardInstalling
				if oldGid == kv.gid {
					kv.mu.Unlock()
					op := Op{
						Type:      SHARDINSTALL,
						ConfigNum: cfg.Num,
						Shard:     s,
						Data:      make(map[string]string),
						LastReply: make(map[int64]LastReply),
					}
					kv.rf.Start(op)
					kv.mu.Lock()
					continue
				}
				go kv.pullOneShard(cfg.Num, s, servers)
			}
		}
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
	labgob.Register(shardctrler.Config{})
	labgob.Register(PullShardArgs{})
	labgob.Register(PullShardReply{})

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

	kv.sc = shardctrler.MakeClerk(kv.ctrlers)
	kv.config = shardctrler.Config{}
	kv.prevConfig = shardctrler.Config{}

	for i := 0; i < shardctrler.NShards; i++ {
		kv.shardStatus[i] = ShardGC
	}

	kv.persister = persister
	kv.decodeSnapshot(kv.persister.ReadSnapshot())

	go kv.readApplied()

	go kv.configPuller()

	go kv.pullShardLoop()

	return kv
}

// helper function
func equalOp(opA *Op, opB *Op) bool {
	return opA.SeqId == opB.SeqId && opA.ClientId == opB.ClientId
}

func copyKVStore(kvstore map[string]string) map[string]string {
	copy := make(map[string]string)
	for k, v := range kvstore {
		copy[k] = v
	}
	return copy
}

func copyLastReply(lastReply map[int64]LastReply) map[int64]LastReply {
	copy := make(map[int64]LastReply)
	for k, v := range lastReply {
		copy[k] = v
	}
	return copy
}

func copyConfig(dst *shardctrler.Config, src *shardctrler.Config) {
	dst.Num = src.Num
	dst.Shards = src.Shards
	dst.Groups = make(map[int][]string)
	for gid, servers := range src.Groups {
		copied := make([]string, len(servers))
		copy(copied, servers)
		dst.Groups[gid] = copied
	}
}
