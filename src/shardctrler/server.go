package shardctrler

import (
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type LastReply struct {
	SeqId int
	Reply Config
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs   []Config // indexed by config num
	waitChs   map[int]chan Op
	lastReply map[int64]LastReply
}

type Op struct {
	// Your data here.
	Type     string
	Servers  map[int][]string // for join
	GIDs     []int            // for leave
	Shard    int              // for move
	GID      int              // for move
	Num      int              // for query
	SeqId    int
	ClientId int64
}

func (sc *ShardCtrler) getWaitCh(index int) chan Op {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch, ok := sc.waitChs[index]
	if !ok {
		ch = make(chan Op, 1)
		sc.waitChs[index] = ch
	}
	return ch
}

func (sc *ShardCtrler) removeWaitCh(index int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	delete(sc.waitChs, index)
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	if !sc.rf.IsLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	// check duplicate request
	sc.mu.Lock()
	last, ok := sc.lastReply[args.ClientId]
	if ok && args.SeqId <= last.SeqId {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	op := Op{
		Type:     JOIN,
		Servers:  args.Servers,
		SeqId:    args.SeqId,
		ClientId: args.ClientId,
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := sc.getWaitCh(index)

	// check duplicate request again
	sc.mu.Lock()
	last, ok = sc.lastReply[args.ClientId]
	if ok && args.SeqId <= last.SeqId {
		sc.mu.Unlock()
		sc.removeWaitCh(index)
		reply.Err = OK
		return
	}
	sc.mu.Unlock()

	select {
	case appliedOp := <-ch:
		if equalOp(&appliedOp, &op) && sc.rf.IsLeader() {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(800 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
	sc.removeWaitCh(index)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	if !sc.rf.IsLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	// check duplicate request
	sc.mu.Lock()
	last, ok := sc.lastReply[args.ClientId]
	if ok && args.SeqId <= last.SeqId {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	op := Op{
		Type:     LEAVE,
		GIDs:     args.GIDs,
		SeqId:    args.SeqId,
		ClientId: args.ClientId,
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := sc.getWaitCh(index)

	// check duplicate request again
	sc.mu.Lock()
	last, ok = sc.lastReply[args.ClientId]
	if ok && args.SeqId <= last.SeqId {
		sc.mu.Unlock()
		sc.removeWaitCh(index)
		reply.Err = OK
		return
	}
	sc.mu.Unlock()

	select {
	case appliedOp := <-ch:
		if equalOp(&appliedOp, &op) && sc.rf.IsLeader() {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(800 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
	sc.removeWaitCh(index)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	if !sc.rf.IsLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	// check duplicate request
	sc.mu.Lock()
	last, ok := sc.lastReply[args.ClientId]
	if ok && args.SeqId <= last.SeqId {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	op := Op{
		Type:     MOVE,
		Shard:    args.Shard,
		GID:      args.GID,
		SeqId:    args.SeqId,
		ClientId: args.ClientId,
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := sc.getWaitCh(index)

	// check duplicate request again
	sc.mu.Lock()
	last, ok = sc.lastReply[args.ClientId]
	if ok && args.SeqId <= last.SeqId {
		sc.mu.Unlock()
		sc.removeWaitCh(index)
		reply.Err = OK
		return
	}
	sc.mu.Unlock()

	select {
	case appliedOp := <-ch:
		if equalOp(&appliedOp, &op) && sc.rf.IsLeader() {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(800 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
	sc.removeWaitCh(index)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if !sc.rf.IsLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	// check duplicate request
	sc.mu.Lock()
	last, ok := sc.lastReply[args.ClientId]
	if ok && args.SeqId <= last.SeqId {
		reply.Err = OK
		copyConfig(&reply.Config, &last.Reply)
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	op := Op{
		Type:     QUERY,
		Num:      args.Num,
		SeqId:    args.SeqId,
		ClientId: args.ClientId,
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := sc.getWaitCh(index)

	// check duplicate request again
	sc.mu.Lock()
	last, ok = sc.lastReply[args.ClientId]
	if ok && args.SeqId <= last.SeqId {
		reply.Err = OK
		copyConfig(&reply.Config, &last.Reply)
		sc.mu.Unlock()
		sc.removeWaitCh(index)
		return
	}
	sc.mu.Unlock()

	select {
	case appliedOp := <-ch:
		if equalOp(&appliedOp, &op) && sc.rf.IsLeader() {
			reply.Err = OK
			sc.mu.Lock()
			last = sc.lastReply[args.ClientId]
			copyConfig(&reply.Config, &last.Reply)
			sc.mu.Unlock()
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(800 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
	sc.removeWaitCh(index)
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) rebanlanceShards(config *Config) {
	if len(config.Groups) == 0 {
		for i := 0; i < NShards; i++ {
			config.Shards[i] = 0
		}
		return
	}

	gids := make([]int, 0)
	for gid := range config.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	count := make(map[int]int)
	count[0] = 0
	for _, gid := range gids {
		count[gid] = 0
	}
	for _, gid := range config.Shards {
		count[gid]++
	}

	avg := NShards / len(gids)
	extra := NShards % len(gids)
	target := make(map[int]int)
	target[0] = 0
	for i, gid := range gids {
		if i < extra {
			target[gid] = avg + 1
		} else {
			target[gid] = avg
		}
	}

	over := []int{}
	under := []int{}
	over = append(over, 0)
	for _, gid := range gids {
		if count[gid] < target[gid] {
			under = append(under, gid)
		} else if count[gid] > target[gid] {
			over = append(over, gid)
		}
	}

	for _, ogid := range over {
		for count[ogid] > target[ogid] {
			for i := 0; i < NShards && count[ogid] > target[ogid]; i++ {
				if config.Shards[i] == ogid {
					ugid := under[0]
					config.Shards[i] = ugid
					count[ogid]--
					count[ugid]++
					if count[ugid] == target[ugid] {
						under = under[1:]
					}
				}
			}
		}
	}
}

// should hold lock before calling
func (sc *ShardCtrler) applyOp(op *Op) {
	switch op.Type {
	case QUERY:
		if op.Num == -1 || op.Num >= len(sc.configs) {
			op.Num = len(sc.configs) - 1
		}
		sc.lastReply[op.ClientId] = LastReply{
			SeqId: op.SeqId,
			Reply: sc.configs[op.Num],
		}
	case JOIN:
		newConfig := &Config{}
		copyConfig(newConfig, &sc.configs[len(sc.configs)-1])
		newConfig.Num++

		for gid, servers := range op.Servers {
			newConfig.Groups[gid] = append([]string{}, servers...)
		}
		sc.rebanlanceShards(newConfig)
		sc.configs = append(sc.configs, *newConfig)

		sc.lastReply[op.ClientId] = LastReply{
			SeqId: op.SeqId,
		}
	case LEAVE:
		newConfig := &Config{}
		copyConfig(newConfig, &sc.configs[len(sc.configs)-1])
		newConfig.Num++

		leaving := make(map[int]bool)
		for _, gid := range op.GIDs {
			delete(newConfig.Groups, gid)
			leaving[gid] = true
		}
		for i := 0; i < NShards; i++ {
			if leaving[newConfig.Shards[i]] {
				newConfig.Shards[i] = 0
			}
		}
		sc.rebanlanceShards(newConfig)
		sc.configs = append(sc.configs, *newConfig)

		sc.lastReply[op.ClientId] = LastReply{
			SeqId: op.SeqId,
		}
	case MOVE:
		newConfig := &Config{}
		copyConfig(newConfig, &sc.configs[len(sc.configs)-1])
		newConfig.Num++

		newConfig.Shards[op.Shard] = op.GID
		sc.configs = append(sc.configs, *newConfig)

		sc.lastReply[op.ClientId] = LastReply{
			SeqId: op.SeqId,
		}
	}
}

func (sc *ShardCtrler) readApplied() {
	for msg := range sc.applyCh {
		sc.mu.Lock()
		op := msg.Command.(Op)

		// duplicate checking
		last, ok := sc.lastReply[op.ClientId]
		if ok && op.SeqId <= last.SeqId {
			sc.mu.Unlock()
			continue
		}

		sc.applyOp(&op)
		ch, ok := sc.waitChs[msg.CommandIndex]
		if ok {
			delete(sc.waitChs, msg.CommandIndex)
		}
		sc.mu.Unlock()

		if ok {
			ch <- op
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.waitChs = make(map[int]chan Op)
	sc.lastReply = make(map[int64]LastReply)

	go sc.readApplied()

	return sc
}

// helper function
func equalOp(opA *Op, opB *Op) bool {
	return opA.SeqId == opB.SeqId && opA.ClientId == opB.ClientId
}

func copyConfig(dst *Config, src *Config) {
	dst.Num = src.Num
	dst.Shards = src.Shards
	dst.Groups = make(map[int][]string)
	for gid, servers := range src.Groups {
		copied := make([]string, len(servers))
		copy(copied, servers)
		dst.Groups[gid] = copied
	}
}
