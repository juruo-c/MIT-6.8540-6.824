package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

const (
	GET          = "GET"
	PUT          = "PUT"
	APPEND       = "APPEND"
	CONFIG       = "CONFIG"
	PULLSHARD    = "PULLSHARD"
	SHARDINSTALL = "SHARDINSTALL"
)

const (
	ShardServing = iota
	ShardPulling
	ShardInstalling
	ShardGC
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	SeqId    int
	ClientId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	SeqId    int
	ClientId int64
}

type GetReply struct {
	Err   Err
	Value string
}

type PullShardArgs struct {
	ConfigNum int
	Shard     int
}

type PullShardReply struct {
	Err       Err
	Data      map[string]string
	LastReply map[int64]LastReply
}
