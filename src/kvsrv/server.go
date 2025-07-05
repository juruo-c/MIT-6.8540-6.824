package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	mp map[string]string
	cache sync.Map
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Value = kv.mp[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if args.Type == Report {
		kv.cache.Delete(args.Id)
		return
	}
	res, ok := kv.cache.Load(args.Id)
	if ok {
		reply.Value = res.(string) 
		return
	}
	kv.mu.Lock()
	old := kv.mp[args.Key]
	kv.mp[args.Key] = args.Value
	reply.Value = old
	kv.mu.Unlock()

	kv.cache.Store(args.Id, old)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if args.Type == Report {
		kv.cache.Delete(args.Id)
		return
	}
	res, ok := kv.cache.Load(args.Id)
	if ok {
		reply.Value = res.(string) // 重复请求，返回之前的结果
		return
	}
	kv.mu.Lock()
	old := kv.mp[args.Key]
	kv.mp[args.Key] = old + args.Value
	reply.Value = old
	kv.mu.Unlock()

	kv.cache.Store(args.Id, old) // 记录请求
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.mp = make(map[string]string)

	return kv
}
