package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type VersionedValue struct {
	Value   string
	Version rpc.Tversion
}
type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	mu           sync.Mutex
	versionedMap map[string]VersionedValue
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch req.(type) {
	case rpc.GetArgs:
		args := req.(rpc.GetArgs)
		reply := rpc.GetReply{}
		versionedValue, ok := kv.versionedMap[args.Key]
		if ok {
			reply.Err = rpc.OK
			reply.Value = versionedValue.Value
			reply.Version = versionedValue.Version
		} else {
			reply.Err = rpc.ErrNoKey
		}
		return reply
	case rpc.PutArgs:
		args := req.(rpc.PutArgs)
		reply := rpc.PutReply{}
		versionedValue, ok := kv.versionedMap[args.Key]
		if ok {
			if args.Version != versionedValue.Version {
				reply.Err = rpc.ErrVersion
			} else {
				reply.Err = rpc.OK
				kv.versionedMap[args.Key] = VersionedValue{Value: args.Value, Version: args.Version + 1}
			}
		} else {
			if args.Version == 0 {
				reply.Err = rpc.OK
				kv.versionedMap[args.Key] = VersionedValue{args.Value, 1}
			} else {
				reply.Err = rpc.ErrNoKey
			}
		}
		return reply
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.versionedMap) != nil {
		log.Fatalf("%v couldn't encode versionedMap", kv.me)
	}
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.versionedMap) != nil {
		log.Fatalf("%v couldn't decode versionedMap", kv.me)
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader || rep == nil {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	getReply := rep.(rpc.GetReply)

	reply.Err = getReply.Err
	reply.Value = getReply.Value
	reply.Version = getReply.Version
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, rep := kv.rsm.Submit(*args)

	if err == rpc.ErrWrongLeader || rep == nil {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	putReply := rep.(rpc.PutReply)
	reply.Err = putReply.Err
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
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rsm.CommitedOp{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(rpc.PutReply{})
	labgob.Register(rpc.GetReply{})

	kv := &KVServer{me: me}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	kv.versionedMap = make(map[string]VersionedValue)

	return []tester.IService{kv, kv.rsm.Raft()}
}
