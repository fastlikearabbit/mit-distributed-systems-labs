package shardgrp

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
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
	gid  tester.Tgid

	mu           sync.Mutex
	versionedMap map[string]VersionedValue

	latestNum map[shardcfg.Tshid]shardcfg.Tnum
	frozen    map[shardcfg.Tshid]bool
}

func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch req.(type) {
	case rpc.GetArgs:
		args := req.(rpc.GetArgs)
		reply := rpc.GetReply{}

		sh := shardcfg.Key2Shard(args.Key)

		if _, known := kv.latestNum[sh]; !known {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}

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

		sh := shardcfg.Key2Shard(args.Key)

		if kv.frozen[sh] {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}

		if _, known := kv.latestNum[sh]; !known {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}

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

	case shardrpc.FreezeShardArgs:
		args := req.(shardrpc.FreezeShardArgs)
		reply := shardrpc.FreezeShardReply{}

		currentNum, known := kv.latestNum[args.Shard]

		if !known {
			reply.Err = rpc.OK
			reply.Num = args.Num
			state := make(map[string]VersionedValue)
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			if e.Encode(state) == nil {
				reply.State = w.Bytes()
			}
			return reply
		}

		if args.Num <= currentNum {
			reply.Err = rpc.OK
			reply.Num = currentNum

			if kv.frozen[args.Shard] || args.Num == currentNum {
				state := make(map[string]VersionedValue)
				for k, v := range kv.versionedMap {
					sh := shardcfg.Key2Shard(k)
					if sh == args.Shard {
						state[k] = v
					}
				}
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				if e.Encode(state) == nil {
					reply.State = w.Bytes()
				}
			}
			return reply
		}

		kv.frozen[args.Shard] = true
		kv.latestNum[args.Shard] = args.Num

		reply.Err = rpc.OK
		reply.Num = args.Num

		state := make(map[string]VersionedValue)
		for k, v := range kv.versionedMap {
			sh := shardcfg.Key2Shard(k)
			if sh == args.Shard {
				state[k] = v
			}
		}
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		if e.Encode(state) != nil {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}
		reply.State = w.Bytes()

		return reply

	case shardrpc.InstallShardArgs:
		args := req.(shardrpc.InstallShardArgs)
		reply := shardrpc.InstallShardReply{}

		if args.Num <= kv.latestNum[args.Shard] {
			reply.Err = rpc.OK
			return reply
		}

		for k := range kv.versionedMap {
			if shardcfg.Key2Shard(k) == args.Shard {
				delete(kv.versionedMap, k)
			}
		}

		if len(args.State) > 0 {
			stateMap := make(map[string]VersionedValue)
			r := bytes.NewBuffer(args.State)
			d := labgob.NewDecoder(r)
			if d.Decode(&stateMap) != nil {
				reply.Err = rpc.ErrWrongGroup
				return reply
			}

			for k, v := range stateMap {
				kv.versionedMap[k] = v
			}
		}

		kv.latestNum[args.Shard] = args.Num
		reply.Err = rpc.OK
		return reply

	case shardrpc.DeleteShardArgs:
		args := req.(shardrpc.DeleteShardArgs)
		reply := shardrpc.DeleteShardReply{}

		currentNum, known := kv.latestNum[args.Shard]

		if !known {
			reply.Err = rpc.OK
			return reply
		}

		if args.Num < currentNum {
			reply.Err = rpc.OK
			return reply
		}

		for k := range kv.versionedMap {
			sh := shardcfg.Key2Shard(k)
			if sh == args.Shard {
				delete(kv.versionedMap, k)
			}
		}

		delete(kv.latestNum, args.Shard)
		delete(kv.frozen, args.Shard)

		reply.Err = rpc.OK
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
	if e.Encode(kv.latestNum) != nil {
		log.Fatalf("%v couldn't encode latestNum", kv.me)
	}

	if e.Encode(kv.frozen) != nil {
		log.Fatalf("%v couldn't encode frozen", kv.me)
	}
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.versionedMap) != nil {
		log.Fatalf("%v couldn't decode versionedMap", kv.me)
	}
	if d.Decode(&kv.latestNum) != nil {
		log.Fatalf("%v couldn't decode latestNum", kv.me)
	}
	if d.Decode(&kv.frozen) != nil {
		log.Fatalf("%v couldn't decode frozen", kv.me)
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader || err == rpc.ErrWrongGroup {
		reply.Err = err
		return
	}

	getReply := rep.(rpc.GetReply)

	reply.Err = getReply.Err
	reply.Value = getReply.Value
	reply.Version = getReply.Version
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, rep := kv.rsm.Submit(*args)

	if err == rpc.ErrWrongLeader || err == rpc.ErrWrongGroup {
		reply.Err = err
		return
	}
	putReply := rep.(rpc.PutReply)
	reply.Err = putReply.Err
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	err, rep := kv.rsm.Submit(*args)

	if err == rpc.ErrWrongLeader || rep == nil {
		reply.Err = err
		return
	}
	freezeReply := rep.(shardrpc.FreezeShardReply)
	reply.Err = freezeReply.Err

	reply.State = freezeReply.State
	reply.Num = freezeReply.Num
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader || rep == nil {
		reply.Err = err
		return
	}
	installReply := rep.(shardrpc.InstallShardReply)
	reply.Err = installReply.Err
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader || rep == nil {
		reply.Err = err
		return
	}
	deleteReply := rep.(shardrpc.DeleteShardReply)
	reply.Err = deleteReply.Err
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

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})
	labgob.Register(rsm.CommitedOp{})
	labgob.Register(VersionedValue{})
	labgob.Register(map[string]VersionedValue{})

	kv := &KVServer{
		gid:          gid,
		me:           me,
		versionedMap: make(map[string]VersionedValue),
		latestNum:    make(map[shardcfg.Tshid]shardcfg.Tnum),
		frozen:       make(map[shardcfg.Tshid]bool),
	}

	if gid == shardcfg.Gid1 {
		for i := 0; i < shardcfg.NShards; i++ {
			kv.latestNum[shardcfg.Tshid(i)] = 0
		}
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	return []tester.IService{kv, kv.rsm.Raft()}
}
