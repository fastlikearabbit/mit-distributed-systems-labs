package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type VersionedValue struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu           sync.RWMutex
	versionedMap map[string]VersionedValue
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.versionedMap = make(map[string]VersionedValue)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	versionedValue, ok := kv.versionedMap[args.Key]
	if ok {
		reply.Err = rpc.OK
		reply.Value = versionedValue.Value
		reply.Version = versionedValue.Version
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
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
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
