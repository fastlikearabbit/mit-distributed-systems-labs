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

func (kv *KVServer) getOp(key string) rpc.TxnOpResponse {
	versionedValue, ok := kv.versionedMap[key]
	if !ok {
		return rpc.TxnOpResponse{Kind: rpc.TxnOpGet, Err: rpc.ErrNoKey}
	}
	return rpc.TxnOpResponse{
		Kind:    rpc.TxnOpGet,
		Value:   versionedValue.Value,
		Version: versionedValue.Version,
		Err:     rpc.OK,
	}
}

func (kv *KVServer) putOp(key, value string) rpc.TxnOpResponse {
	versionedValue, ok := kv.versionedMap[key]
	version := rpc.Tversion(1)
	if ok {
		version = versionedValue.Version + 1
	}
	kv.versionedMap[key] = VersionedValue{Value: value, Version: version}
	return rpc.TxnOpResponse{
		Kind:    rpc.TxnOpPut,
		Value:   value,
		Version: version,
		Err:     rpc.OK,
	}
}

func compareVersion(actual, expected rpc.Tversion, result rpc.TxnCompareResult) (bool, bool) {
	switch result {
	case rpc.TxnCompareEqual:
		return actual == expected, true
	case rpc.TxnCompareNotEqual:
		return actual != expected, true
	case rpc.TxnCompareGreater:
		return actual > expected, true
	case rpc.TxnCompareLess:
		return actual < expected, true
	default:
		return false, false
	}
}

func compareValue(actual, expected string, result rpc.TxnCompareResult) (bool, bool) {
	switch result {
	case rpc.TxnCompareEqual:
		return actual == expected, true
	case rpc.TxnCompareNotEqual:
		return actual != expected, true
	case rpc.TxnCompareGreater:
		return actual > expected, true
	case rpc.TxnCompareLess:
		return actual < expected, true
	default:
		return false, false
	}
}

func (kv *KVServer) evaluateCompare(cmp rpc.TxnCompare) (bool, bool) {
	versionedValue, ok := kv.versionedMap[cmp.Key]
	switch cmp.Target {
	case rpc.TxnCompareVersion:
		actual := rpc.Tversion(0)
		if ok {
			actual = versionedValue.Version
		}
		return compareVersion(actual, cmp.Version, cmp.Result)
	case rpc.TxnCompareValue:
		actual := ""
		if ok {
			actual = versionedValue.Value
		}
		return compareValue(actual, cmp.Value, cmp.Result)
	default:
		return false, false
	}
}

func validTxnOps(ops []rpc.TxnOp) bool {
	for _, op := range ops {
		if op.Kind != rpc.TxnOpGet && op.Kind != rpc.TxnOpPut {
			return false
		}
	}
	return true
}

func (kv *KVServer) applyTxn(args rpc.TxnArgs) rpc.TxnReply {
	reply := rpc.TxnReply{Err: rpc.OK}

	if !validTxnOps(args.Then) || !validTxnOps(args.Else) {
		reply.Err = rpc.ErrTxn
		return reply
	}

	succeeded := true
	for _, cmp := range args.Compare {
		matched, valid := kv.evaluateCompare(cmp)
		if !valid {
			reply.Err = rpc.ErrTxn
			return reply
		}
		if !matched {
			succeeded = false
			break
		}
	}

	reply.Succeeded = succeeded
	ops := args.Then
	if !succeeded {
		ops = args.Else
	}

	for _, op := range ops {
		switch op.Kind {
		case rpc.TxnOpGet:
			reply.Responses = append(reply.Responses, kv.getOp(op.Key))
		case rpc.TxnOpPut:
			reply.Responses = append(reply.Responses, kv.putOp(op.Key, op.Value))
		}
	}

	return reply
}

func (kv *KVServer) applyVersionedPut(args rpc.PutArgs) rpc.PutReply {
	txnReply := kv.applyTxn(rpc.TxnArgs{
		Compare: []rpc.TxnCompare{rpc.CmpVersion(args.Key, rpc.TxnCompareEqual, args.Version)},
		Then:    []rpc.TxnOp{rpc.OpPut(args.Key, args.Value)},
		Else:    []rpc.TxnOp{rpc.OpGet(args.Key)},
	})

	if txnReply.Err != rpc.OK {
		return rpc.PutReply{Err: txnReply.Err}
	}
	if txnReply.Succeeded {
		return rpc.PutReply{Err: rpc.OK}
	}
	if len(txnReply.Responses) > 0 && txnReply.Responses[0].Err == rpc.ErrNoKey {
		return rpc.PutReply{Err: rpc.ErrNoKey}
	}
	return rpc.PutReply{Err: rpc.ErrVersion}
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
		res := kv.getOp(args.Key)
		if res.Err == rpc.OK {
			reply.Err = rpc.OK
			reply.Value = res.Value
			reply.Version = res.Version
		} else {
			reply.Err = rpc.ErrNoKey
		}
		return reply
	case rpc.PutArgs:
		args := req.(rpc.PutArgs)
		return kv.applyVersionedPut(args)
	case rpc.TxnArgs:
		args := req.(rpc.TxnArgs)
		return kv.applyTxn(args)
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

func (kv *KVServer) Txn(args *rpc.TxnArgs, reply *rpc.TxnReply) {
	err, rep := kv.rsm.Submit(*args)

	if err == rpc.ErrWrongLeader || rep == nil {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	txnReply := rep.(rpc.TxnReply)
	reply.Err = txnReply.Err
	reply.Succeeded = txnReply.Succeeded
	reply.Responses = txnReply.Responses
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
	labgob.Register(rpc.TxnArgs{})
	labgob.Register(rpc.TxnCompare{})
	labgob.Register(rpc.TxnOp{})
	labgob.Register(rpc.PutReply{})
	labgob.Register(rpc.GetReply{})
	labgob.Register(rpc.TxnReply{})
	labgob.Register(rpc.TxnOpResponse{})

	kv := &KVServer{
		me:           me,
		versionedMap: make(map[string]VersionedValue),
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	return []tester.IService{kv, kv.rsm.Raft()}
}
