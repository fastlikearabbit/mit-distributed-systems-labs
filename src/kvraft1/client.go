package kvraft

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int
}

type Txn struct {
	ck   *Clerk
	args rpc.TxnArgs
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers, leader: 0}
	return ck
}

func (ck *Clerk) Txn() *Txn {
	return &Txn{ck: ck}
}

func (txn *Txn) If(cmps ...rpc.TxnCompare) *Txn {
	txn.args.Compare = append(txn.args.Compare, cmps...)
	return txn
}

func (txn *Txn) Then(ops ...rpc.TxnOp) *Txn {
	txn.args.Then = append(txn.args.Then, ops...)
	return txn
}

func (txn *Txn) Else(ops ...rpc.TxnOp) *Txn {
	txn.args.Else = append(txn.args.Else, ops...)
	return txn
}

func (txn *Txn) Commit() (rpc.TxnReply, rpc.Err) {
	return txn.ck.CommitTxn(txn.args)
}

func (ck *Clerk) CommitTxn(args rpc.TxnArgs) (rpc.TxnReply, rpc.Err) {
	triedAllServers := false

	for {
		reply := rpc.TxnReply{}
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Txn", &args, &reply)

		if ok && reply.Err != rpc.ErrWrongLeader {
			return reply, reply.Err
		}

		oldLeader := ck.leader
		ck.leader = (ck.leader + 1) % len(ck.servers)

		if ck.leader == 0 || (triedAllServers && ck.leader <= oldLeader) {
			triedAllServers = true
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	triedAllServers := false

	for {
		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Get", &args, &reply)

		if ok && reply.Err == rpc.OK {
			return reply.Value, reply.Version, rpc.OK
		}

		if ok && reply.Err == rpc.ErrNoKey {
			return "", 0, rpc.ErrNoKey
		}

		oldLeader := ck.leader
		ck.leader = (ck.leader + 1) % len(ck.servers)

		if ck.leader == 0 || (triedAllServers && ck.leader <= oldLeader) {
			triedAllServers = true
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.TxnArgs{
		Compare: []rpc.TxnCompare{rpc.CmpVersion(key, rpc.TxnCompareEqual, version)},
		Then:    []rpc.TxnOp{rpc.OpPut(key, value)},
		Else:    []rpc.TxnOp{rpc.OpGet(key)},
	}
	firstAttempt := true
	triedAllServers := false

	for {
		reply := rpc.TxnReply{}
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Txn", &args, &reply)

		if ok {
			if reply.Err == rpc.OK && reply.Succeeded {
				return rpc.OK
			}
			if reply.Err == rpc.OK && !reply.Succeeded {
				if len(reply.Responses) > 0 && reply.Responses[0].Err == rpc.ErrNoKey {
					return rpc.ErrNoKey
				}
				if firstAttempt {
					return rpc.ErrVersion
				}
				return rpc.ErrMaybe
			}
			if reply.Err == rpc.ErrTxn {
				return rpc.ErrTxn
			}
		}

		firstAttempt = false
		oldLeader := ck.leader
		ck.leader = (ck.leader + 1) % len(ck.servers)

		if ck.leader == 0 || (triedAllServers && ck.leader <= oldLeader) {
			triedAllServers = true
			time.Sleep(10 * time.Millisecond)
		}
	}
}
