package shardgrp

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers, leader: 0}

	return ck
}

// In shardgrp/clerk.go

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	triedAllServers := false
	retryCount := 0
	maxRetries := 10

	for {
		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Get", &args, &reply)

		if ok && reply.Err == rpc.OK {
			return reply.Value, reply.Version, rpc.OK
		}

		if ok && reply.Err == rpc.ErrNoKey {
			return "", 0, rpc.ErrNoKey
		}

		if ok && reply.Err == rpc.ErrWrongGroup {
			return "", 0, rpc.ErrWrongGroup
		}

		oldLeader := ck.leader
		ck.leader = (ck.leader + 1) % len(ck.servers)

		if ck.leader == 0 || (triedAllServers && ck.leader <= oldLeader) {
			triedAllServers = true
			retryCount++

			if retryCount >= maxRetries {
				return "", 0, rpc.ErrWrongGroup
			}

			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	firstAttempt := true
	triedAllServers := false
	retryCount := 0
	maxRetries := 10

	for {
		reply := rpc.PutReply{}
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Put", &args, &reply)

		if ok {
			if reply.Err == rpc.ErrWrongGroup {
				return rpc.ErrWrongGroup
			}
			if reply.Err == rpc.OK {
				return rpc.OK
			}
			if reply.Err == rpc.ErrNoKey {
				return rpc.ErrNoKey
			}
			if reply.Err == rpc.ErrVersion {
				if firstAttempt {
					return rpc.ErrVersion
				}
				return rpc.ErrMaybe
			}
		}

		firstAttempt = false
		oldLeader := ck.leader
		ck.leader = (ck.leader + 1) % len(ck.servers)

		if ck.leader == 0 || (triedAllServers && ck.leader <= oldLeader) {
			triedAllServers = true
			retryCount++

			if retryCount >= maxRetries {
				return rpc.ErrWrongGroup
			}

			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	args := shardrpc.FreezeShardArgs{
		Num:   num,
		Shard: s,
	}
	triedAllServers := false
	retryCount := 0
	maxRetries := 10

	for {
		reply := shardrpc.FreezeShardReply{}
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.FreezeShard", &args, &reply)

		if ok && reply.Err == rpc.OK {
			return reply.State, rpc.OK
		}

		if ok && reply.Err == rpc.ErrWrongGroup {
			return nil, rpc.ErrWrongGroup
		}

		oldLeader := ck.leader
		ck.leader = (ck.leader + 1) % len(ck.servers)

		if ck.leader == 0 || (triedAllServers && ck.leader <= oldLeader) {
			triedAllServers = true
			retryCount++

			if retryCount >= maxRetries {
				return nil, rpc.ErrWrongGroup
			}

			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.InstallShardArgs{
		Num:   num,
		Shard: s,
		State: state,
	}
	triedAllServers := false
	retryCount := 0
	maxRetries := 10

	for {
		reply := shardrpc.InstallShardReply{}
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.InstallShard", &args, &reply)

		if ok && reply.Err == rpc.OK {
			return rpc.OK
		}

		if ok && reply.Err == rpc.ErrWrongGroup {
			return rpc.ErrWrongGroup
		}

		oldLeader := ck.leader
		ck.leader = (ck.leader + 1) % len(ck.servers)

		if ck.leader == 0 || (triedAllServers && ck.leader <= oldLeader) {
			triedAllServers = true
			retryCount++

			if retryCount >= maxRetries {
				return rpc.ErrWrongGroup
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.DeleteShardArgs{
		Num:   num,
		Shard: s,
	}
	triedAllServers := false
	retryCount := 0
	maxRetries := 10

	for {
		reply := shardrpc.DeleteShardReply{}
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.DeleteShard", &args, &reply)

		if ok && reply.Err == rpc.OK {
			return rpc.OK
		}

		if ok && reply.Err == rpc.ErrWrongGroup {
			return rpc.ErrWrongGroup
		}

		oldLeader := ck.leader
		ck.leader = (ck.leader + 1) % len(ck.servers)

		if ck.leader == 0 || (triedAllServers && ck.leader <= oldLeader) {
			triedAllServers = true

			retryCount++

			if retryCount >= maxRetries {
				return rpc.ErrWrongGroup
			}

			time.Sleep(10 * time.Millisecond)
		}
	}
}
