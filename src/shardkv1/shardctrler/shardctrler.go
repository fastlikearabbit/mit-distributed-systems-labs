package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"sync"

	"6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	cfgId string
	mu    sync.Mutex
}

type shardMove struct {
	fromGid tester.Tgid
	toGid   tester.Tgid
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	sck.cfgId = "cfgId"
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	sck.IKVClerk.Put(sck.cfgId, cfg.String(), 0)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	for {
		cfgString, ver, err := sck.IKVClerk.Get(sck.cfgId)
		if err != rpc.OK {
			continue
		}

		cur := shardcfg.FromString(cfgString)
		if cur.Num >= new.Num {
			return
		}

		shardsToMove := make(map[int]shardMove)
		for shardNum := 0; shardNum < shardcfg.NShards; shardNum++ {
			oldGid := cur.Shards[shardNum]
			newGid := new.Shards[shardNum]
			if oldGid != newGid {
				shardsToMove[shardNum] = shardMove{
					fromGid: oldGid,
					toGid:   newGid,
				}
			}
		}

		frozenShards := make(map[int][]byte)
		for len(frozenShards) < len(shardsToMove) {
			for shardNum, move := range shardsToMove {
				if _, alreadyFrozen := frozenShards[shardNum]; alreadyFrozen {
					continue
				}
				ck := shardgrp.MakeClerk(sck.clnt, cur.Groups[move.fromGid])
				data, err := ck.FreezeShard(shardcfg.Tshid(shardNum), new.Num)
				if err == rpc.OK {
					frozenShards[shardNum] = data
				}
			}
		}

		installedShards := make(map[int]bool)
		for len(installedShards) < len(shardsToMove) {
			for shardNum, move := range shardsToMove {
				if installedShards[shardNum] {
					continue
				}
				ck := shardgrp.MakeClerk(sck.clnt, new.Groups[move.toGid])
				err := ck.InstallShard(shardcfg.Tshid(shardNum), frozenShards[shardNum], new.Num)
				if err == rpc.OK {
					installedShards[shardNum] = true
				}
			}
		}

		deletedShards := make(map[int]bool)
		for len(deletedShards) < len(shardsToMove) {
			for shardNum, move := range shardsToMove {
				if deletedShards[shardNum] {
					continue
				}
				ck := shardgrp.MakeClerk(sck.clnt, cur.Groups[move.fromGid])
				err := ck.DeleteShard(shardcfg.Tshid(shardNum), new.Num)
				if err == rpc.OK {
					deletedShards[shardNum] = true
				}
			}
		}
		putErr := sck.IKVClerk.Put(sck.cfgId, new.String(), ver)
		if putErr != rpc.OK {
			continue
		}
	}
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	cfg, _, err := sck.IKVClerk.Get(sck.cfgId)
	if err != rpc.OK {
		return nil
	}
	return shardcfg.FromString(cfg)
}
