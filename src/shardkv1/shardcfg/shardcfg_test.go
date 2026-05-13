package shardcfg

import (
	"testing"

	"6.5840/tester1"
)

func check_same_config(t *testing.T, c1 ShardConfig, c2 ShardConfig) {
	if c1.Num != c2.Num {
		t.Fatalf("Num wrong")
	}
	if c1.Shards != c2.Shards {
		t.Fatalf("Shards wrong")
	}
	if len(c1.Groups) != len(c2.Groups) {
		t.Fatalf("number of Groups is wrong")
	}
	for gid, sa := range c1.Groups {
		sa1, ok := c2.Groups[gid]
		if ok == false || len(sa1) != len(sa) {
			t.Fatalf("len(Groups) wrong")
		}
		if ok && len(sa1) == len(sa) {
			for j := 0; j < len(sa); j++ {
				if sa[j] != sa1[j] {
					t.Fatalf("Groups wrong")
				}
			}
		}
	}
}

func TestBasic(t *testing.T) {
	const (
		Gid1 = 1
		Gid2 = 2
	)
	cfg := MakeShardConfig()
	cfg.CheckConfig(t, []tester.Tgid{})

	cfg.JoinBalance(map[tester.Tgid][]string{Gid1: []string{"x", "y", "z"}})
	cfg.CheckConfig(t, []tester.Tgid{Gid1})

	cfg.JoinBalance(map[tester.Tgid][]string{Gid2: []string{"a", "b", "c"}})
	cfg.CheckConfig(t, []tester.Tgid{Gid1, Gid2})

	sa1 := cfg.Groups[Gid1]
	if len(sa1) != 3 || sa1[0] != "x" || sa1[1] != "y" || sa1[2] != "z" {
		t.Fatalf("wrong servers for gid %v: %v\n", Gid1, sa1)
	}
	sa2 := cfg.Groups[Gid2]
	if len(sa2) != 3 || sa2[0] != "a" || sa2[1] != "b" || sa2[2] != "c" {
		t.Fatalf("wrong servers for gid %v: %v\n", Gid2, sa2)
	}

	cfg.LeaveBalance([]tester.Tgid{Gid1})
	cfg.CheckConfig(t, []tester.Tgid{Gid2})

	cfg.LeaveBalance([]tester.Tgid{Gid2})
	cfg.CheckConfig(t, []tester.Tgid{})
}

func TestConsistentHashRebalance(t *testing.T) {
	const (
		Gid1 = 1
		Gid2 = 2
		Gid3 = 3
	)

	cfg := MakeShardConfig()
	cfg.JoinBalance(map[tester.Tgid][]string{Gid1: []string{"a"}})
	oneGroup := cfg.Copy()

	cfg.JoinBalance(map[tester.Tgid][]string{Gid2: []string{"b"}})
	cfg.CheckConfig(t, []tester.Tgid{Gid1, Gid2})

	moved := 0
	for shard, oldGid := range oneGroup.Shards {
		if cfg.Shards[shard] != oldGid {
			moved++
			if cfg.Shards[shard] != Gid2 {
				t.Fatalf("shard %d moved from %d to %d, not to the joining group", shard, oldGid, cfg.Shards[shard])
			}
		}
	}
	if moved == 0 || moved == NShards {
		t.Fatalf("join moved %d shards out of %d; expected a non-trivial subset", moved, NShards)
	}

	twoGroups := cfg.Copy()
	cfg.JoinBalance(map[tester.Tgid][]string{Gid3: []string{"c"}})
	cfg.CheckConfig(t, []tester.Tgid{Gid1, Gid2, Gid3})
	for shard, oldGid := range twoGroups.Shards {
		if cfg.Shards[shard] != oldGid && cfg.Shards[shard] != Gid3 {
			t.Fatalf("shard %d moved from existing group %d to existing group %d on join", shard, oldGid, cfg.Shards[shard])
		}
	}

	cfg.LeaveBalance([]tester.Tgid{Gid2})
	cfg.CheckConfig(t, []tester.Tgid{Gid1, Gid3})
	for shard, gid := range cfg.Shards {
		if gid == Gid2 {
			t.Fatalf("shard %d still assigned to departed group %d", shard, gid)
		}
	}
}
