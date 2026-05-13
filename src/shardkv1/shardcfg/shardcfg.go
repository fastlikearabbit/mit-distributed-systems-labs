package shardcfg

import (
	"encoding/json"
	"hash/fnv"
	"log"
	"runtime/debug"
	"slices"
	"sort"
	"strconv"
	"testing"

	"6.5840/tester1"
)

type Tshid int
type Tnum int

const (
	NShards  = 32 // The number of virtual shards.
	NumFirst = Tnum(1)
)

const ringReplicas = 64

const (
	Gid1 = tester.Tgid(1)
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func Key2Shard(key string) Tshid {
	shard := Tshid(hashString(key) % NShards)
	return shard
}

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type ShardConfig struct {
	Num    Tnum                     // config number
	Shards [NShards]tester.Tgid     // shard -> gid
	Groups map[tester.Tgid][]string // gid -> servers[]
}

func MakeShardConfig() *ShardConfig {
	c := &ShardConfig{
		Groups: make(map[tester.Tgid][]string),
	}
	return c
}

func (cfg *ShardConfig) String() string {
	b, err := json.Marshal(cfg)
	if err != nil {
		log.Fatalf("Unmarshall err %v", err)
	}
	return string(b)
}

func FromString(s string) *ShardConfig {
	scfg := &ShardConfig{}
	if err := json.Unmarshal([]byte(s), scfg); err != nil {
		log.Fatalf("Unmarshall err %v", err)
	}
	return scfg
}

func (cfg *ShardConfig) Copy() *ShardConfig {
	c := MakeShardConfig()
	c.Num = cfg.Num
	c.Shards = cfg.Shards
	for k, srvs := range cfg.Groups {
		s := make([]string, len(srvs))
		copy(s, srvs)
		c.Groups[k] = s
	}
	return c
}

func hashString(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

func sortedGroups(groups map[tester.Tgid][]string) []tester.Tgid {
	groupsSorted := make([]tester.Tgid, len(groups))
	i := 0
	for k := range groups {
		groupsSorted[i] = k
		i++
	}
	slices.Sort(groupsSorted)
	return groupsSorted
}

type ringNode struct {
	token uint32
	gid   tester.Tgid
}

func makeRing(groups map[tester.Tgid][]string) []ringNode {
	ring := make([]ringNode, 0, len(groups)*ringReplicas)
	for _, gid := range sortedGroups(groups) {
		for replica := 0; replica < ringReplicas; replica++ {
			token := hashString("gid:" + strconv.Itoa(int(gid)) + ":replica:" + strconv.Itoa(replica))
			ring = append(ring, ringNode{token: token, gid: gid})
		}
	}
	slices.SortFunc(ring, func(a, b ringNode) int {
		if a.token < b.token {
			return -1
		}
		if a.token > b.token {
			return 1
		}
		if a.gid < b.gid {
			return -1
		}
		if a.gid > b.gid {
			return 1
		}
		return 0
	})
	return ring
}

func shardToken(shard int) uint32 {
	return hashString("shard:" + strconv.Itoa(shard))
}

func targetLoads(groups []tester.Tgid) map[tester.Tgid]int {
	targets := make(map[tester.Tgid]int, len(groups))
	base := NShards / len(groups)
	extra := NShards % len(groups)
	for i, gid := range groups {
		targets[gid] = base
		if i < extra {
			targets[gid]++
		}
	}
	return targets
}

func ringStart(ring []ringNode, token uint32) int {
	i := sort.Search(len(ring), func(i int) bool {
		return ring[i].token >= token
	})
	if i == len(ring) {
		return 0
	}
	return i
}

func groupRank(ring []ringNode, token uint32, gid tester.Tgid) int {
	start := ringStart(ring, token)
	seen := make(map[tester.Tgid]bool)
	rank := 0

	for offset := 0; offset < len(ring); offset++ {
		node := ring[(start+offset)%len(ring)]
		if seen[node.gid] {
			continue
		}
		if node.gid == gid {
			return rank
		}
		seen[node.gid] = true
		rank++
	}

	return len(ring)
}

func pickGroup(ring []ringNode, token uint32, counts map[tester.Tgid]int, targets map[tester.Tgid]int) tester.Tgid {
	start := ringStart(ring, token)
	seen := make(map[tester.Tgid]bool, len(targets))

	for offset := 0; offset < len(ring); offset++ {
		node := ring[(start+offset)%len(ring)]
		if seen[node.gid] {
			continue
		}
		seen[node.gid] = true
		if counts[node.gid] < targets[node.gid] {
			return node.gid
		}
	}

	log.Fatalf("no eligible group for shard token %v", token)
	return 0
}

type shardPlacement struct {
	shard int
	token uint32
}

type shardCandidate struct {
	shard shardPlacement
	rank  int
}

// balance assignment of virtual shards to groups with a bounded-load
// consistent hash ring. The ring gives each shard a deterministic preferred
// owner order, and the load targets keep groups within one shard of each other.
func (c *ShardConfig) Rebalance() {
	if len(c.Groups) < 1 {
		for s := range c.Shards {
			c.Shards[s] = 0
		}
		return
	}

	groups := sortedGroups(c.Groups)
	targets := targetLoads(groups)
	ring := makeRing(c.Groups)
	counts := make(map[tester.Tgid]int, len(groups))
	shards := make([]shardPlacement, 0, NShards)
	candidates := make(map[tester.Tgid][]shardCandidate, len(groups))
	assigned := make(map[int]bool, NShards)

	for shard := range c.Shards {
		placement := shardPlacement{
			shard: shard,
			token: shardToken(shard),
		}
		shards = append(shards, placement)

		gid := c.Shards[shard]
		if _, ok := targets[gid]; ok {
			candidates[gid] = append(candidates[gid], shardCandidate{
				shard: placement,
				rank:  groupRank(ring, placement.token, gid),
			})
		}
	}
	slices.SortFunc(shards, func(a, b shardPlacement) int {
		if a.token < b.token {
			return -1
		}
		if a.token > b.token {
			return 1
		}
		return a.shard - b.shard
	})

	for _, gid := range groups {
		groupCandidates := candidates[gid]
		slices.SortFunc(groupCandidates, func(a, b shardCandidate) int {
			if a.rank != b.rank {
				return a.rank - b.rank
			}
			if a.shard.token < b.shard.token {
				return -1
			}
			if a.shard.token > b.shard.token {
				return 1
			}
			return a.shard.shard - b.shard.shard
		})
		for _, candidate := range groupCandidates {
			if counts[gid] >= targets[gid] {
				break
			}
			c.Shards[candidate.shard.shard] = gid
			counts[gid]++
			assigned[candidate.shard.shard] = true
		}
	}

	for _, placement := range shards {
		if assigned[placement.shard] {
			continue
		}
		gid := pickGroup(ring, placement.token, counts, targets)
		c.Shards[placement.shard] = gid
		counts[gid]++
	}
}

func (cfg *ShardConfig) Join(servers map[tester.Tgid][]string) bool {
	changed := false
	for gid, servers := range servers {
		_, ok := cfg.Groups[gid]
		if ok {
			log.Printf("re-Join %v", gid)
			return false
		}
		for xgid, xservers := range cfg.Groups {
			for _, s1 := range xservers {
				for _, s2 := range servers {
					if s1 == s2 {
						log.Fatalf("Join(%v) puts server %v in groups %v and %v", gid, s1, xgid, gid)
					}
				}
			}
		}
		// new GID
		// modify cfg to reflect the Join()
		cfg.Groups[gid] = servers
		changed = true
	}
	if changed == false {
		log.Fatalf("Join but no change")
	}
	cfg.Num += 1
	return true
}

func (cfg *ShardConfig) Leave(gids []tester.Tgid) bool {
	changed := false
	for _, gid := range gids {
		_, ok := cfg.Groups[gid]
		if ok == false {
			// already no GID!
			log.Printf("Leave(%v) but not in config", gid)
			return false
		} else {
			// modify op.Config to reflect the Leave()
			delete(cfg.Groups, gid)
			changed = true
		}
	}
	if changed == false {
		debug.PrintStack()
		log.Fatalf("Leave but no change")
	}
	cfg.Num += 1
	return true
}

func (cfg *ShardConfig) JoinBalance(servers map[tester.Tgid][]string) bool {
	if !cfg.Join(servers) {
		return false
	}
	cfg.Rebalance()
	return true
}

func (cfg *ShardConfig) LeaveBalance(gids []tester.Tgid) bool {
	if !cfg.Leave(gids) {
		return false
	}
	cfg.Rebalance()
	return true
}

func (cfg *ShardConfig) GidServers(sh Tshid) (tester.Tgid, []string, bool) {
	gid := cfg.Shards[sh]
	srvs, ok := cfg.Groups[gid]
	return gid, srvs, ok
}

func (cfg *ShardConfig) IsMember(gid tester.Tgid) bool {
	for _, g := range cfg.Shards {
		if g == gid {
			return true
		}
	}
	return false
}

func (cfg *ShardConfig) CheckConfig(t *testing.T, groups []tester.Tgid) {
	if len(cfg.Groups) != len(groups) {
		fatalf(t, "wanted %v groups, got %v", len(groups), len(cfg.Groups))
	}

	// are the groups as expected?
	for _, g := range groups {
		_, ok := cfg.Groups[g]
		if ok != true {
			fatalf(t, "missing group %v", g)
		}
	}

	// any un-allocated shards?
	if len(groups) > 0 {
		for s, g := range cfg.Shards {
			_, ok := cfg.Groups[g]
			if ok == false {
				fatalf(t, "shard %v -> invalid group %v", s, g)
			}
		}
	}

	// more or less balanced sharding?
	counts := map[tester.Tgid]int{}
	for _, g := range cfg.Shards {
		counts[g] += 1
	}
	min := 257
	max := 0
	for g, _ := range cfg.Groups {
		if counts[g] > max {
			max = counts[g]
		}
		if counts[g] < min {
			min = counts[g]
		}
	}
	if max > min+1 {
		fatalf(t, "max %v too much larger than min %v", max, min)
	}
}

func fatalf(t *testing.T, format string, args ...any) {
	debug.PrintStack()
	t.Fatalf(format, args...)
}
