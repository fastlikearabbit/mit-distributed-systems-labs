package rsm

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	Me  int
	Id  uint64
	Req any
}

type CommitedOp struct {
	Me    int
	Index int
	Res   any
}

var idCounter uint64 = 0

func GenerateId() uint64 {
	return atomic.AddUint64(&idCounter, 1)
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu                 sync.Mutex
	me                 int
	rf                 raftapi.Raft
	applyCh            chan raftapi.ApplyMsg
	maxraftstate       int // snapshot if log grows this big
	sm                 StateMachine
	waitingCommitedOps map[uint64]*CommitedOp
	done               int32
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:                 me,
		maxraftstate:       maxraftstate,
		applyCh:            make(chan raftapi.ApplyMsg),
		sm:                 sm,
		waitingCommitedOps: make(map[uint64]*CommitedOp),
		done:               0,
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	go rsm.reader()

	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) Done() bool {
	z := atomic.LoadInt32(&rsm.done)
	return z == 1
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader, and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	rsm.mu.Lock()
	op := Op{
		Me:  rsm.me,
		Id:  GenerateId(),
		Req: req,
	}

	rf := rsm.Raft()
	entryIndex, entryTerm, isLeader := rf.Start(op)

	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	rsm.mu.Unlock()

	for !rsm.Done() {
		rsm.mu.Lock()
		if rsm.waitingCommitedOps[op.Id] != nil {
			commitedOp := rsm.waitingCommitedOps[op.Id]

			if commitedOp.Me != op.Me && commitedOp.Index != entryIndex {
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
			rsm.mu.Unlock()
			return rpc.OK, commitedOp.Res
		}

		currentTerm, _ := rf.GetState()

		if currentTerm != entryTerm {
			rsm.mu.Unlock()
			return rpc.ErrWrongLeader, nil
		}
		rsm.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	return rpc.ErrWrongLeader, nil
}

func (rsm *RSM) reader() {
	for {
		select {
		case applyMsg, ok := <-rsm.applyCh:
			if !ok {
				atomic.StoreInt32(&rsm.done, 1)
				return
			}
			op := applyMsg.Command.(Op)
			res := rsm.sm.DoOp(op.Req)

			rsm.mu.Lock()
			rsm.waitingCommitedOps[op.Id] = &CommitedOp{
				Me:    op.Me,
				Index: applyMsg.CommandIndex,
				Res:   res,
			}
			rsm.mu.Unlock()
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}
