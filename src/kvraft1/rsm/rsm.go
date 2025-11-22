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
	waitingCommitedOps map[uint64]chan CommitedOp
	done               int32
	persister          *tester.Persister
	lastApplied        int // track last applied index for snapshotting
}

func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:                 me,
		maxraftstate:       maxraftstate,
		applyCh:            make(chan raftapi.ApplyMsg),
		sm:                 sm,
		waitingCommitedOps: make(map[uint64]chan CommitedOp),
		done:               0,
		persister:          persister,
		lastApplied:        0,
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	snap := persister.ReadSnapshot()
	if len(snap) > 0 {
		sm.Restore(snap)
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

	ch := make(chan CommitedOp, 1)
	rsm.waitingCommitedOps[op.Id] = ch

	rf := rsm.Raft()
	entryIndex, entryTerm, isLeader := rf.Start(op)

	if !isLeader {
		delete(rsm.waitingCommitedOps, op.Id)
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	rsm.mu.Unlock()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for !rsm.Done() {
		select {
		case commitedOp := <-ch:
			rsm.mu.Lock()
			delete(rsm.waitingCommitedOps, op.Id)
			rsm.mu.Unlock()

			if commitedOp.Index == entryIndex {
				return rpc.OK, commitedOp.Res
			}
			return rpc.ErrWrongLeader, nil

		case <-ticker.C:
			currentTerm, stillLeader := rf.GetState()
			if !stillLeader || currentTerm != entryTerm {
				rsm.mu.Lock()
				delete(rsm.waitingCommitedOps, op.Id)
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
		}
	}

	rsm.mu.Lock()
	delete(rsm.waitingCommitedOps, op.Id)
	rsm.mu.Unlock()
	return rpc.ErrWrongLeader, nil
}

func (rsm *RSM) reader() {
	for {
		applyMsg, ok := <-rsm.applyCh
		if !ok {
			atomic.StoreInt32(&rsm.done, 1)
			return
		}

		rsm.mu.Lock()

		if applyMsg.SnapshotValid {
			if applyMsg.SnapshotIndex > rsm.lastApplied {
				rsm.sm.Restore(applyMsg.Snapshot)
				rsm.lastApplied = applyMsg.SnapshotIndex
			}
			rsm.mu.Unlock()
			continue
		}

		if !applyMsg.CommandValid {
			rsm.mu.Unlock()
			continue
		}

		if applyMsg.CommandIndex <= rsm.lastApplied {
			rsm.mu.Unlock()
			continue
		}

		op := applyMsg.Command.(Op)

		res := rsm.sm.DoOp(op.Req)

		rsm.lastApplied = applyMsg.CommandIndex

		if ch, ok := rsm.waitingCommitedOps[op.Id]; ok {
			select {
			case ch <- CommitedOp{
				Me:    op.Me,
				Index: applyMsg.CommandIndex,
				Res:   res,
			}:
			default:
			}
		}

		if rsm.maxraftstate > 0 && rsm.Raft().PersistBytes() > rsm.maxraftstate {
			rsm.takeSnapshot(applyMsg.CommandIndex)
		}

		rsm.mu.Unlock()
	}
}

func (rsm *RSM) takeSnapshot(index int) {
	snapshot := rsm.sm.Snapshot()
	rsm.Raft().Snapshot(index, snapshot)
}
