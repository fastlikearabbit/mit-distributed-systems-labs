package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

const HeartbeatInterval = 100 * time.Millisecond

type State string

const (
	Follower  State = "follower"
	Candidate       = "candidate"
	Leader          = "leader"
)

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state State

	// persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	lastPing        time.Time
	electionTimeout time.Duration

	applyCh            chan raftapi.ApplyMsg
	applyCommitCondVar *sync.Cond
}

func (rf *Raft) GetLastLogEntry() LogEntry {
	return rf.log[len(rf.log)-1]
}

func (rf *Raft) GetPrevLogEntry() LogEntry {
	if len(rf.log) <= 1 {
		return rf.log[0]
	}
	return rf.log[len(rf.log)-2]
}

// ----------------------------------------------------------------
// make sure to hold rf.mu while calling the conversion functions

func (rf *Raft) ConvertToFollower(newTerm int) {
	rf.state = Follower
	rf.currentTerm = newTerm
	rf.votedFor = -1
}

func (rf *Raft) ConvertToCandidate() {
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
}

func (rf *Raft) ConvertToLeader() {
	rf.state = Leader

	lastLogIndex := rf.GetLastLogEntry().Index
	for server := range rf.peers {
		rf.nextIndex[server] = lastLogIndex + 1
		rf.matchIndex[server] = 0
	}
}

// ---------------------------------------------------------------

func GetRandElectionTimeout() time.Duration {
	ms := 300 + rand.Int63()%300
	return time.Duration(ms) * time.Millisecond
}

func (rf *Raft) ResetElectionTimer() {
	rf.lastPing = time.Now()
	rf.electionTimeout = GetRandElectionTimeout()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.state == Leader

	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil || e.Encode(rf.votedFor) != nil || e.Encode(rf.log) != nil {
		log.Fatal("error encoding persisted raft state")
	}
	raftState := w.Bytes()
	rf.persister.Save(raftState, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var rflog []LogEntry

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&rflog) != nil {
		log.Fatal("error decoding persisted raft state")
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = rflog
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLogEntry := rf.GetLastLogEntry()

	if args.Term > rf.currentTerm {
		rf.ConvertToFollower(args.Term)
		rf.persist()
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	isViableCandidate := (args.LastLogTerm > lastLogEntry.Term) ||
		(args.LastLogTerm == lastLogEntry.Term && args.LastLogIndex >= lastLogEntry.Index)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && isViableCandidate {
		rf.ResetElectionTimer()
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.persist()
	}
	reply.Term = rf.currentTerm
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	XTerm  int
	XIndex int
	XLen   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.ConvertToFollower(args.Term)
		rf.persist()
	}
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	rf.ResetElectionTimer()

	if len(rf.log) <= args.PrevLogIndex {
		reply.Success = false
		reply.XLen = len(rf.log)
		reply.XTerm = -1
		reply.XIndex = -1
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.XTerm = rf.log[args.PrevLogIndex].Term

		reply.XIndex = args.PrevLogIndex
		for reply.XIndex > 0 && rf.log[reply.XIndex-1].Term == reply.XTerm {
			reply.XIndex--
		}
		reply.XLen = len(rf.log)
		return
	}

	rf.ResetElectionTimer()
	reply.Success = true

	if args.Entries != nil && len(args.Entries) > 0 {
		logIndex := args.PrevLogIndex + 1
		newEntriesIndex := 0

		for logIndex < len(rf.log) && newEntriesIndex < len(args.Entries) {
			if rf.log[logIndex].Term != args.Entries[newEntriesIndex].Term {
				break
			}
			logIndex++
			newEntriesIndex++
		}

		if newEntriesIndex < len(args.Entries) {
			rf.log = rf.log[:logIndex]
			rf.log = append(rf.log, args.Entries[newEntriesIndex:]...)
			rf.persist()
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.GetLastLogEntry().Index)
		rf.applyCommitCondVar.Broadcast()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.log)
	term := rf.currentTerm
	isLeader := rf.state == Leader

	if !isLeader {
		return 0, 0, false
	}
	logEntry := LogEntry{
		Term:    term,
		Index:   index,
		Command: command,
	}
	rf.log = append(rf.log, logEntry)
	rf.persist()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() {
	rf.ConvertToCandidate()
	rf.persist()
	rf.ResetElectionTimer()

	lastLogEntry := rf.GetLastLogEntry()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogEntry.Index,
		LastLogTerm:  lastLogEntry.Term,
	}
	rf.mu.Unlock()

	var electionMutex sync.Mutex
	electionCond := sync.NewCond(&electionMutex)

	voteCount := 0
	finishCount := 0

	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		reply := RequestVoteReply{}
		go func(server int) {
			ok := rf.sendRequestVote(server, &args, &reply)
			electionMutex.Lock()
			finishCount += 1
			if !ok {
				electionMutex.Unlock()
				return
			}
			if reply.VoteGranted {
				voteCount += 1
			}

			rf.mu.Lock()
			if reply.Term > args.Term {
				rf.ConvertToFollower(reply.Term)
				rf.persist()
			}
			rf.mu.Unlock()

			electionCond.Broadcast()
			electionMutex.Unlock()
		}(server)
	}

	rf.mu.Lock()
	if rf.state == Follower || rf.currentTerm != args.Term {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	electionMutex.Lock()
	for voteCount < len(rf.peers)/2 && finishCount != len(rf.peers) {
		electionCond.Wait()
	}

	rf.mu.Lock()
	if voteCount >= len(rf.peers)/2 && rf.currentTerm == args.Term {
		electionMutex.Unlock()
		rf.ConvertToLeader()
		go rf.sendHeartBeats()
		return
	}

	rf.mu.Unlock()
	electionMutex.Unlock()
}

func (rf *Raft) sendHeartBeats() {
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader || rf.currentTerm != currentTerm {
			rf.mu.Unlock()
			return
		}

		for server := range rf.peers {
			if server == rf.me {
				continue
			}

			go rf.sendAppendEntriesToFollower(server)
		}
		rf.mu.Unlock()

		time.Sleep(HeartbeatInterval)
	}
}

func (rf *Raft) sendAppendEntriesToFollower(server int) {
	rf.mu.Lock()

	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	prevLogIndex := rf.nextIndex[server] - 1
	if prevLogIndex < 0 || prevLogIndex >= len(rf.log) {
		rf.mu.Unlock()
		return
	}

	prevLogTerm := rf.log[prevLogIndex].Term

	var entriesToSend []LogEntry
	if rf.nextIndex[server] < len(rf.log) {
		entriesToSend = make([]LogEntry, len(rf.log[rf.nextIndex[server]:]))
		copy(entriesToSend, rf.log[rf.nextIndex[server]:])
	}

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entriesToSend,
		LeaderCommit: rf.commitIndex,
	}

	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, &args, &reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader || rf.currentTerm != currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.ConvertToFollower(reply.Term)
		rf.persist()
		return
	}

	if reply.Success {
		if len(args.Entries) > 0 {
			newMatchIndex := args.PrevLogIndex + len(args.Entries)
			if newMatchIndex > rf.matchIndex[server] {
				rf.matchIndex[server] = newMatchIndex
				rf.nextIndex[server] = newMatchIndex + 1
				rf.updateCommitIndex()
			}
		}
	} else {
		if reply.XTerm == -1 {
			rf.nextIndex[server] = reply.XLen
		} else {
			lastIndexOfXTerm := -1
			for i := len(rf.log) - 1; i > 0; i-- {
				if rf.log[i].Term == reply.XTerm {
					lastIndexOfXTerm = i
					break
				}
				if rf.log[i].Term < reply.XTerm {
					break
				}
			}

			if lastIndexOfXTerm != -1 {
				rf.nextIndex[server] = lastIndexOfXTerm + 1
			} else {
				rf.nextIndex[server] = reply.XIndex
			}
		}

		if rf.nextIndex[server] < 1 {
			rf.nextIndex[server] = 1
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	if rf.state != Leader {
		return
	}

	for n := rf.GetLastLogEntry().Index; n > rf.commitIndex; n-- {
		if rf.log[n].Term != rf.currentTerm {
			continue
		}

		replicationCount := 0
		for server := range rf.peers {
			if server != rf.me && rf.matchIndex[server] >= n {
				replicationCount++
			}
		}

		if replicationCount >= len(rf.peers)/2 {
			rf.commitIndex = n
			rf.applyCommitCondVar.Broadcast()
			return
		}
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		if time.Since(rf.lastPing) > rf.electionTimeout && rf.state != Leader {
			go rf.startElection()
		} else {
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCommitCondVar.Wait()
		}
		rf.lastApplied += 1

		entry := rf.log[rf.lastApplied]
		applyMsg := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: entry.Index,
		}
		rf.mu.Unlock()
		rf.applyCh <- applyMsg
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = Follower

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	// dummy entry
	rf.log = append(rf.log, LogEntry{Term: 0, Index: 0, Command: nil})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.lastPing = time.Now()
	rf.electionTimeout = GetRandElectionTimeout()

	rf.applyCh = applyCh
	rf.applyCommitCondVar = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}
