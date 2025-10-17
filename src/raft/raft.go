package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	Follower = iota
	Candidate
	Leader
)

const (
	ElectionTimeoutLowerBound = 300
	ElectionTimeoutUpperBound = 1000
	HeartBeatTimeout          = 30
)

// log entry structure
type Log struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []Log

	state         int
	lastHeartBeat time.Time

	commitIndex int
	lastApplied int

	// just on leader
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.currentTerm) != nil ||
		d.Decode(&rf.votedFor) != nil ||
		d.Decode(&rf.log) != nil {
		log.Fatalf("Server %v %p (Term: %v) readPersist error", rf.me, rf, rf.currentTerm)
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

/* AppendEntries RPC handler */
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	// convert to follower
	rf.currentTerm = args.Term
	rf.convertState(Follower)
	rf.persist()

	// check previous log index
	if args.PrevLogIndex > len(rf.log) {
		reply.Term, reply.Success = rf.currentTerm, false
		reply.ConflictTerm, reply.ConflictIndex = 0, len(rf.log)+1
		return
	}
	if args.PrevLogIndex >= 1 && args.PrevLogTerm != rf.log[args.PrevLogIndex-1].Term {
		reply.Term, reply.Success = rf.currentTerm, false
		conflictIndex := args.PrevLogIndex
		for conflictIndex >= 1 && rf.log[conflictIndex-1].Term == rf.log[args.PrevLogIndex-1].Term {
			conflictIndex--
		}
		reply.ConflictTerm, reply.ConflictIndex = rf.log[args.PrevLogIndex-1].Term, conflictIndex+1
		// !!!! delete the conflict entries
		rf.log = rf.log[:args.PrevLogIndex-1]
		rf.persist()
		return
	}

	// append entries to log
	rf.log = append(rf.log[:args.PrevLogIndex], args.Entries...)
	reply.Term, reply.Success = rf.currentTerm, true

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log))
		rf.applyCond.Signal()
	}
	rf.persist()
}

/* RequestVote RPC handler */
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		// convert to follower
		rf.currentTerm = args.Term
		rf.convertState(Follower)
		rf.persist()
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		UpToDate(args.LastLogTerm, args.LastLogIndex, rf.getLastLogTerm(), len(rf.log)) {
		rf.votedFor = args.CandidateId
		rf.resetTimer()
		rf.persist()
		reply.Term, reply.VoteGranted = rf.currentTerm, true
	} else {
		rf.votedFor = -1
		rf.persist()
		reply.Term, reply.VoteGranted = rf.currentTerm, false
	}
}

/* RequestVote RPC sender */
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// hold lock first !!!
func (rf *Raft) broadcastRequestVote() {
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log),
		LastLogTerm:  rf.getLastLogTerm(),
	}

	votedNum := 1
	for id := range rf.peers {
		if id != rf.me {
			go func(server int) {
				reply := &RequestVoteReply{}
				if rf.sendRequestVote(server, args, reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.state != Candidate {
						return
					}
					if rf.currentTerm == args.Term {
						if reply.VoteGranted {
							votedNum++
							if votedNum > len(rf.peers)/2 {
								rf.convertState(Leader)
							}
						} else {
							if reply.Term > rf.currentTerm {
								rf.currentTerm = reply.Term
								rf.convertState(Follower)
								rf.persist()
							}
						}
					}
				}
			}(id)
		}
	}
}

/* AppendEntries RPC sender */
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// hold lock first !!!
func (rf *Raft) broadcastHeartBeat() {
	for id := range rf.peers {
		if id != rf.me {
			go func(server int) {
				reply := &AppendEntriesReply{}
				rf.mu.Lock()
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					LeaderCommit: rf.commitIndex,
					PrevLogIndex: 0,
					PrevLogTerm:  0,
				}
				if rf.nextIndex[server] != 0 {
					args.PrevLogIndex = rf.nextIndex[server] - 1
				}
				if args.PrevLogIndex > 0 {
					args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
				}
				if rf.nextIndex[server] >= 1 && rf.nextIndex[server] <= len(rf.log) {
					args.Entries = rf.log[rf.nextIndex[server]-1:]
				}
				rf.mu.Unlock()

				if rf.sendAppendEntries(server, args, reply) {
					rf.mu.Lock()
					if rf.state == Leader && args.Term == rf.currentTerm {
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.convertState(Follower)
							rf.persist()
						} else {
							if reply.Success {
								if len(args.Entries) > 0 {
									rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
									rf.matchIndex[server] = rf.nextIndex[server] - 1
								}
								rf.updateCommitIndex()
							} else {
								rf.nextIndex[server] = reply.ConflictIndex
							}
						}
					}
					rf.mu.Unlock()
				}
			}(id)
		}
	}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = (rf.state == Leader)

	if !isLeader || rf.killed() {
		return index, term, false
	}

	rf.log = append(rf.log, Log{command, rf.currentTerm})
	index = len(rf.log)
	rf.persist()

	// fmt.Printf("[Start] server %d start to append command %v index = %d\n", rf.me, command, index)

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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// hold lock first
func (rf *Raft) updateCommitIndex() {
	for N := len(rf.log); N >= rf.commitIndex+1 && rf.log[N-1].Term == rf.currentTerm; N-- {
		count := 1
		for id, _ := range rf.peers {
			if id != rf.me && rf.matchIndex[id] >= N {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			rf.applyCond.Signal()
			break
		}
	}
}

func (rf *Raft) applyCommittedLog() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		if rf.lastApplied < rf.commitIndex {
			msgs := make([]ApplyMsg, 0)
			for i := rf.lastApplied; i < rf.commitIndex; i++ {
				msgs = append(msgs, ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i].Command,
					CommandIndex: i + 1,
				})
			}
			for _, msg := range msgs {
				rf.applyCh <- msg
			}
			rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()

		// Your code here (3A)
		// Check if a leader election should be started.

		if rf.state == Follower {
			if time.Since(rf.lastHeartBeat) > time.Duration(getElectionTimeout())*time.Millisecond {
				rf.convertState(Candidate)
				rf.broadcastRequestVote()
			}
		} else if rf.state == Candidate {
			if time.Since(rf.lastHeartBeat) > time.Duration(getElectionTimeout())*time.Millisecond {
				rf.resetTimer()
				rf.broadcastRequestVote()
			}
		} else {
			rf.broadcastHeartBeat()
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 100
		// milliseconds.
		ms := 50 + (rand.Int63() % 50)
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.resetTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applyCommittedLog()

	return rf
}

/* utils */

func getElectionTimeout() int64 {
	return ElectionTimeoutLowerBound + (rand.Int63() % (ElectionTimeoutUpperBound - ElectionTimeoutLowerBound))
}

func UpToDate(llTermC int, llIndexC int, llTermR int, llIndexR int) bool {
	return llTermC > llTermR || (llTermC == llTermR && llIndexC >= llIndexR)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

/* helper functions */

func (rf *Raft) resetTimer() {
	rf.lastHeartBeat = time.Now()
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) != 0 {
		return rf.log[len(rf.log)-1].Term
	}
	return 0
}

func (rf *Raft) initializeLeaderState() {
	for id, _ := range rf.peers {
		if id != rf.me {
			rf.nextIndex[id] = len(rf.log) + 1
			rf.matchIndex[id] = 0
		}
	}
}

func (rf *Raft) convertState(state int) {
	rf.resetTimer()
	if state == rf.state {
		return
	}

	rf.state = state
	if state == Follower {
		rf.votedFor = -1
		rf.persist()
	} else if state == Candidate {
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.persist()
	} else {
		rf.initializeLeaderState()
	}
}
