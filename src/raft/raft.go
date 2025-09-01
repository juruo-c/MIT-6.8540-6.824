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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	// "fmt"

	//	"6.5840/labgob"
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
	ElectionTimeoutLowerBound = 1000
	ElectionTimeoutUpperBound = 3000
	HeartBeatTimeout = 100
)

// log entry structure
type Log struct {
	Command		interface{}
	Term		int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor 	int
	log			[]Log
	
	state			int
	elapseTime  	int
	electionTimeout int

	// use when state is candidate
	votedNum		int

	// lock
	mutex			sync.Mutex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
	Term 			int
	LeaderId		int
	PrevLogIndex	int
	PrevLogTerm		int
	Entries			[]Log
	LeaderCommit	int
}

type AppendEntriesReply struct {
	Term  			int
	Success 		bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	if len(args.Entries) == 0 {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1

		rf.elapseTime = 0
		reply.Success = true
		reply.Term = rf.currentTerm
		return
	}

	// TODO
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term			int
	VoteGranted		bool
}

// log up-to-date checking
func UpToDate(llTermC int, llIndexC int, llTermR int, llIndexR int) bool {
	if llTermC < llTermR {
		return false
	}
	if llTermC > llTermR {
		return true
	}
	return llIndexC >= llIndexR
}

func (rf *Raft) getLastLogTerm() int{
	if len(rf.log) != 0 {
		return rf.log[len(rf.log) - 1].Term
	}
	return 0
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mutex.Lock()
	defer rf.mutex.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		reply.Term = rf.currentTerm
	}

	rf.elapseTime = 0

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && 
	UpToDate(args.LastLogTerm, args.LastLogIndex, rf.getLastLogTerm(), len(rf.log) - 1) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
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

func (rf *Raft) sendRequestVoteAndCollectReply(server int) {
	rf.mutex.Lock()
	args := RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm: rf.getLastLogTerm(),
	}
	rf.mutex.Unlock()

	reply := RequestVoteReply{}
	for !rf.sendRequestVote(server, &args, &reply) {
		time.Sleep(10 * time.Millisecond)
	}

	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	if reply.VoteGranted {
		rf.votedNum ++
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
		}
	}
}

func (rf *Raft) boardRequestVote() {
	for id, _ := range rf.peers {
		if id != rf.me {
			go rf.sendRequestVoteAndCollectReply(id)
		}
	}
}

func (rf *Raft) startNewTerm() {
	rf.mutex.Lock()
	rf.currentTerm ++
	rf.votedNum = 1
	rf.votedFor = rf.me
	rf.elapseTime = 0
	rf.mutex.Unlock()

	rf.boardRequestVote()
}

func (rf *Raft) sendHeartBeat(server int) {
	rf.mutex.Lock()
	args := AppendEntriesArgs{
		Term: rf.currentTerm,
		LeaderId: rf.me,
	}
	rf.mutex.Unlock()

	reply := AppendEntriesReply{}
	for !rf.peers[server].Call("Raft.AppendEntries", &args, &reply) {
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) boardHeartBeat() {
	rf.mutex.Lock()
	rf.elapseTime = 0
	rf.mutex.Unlock()
	for id, _ := range rf.peers {
		if id != rf.me {
			go rf.sendHeartBeat(id)
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

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mutex.Lock()
		state := rf.state
		elapseTime := rf.elapseTime
		rf.mutex.Unlock()

		// Your code here (3A)
		// Check if a leader election should be started.
		switch (state) {
		case Follower:
			if elapseTime >= rf.electionTimeout {
				rf.mutex.Lock()
				rf.state = Candidate
				rf.mutex.Unlock()
				rf.startNewTerm()
			}
		case Candidate:
			rf.mutex.Lock()
			winElection := rf.votedNum >= (len(rf.peers) + 1) / 2
			rf.mutex.Unlock()
			// fmt.Printf("server %d: votedNum = %d\n", rf.me, rf.votedNum)
			if winElection { // win the election
				rf.mutex.Lock()
				rf.state = Leader
				// fmt.Printf("server %d: become leader\n", rf.me)
				rf.mutex.Unlock()
				rf.boardHeartBeat()
			} else if elapseTime >= rf.electionTimeout {
				rf.startNewTerm()
			}
		case Leader:
			if elapseTime >= HeartBeatTimeout {
				rf.boardHeartBeat()
			}
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		rf.mutex.Lock()
		rf.elapseTime += int(ms)
		rf.mutex.Unlock()
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func getElectionTimeout() int64 {
	return ElectionTimeoutLowerBound + (rand.Int63() % ElectionTimeoutUpperBound)
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

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.electionTimeout = int(getElectionTimeout())
	rf.elapseTime = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
