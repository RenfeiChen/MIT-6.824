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
	"labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// States

const (
	LEADER = iota
	FOLLOWER
	CANDIDATE
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Accodinrg to the Figure 2

	// Persistent info for all raft servers
	// Need to write to persister before responding to RPC

	currentTerm int   // The current term ID (initialize with 0)
	votedFor    int   // the candidate ID which this server vote for during this term
	log         []int // the log of the index (the first index of log is 1)

	// Volatile info for all raft servers

	commitIndex int // the last commited index to the log files (init with 0)
	lastApplied int // the last applied index to the RSM (init with 0)

	// Volatile info for Leaders
	// Need to initialize after each election

	nextIndex  []int // index of the next log entry to send to that server
	matchIndex []int // index of highest log entry known to be replicated on server

	// Other info

	state             int           // Leader, Candidate or Follower
	heartbeatInterval time.Duration // interval between sending to hearbeat 200ms

	// channels to receive appendEntries RPC and receive votes requests and leaders

	appendEntriesChannel chan bool // buffered channel to receive appendEntries
	votesRequestsChannel chan bool // buffered channel to receive vote Requests
	leaderChannel        chan bool // buffered channel to know if there is already a leader
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	// Since other goroutines will modify the current term like when we send
	// a heartbeat to RPC and we change the CANDIDATE to FOLLOWER meanwhile
	// we are getting the state, it can be harmful in concurrency.
	// So we need to lock the whole process in the GetState function
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
}

// checkState return if it is the current state and term
// if not it means other goroutines have changed this server's info, so we
// can't continue the process

func (rf *Raft) checkState(state int, term int) bool {
	return rf.state == state && rf.currentTerm == term
}

// converToCandidate changes the current state to CANDIDATE, and increase the
// term since we will start a new election

func (rf *Raft) convertToCandidate() {
	// stateDesc := []string{"LEADER", "FOLLWER", "CANDIDATE"}
	// fmt.Printf("Convert server(%v) state(%v=>CANDIDATE) term(%v)\n", rf.me,
	// 	stateDesc[rf.state], rf.currentTerm+1)
	rf.state = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
}

// converToFollower changes the current state to FOLLOWER and update the stale
// term, and make the votedFor to -1

func (rf *Raft) convertToFollower(term int) {
	// stateDesc := []string{"LEADER", "FOLLWER", "CANDIDATE"}
	// fmt.Printf("Convert server(%v) state(%v=>FOLLOWER) term(%v)\n", rf.me,
	// 	stateDesc[rf.state], rf.currentTerm+1)
	rf.state = FOLLOWER
	rf.currentTerm = term
	rf.votedFor = -1
}

// converToLeader changes the current state to LEADER

func (rf *Raft) convertToLeader() {
	// we need to check the current state since maybe some other goroutines
	// have changed this server's state and it cannot be elected as leader
	if rf.state != CANDIDATE {
		return
	}
	// stateDesc := []string{"LEADER", "FOLLOWER", "CANDIDATE"}
	// fmt.Printf("Convert server(%v) state(%v=>LEADER) term(%v)\n", rf.me,
	// 	stateDesc[rf.state], rf.currentTerm)
	rf.state = LEADER
}

// dropAndSet means if there's something in the channel, we need to pop it out
// and push the new thing into channel, if there's nothing in the channel, we
// can just push it into channel.

// Reason is that since each channel can only receive one RPC at the same time,
// like appendEntries channel, it can only receive one appendEntries from the
// current LEADER, if there is something in the channel, it means the RPC is from
// the old LEADER, and we can't handle it twice(firstly the old LEADER's RPC and
// then the current LEADER's RPC), it is illegal, so we need to pop it out and
// push the current LEADER'S RPC to this channel, and other channels are same.

func dropAndSet(ch chan bool) {
	select {
	case <-ch:
	default:
	}
	ch <- true
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // candidate's term
	CandidateId int // candidate requesting vote
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received the vote
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	// check it for all servers
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	// check if it can vote for the request
	if args.Term == rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		// it has voted other server, so it can't be either CANDIDATE or LEADER
		rf.state = FOLLOWER
		// drop and set the vote channel to notify this server has voted
		dropAndSet(rf.votesRequestsChannel)
		// fmt.Printf("%v vote %v my term:%d, vote term:%d\n", rf.me, args.CandidateId, rf.currentTerm, args.Term)
	}

}

type AppendEntriesArgs struct {
	Term     int // leader's term
	LeaderId int // so follower can redirect clients
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	// need to check before handling the RPC
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}
	if args.Term == rf.currentTerm {
		// it has received the AppendEntries RPC, so update to FOLLOWER
		rf.state = FOLLOWER
		// notify this server has received appendEntries
		dropAndSet(rf.appendEntriesChannel)
		reply.Success = true
	}
	reply.Term = rf.currentTerm

	return
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = FOLLOWER
	rf.heartbeatInterval = time.Millisecond * 50
	rf.appendEntriesChannel = make(chan bool, 1)
	rf.votesRequestsChannel = make(chan bool, 1)
	rf.leaderChannel = make(chan bool, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {

			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()
			electionTimeout := time.Millisecond * time.Duration(300+rand.Intn(100))
			switch state {
			case FOLLOWER:
				select {
				// block until the election time out
				case <-rf.appendEntriesChannel:
				case <-rf.votesRequestsChannel:
				case <-time.After(electionTimeout):
					rf.mu.Lock()
					rf.convertToCandidate()
					rf.mu.Unlock()
				}
			case CANDIDATE:
				// create a goroutine to check if there is a server who has won
				// the vote election
				go rf.startElection()
				select {
				// check if it has received appendEntries or voted for other
				// server or there's already a leader
				case <-rf.appendEntriesChannel:
				case <-rf.votesRequestsChannel:
				case <-rf.leaderChannel:
				case <-time.After(electionTimeout):
					rf.mu.Lock()
					rf.convertToCandidate()
					rf.mu.Unlock()
				}
			case LEADER:
				// send heartbeats to other server
				rf.sendHeartbeats()
				time.Sleep(rf.heartbeatInterval)
			}
		}
	}()
	return rf
}

// startElection is called when a follower becomes a candidate
func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.state != CANDIDATE {
		rf.mu.Unlock()
		return
	}
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
	rf.mu.Unlock()
	// the number of received votes
	var numVoted int32 = 1
	// iterate all the servers to request votes from them
	for i := 0; i < len(rf.peers); i++ {
		// cannot request itself
		if i == rf.me {
			continue
		}
		// we need to do the requestVotes simultaneously so we need to create
		// a goroutine to handle it
		go func(serverId int, args RequestVoteArgs) {

			var reply RequestVoteReply
			if rf.sendRequestVote(serverId, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// check the term, if the CANDIDATE has stale term, it cannot
				// be the LEADER, so just update it to FOLLOWER and return
				if reply.Term > rf.currentTerm {
					rf.convertToFollower(reply.Term)
					return
				}
				// it has been changed in the other goroutines so return
				if !rf.checkState(CANDIDATE, args.Term) {
					return
				}
				// received vote, so plus numVoted
				if reply.VoteGranted {
					atomic.AddInt32(&numVoted, 1)
				}
				// wins the election, conver to leader and handle the channel
				if atomic.LoadInt32(&numVoted) > int32(len(rf.peers)/2) {
					// fmt.Printf("Server(%d) win vote\n", rf.me)
					rf.convertToLeader()
					dropAndSet(rf.leaderChannel)
				}
			}
		}(i, args)
	}
}

// Send heartbeats to other servers to update
func (rf *Raft) sendHeartbeats() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(serverId int) {
			var reply AppendEntriesReply
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				return
			}
			args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
			rf.mu.Unlock()
			if rf.sendAppendEntries(serverId, &args, &reply) {
				rf.mu.Lock()
				// the leader has stale term, convert to FOLLOWER
				if reply.Term > rf.currentTerm {
					rf.convertToFollower(reply.Term)
					rf.mu.Unlock()
					return
				}
				// the LEADER has been changed to other state
				if !rf.checkState(LEADER, args.Term) {
					rf.mu.Unlock()
					return
				}
				if reply.Success {
					rf.mu.Unlock()
					return
				} else {
					rf.mu.Unlock()
				}
			}
		}(i)
	}
}
