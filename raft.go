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

	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type StateType uint32

const (
	LeaderState    StateType = 0
	FollowerState  StateType = 1
	CandidateState StateType = 2
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state      StateType
	timedOut   bool
	commitChan chan int

	// Persisted
	currentTerm int
	votedFor    int
	log         Log

	// Volatile
	commitIndex int
	lastApplied int

	// Leader's Only, init to 0, increasing monotonically
	matchIndex []int
	// Leader's Only, init to leader's last log index + 1
	nextIndex []int
}

type Log []LogEntry

type LogEntry struct {
	Term  int
	Entry interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = (rf.state == LeaderState)

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Update term for new election if it's higher.
	// Set votedFor to -1 for new term
	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
		rf.revertToFollower()
	}

	// Always set reply to current term
	reply.Term = rf.currentTerm

	// election restriction from Section 5.4
	lastLogTerm := rf.log[len(rf.log)-1].Term
	votedAlready := rf.votedFor != -1

	switch {
	case rf.votedFor == args.CandidateId:
		reply.VoteGranted = true
	case votedAlready:
		reply.VoteGranted = false
	case lastLogTerm > args.LastLogTerm:
		reply.VoteGranted = false
	case lastLogTerm < args.LastLogTerm:
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	case lastLogTerm == args.LastLogTerm:
		if len(rf.log)-1 <= args.LastLogIndex {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		} else {
			reply.VoteGranted = false
		}
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

// On conversion to candidate, start election and do:
//  1. Increment currentTerm
//  2. Vote for self
//  3. Reset election timer (handled in ticker)
//  4. Send request vote RPC
func (rf *Raft) beginElection() {
	// ----------------------v Locked to snapshot args
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me

	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.log)
	args.LastLogTerm = rf.log[len(rf.log)-1].Term
	rf.mu.Unlock()
	// ----------------------^ Locked

	numVotes := 1
	votesNeeded := len(rf.peers)/2 + 1
	votesGathered := 1
	var wg sync.WaitGroup

	handleVotes := func(server int) {
		reply := RequestVoteReply{}
		rf.sendRequestVote(server, &args, &reply)

		// --------------------------------v Locked
		rf.mu.Lock()

		votesGathered++

		if reply.VoteGranted && reply.Term == rf.currentTerm {
			numVotes++
		} else if reply.Term > rf.currentTerm {
			rf.updateTerm(reply.Term)
			rf.revertToFollower()
		}

		if votesGathered == votesNeeded {
			wg.Done()
		}
		rf.mu.Unlock()
		// --------------------------------^ Locked

		// If raft receives an old reply, discard it
		// If it sees a new higher term from follower, convert to follower.
	}

	wg.Add(1)
	rf.sendToPeers(handleVotes)
	waitChan := make(chan bool, 1)

	go func(ch chan bool) {
		wg.Wait()
		waitChan <- true
	}(waitChan)

	select {
	case <-waitChan:
		//proceed
	case <-time.After(600 * time.Millisecond):
		return
	}

	rf.mu.Lock()
	becomeLeaderCond := numVotes >= votesNeeded &&
		rf.currentTerm == args.Term &&
		rf.state == CandidateState

	if becomeLeaderCond {
		rf.becomeLeader()

		// Declare leader by sending heartbeat
		args := AppendEntriesArgs{Term: rf.currentTerm}
		heartbeat := func(server int) {
			reply := AppendEntriesReply{}
			rf.sendAppendEntries(server, &args, &reply)

			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.state = FollowerState
			}
			rf.mu.Unlock()
		}
		rf.sendToPeers(heartbeat)
	}
	rf.mu.Unlock()
}

// Sent by the leader containing it's raft info
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// Reply containins the followers currentTerm, for updating candidate/leader
type AppendEntriesReply struct {
	Term    int
	Success bool

	// For backing up quickly over logs.
	ConflictingTerm int
	LogLength       int
}

// 1. Reply false if term < currentTerm (??5.1)
// 2. Reply false if log doesn???t contain an entry at prevLogIndex
// whose term matches prevLogTerm (??5.3)
// 3. If an existing entry conflicts with a new one (same index
// but different terms), delete the existing entry and all that
// follow it (??5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex =
// min(leaderCommit, index of last new entry)
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// -------------v for updating leader status
	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
		rf.revertToFollower()
	}

	// Always set reply to current term
	reply.Term = rf.currentTerm

	if args.Term == rf.currentTerm && rf.state == CandidateState {
		rf.revertToFollower()
	}
	// -------------^ for updating leader status

	// Step 1.
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// Valid leader.
	rf.timedOut = false

	// Step 2.
	// (If you get an AppendEntries RPC with a prevLogIndex that points beyond the end of your log,
	// you should handle it the same as if you did have that entry but the term did not match --
	// reply false -- this is a special condition of step 2).
	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		reply.LogLength = len(rf.log)
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictingTerm = rf.log[args.PrevLogIndex].Term
		return
	}

	// Step 3.
	for i, entry := range args.Entries {
		entryIndex := args.PrevLogIndex + i + 1
		endLogIndex := len(rf.log) - 1 // Check to avoid indexing out of range
		if entryIndex <= endLogIndex && entry.Term != rf.log[entryIndex].Term {
			rf.log = rf.log[:entryIndex]
			break
		}
	}

	// Only append append the entries and update if they are going in the right spot!
	// This comes up because old rpc's come in that might not truncate the log.
	if len(rf.log)-1 == args.PrevLogIndex {
		reply.Success = true

		// Step 4. Append any entries not in the log
		rf.log = append(rf.log, args.Entries...)

		// Step 5. Only update NEW entry, not possibly incorrect logs ahead of leader.
		if args.LeaderCommit > rf.commitIndex {
			indexLastNewEntry := args.PrevLogIndex + len(args.Entries)
			newCommitIndex := min(args.LeaderCommit, indexLastNewEntry)
			rf.commitIndex = newCommitIndex
			go rf.kickApplyChan(newCommitIndex)
		}
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	replyChan := make(chan bool, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()

	replyChan <- rf.peers[server].Call("Raft.AppendEntries", args, reply)
	select {
	case <-ctx.Done():
		return false
	case ok := <-replyChan:
		return ok
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

	// ----------------------------------------v Locked
	rf.mu.Lock()
	index = len(rf.log)
	term = rf.currentTerm
	isLeader = rf.state == LeaderState

	if isLeader {
		newEntry := LogEntry{Entry: command, Term: term}
		rf.log = append(rf.log, newEntry)
	}

	rf.mu.Unlock()
	// ----------------------------------------^ Locked

	return index, term, isLeader
}

// Sends out AppendEntries to a given server and handles the reply.
// Will keep retrying the server (and blocking) if no response is given quick enough.
func (rf *Raft) sendLogUpdates(server int) {
loop:
	for !rf.killed() {

		rf.mu.Lock()
		isLeader := rf.state == LeaderState
		rf.mu.Unlock()

		if isLeader {
			rf.mu.Lock() // Lock to prevent inconsistency amongst args
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[server] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
				Entries:      rf.log[rf.nextIndex[server]:],
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()

			reply := AppendEntriesReply{Term: 0, Success: false}

			replyChan := make(chan bool, 1)
			timeout := 250 * time.Millisecond
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			go func(ctx context.Context) {
				replyChan <- rf.sendAppendEntries(server, &args, &reply)
			}(ctx)

			select {
			case <-ctx.Done():
				// Server probably unreachable,
				// then keep looping and ignore the reply.
			case <-replyChan:
				// -------------------------------v Locked while handling reply
				rf.mu.Lock()

				if reply.Success && reply.Term == rf.currentTerm {
					// for the purpose of updating commitIndex
					rf.nextIndex[rf.me] = len(rf.log)
					rf.matchIndex[rf.me] = len(rf.log) - 1

					rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
					rf.matchIndex[server] = rf.nextIndex[server] - 1

					// Exit the loop once the rpc call goes through, unlock mu before hand
					// to avoid holding the lock forever
					rf.mu.Unlock()
					break loop

				} else if rf.nextIndex[server] > 1 && rf.nextIndex[server] > rf.matchIndex[server]+1 && reply.Term == rf.currentTerm {
					// decrement nextIndex, retry
					switch {
					case reply.LogLength != 0:
						rf.nextIndex[server] = reply.LogLength
					case reply.ConflictingTerm != 0:
						for i := args.PrevLogIndex; i > rf.matchIndex[server]; i-- {
							if rf.log[i].Term <= reply.ConflictingTerm {
								rf.nextIndex[server] = i + 1
								break
							}
						}
					default:
						rf.nextIndex[server]--
					}
				}
				rf.mu.Unlock()
				// -------------------------------^ Locked
			}
		}
	}
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
	// fmt.Printf("%v, term:%v leader:%v commit:%v, loglength:%v\n", rf.me, rf.currentTerm, rf.state == LeaderState, rf.commitIndex, len(rf.log))

}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// Each valid AE/Heartbeat should set timedOut to false, which is set to True
// after every election timeout.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Check if it's timed out,
		// then reset the timer,
		// then sleep for an election timeout cycle
		// While the election starts, keep election timeout going.

		// 250-500 ms Election Timeout
		randTime := rand.Intn(250)
		time.Sleep(time.Duration(300+randTime) * time.Millisecond)

		rf.mu.Lock()
		if rf.timedOut && rf.state != LeaderState {
			rf.becomeCandidate()
			go rf.beginElection()
		}

		rf.timedOut = true

		rf.mu.Unlock()
		// Reset timer upon valid AE RPC, anytime votedFor is set
	}
}

// For leaders to send out heartbeats periodically
func (rf *Raft) heartbeatLoop() {
	for !rf.killed() {

		rf.mu.Lock()
		if rf.state == LeaderState {
			// send append entries heartbeats every 100ms, forward requests
			heartbeat := func(server int) {
				rf.mu.Lock()
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[server] - 1,
					PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
					Entries:      []LogEntry{},
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()

				reply := AppendEntriesReply{}
				rf.sendAppendEntries(server, &args, &reply)

				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.revertToFollower()
				}
				rf.mu.Unlock()
			}

			rf.sendToPeers(heartbeat)
		}
		rf.mu.Unlock()

		time.Sleep(100 * time.Millisecond)
	}
}

// Leader periodically sends out append entries when follower logs aren't
// up to date with leader's log.
func (rf *Raft) maintainLogsLoop(server int) {
	for !rf.killed() {
		rf.mu.Lock()
		isLeader := rf.state == LeaderState
		rf.mu.Unlock()

		if isLeader {
			rf.mu.Lock()
			cond := len(rf.log)-1 >= rf.nextIndex[server]
			rf.mu.Unlock()

			if cond {
				rf.sendLogUpdates(server)
			}
		}
	}
}

// Use this as a long running go routine to send applyCh messages when entries are committed
func (rf *Raft) applyChRoutine(applyCh chan ApplyMsg) {
	for !rf.killed() {
		<-rf.commitChan

		rf.mu.Lock()
		applyIndex := rf.lastApplied
		commitIndex := rf.commitIndex
		rf.mu.Unlock()

		for applyIndex < commitIndex {
			rf.mu.Lock()
			applyIndex++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[applyIndex].Entry,
				CommandIndex: applyIndex,
			}
			rf.lastApplied++
			rf.mu.Unlock()

			applyCh <- applyMsg
		}
	}
}

func (rf *Raft) commitLoop() {
	for !rf.killed() {
		rf.mu.Lock()
		isLeader := rf.state == LeaderState
		if isLeader {
			// If there exists an N such that N > commitIndex, a majority
			// of matchIndex[i] ??? N, and log[N].term == currentTerm:
			// set commitIndex = N (??5.3, ??5.4).
			for n := rf.commitIndex + 1; n < len(rf.log); n++ {
				if rf.log[n].Term == rf.currentTerm {
					commitedCount := 0
					for i := range rf.peers {
						if rf.matchIndex[i] >= n {
							commitedCount++
						}
					}
					if commitedCount > len(rf.peers)/2 {
						rf.commitIndex = n
						go rf.kickApplyChan(n)
					}
				}
			}
		}
		rf.mu.Unlock()

		time.Sleep(25 * time.Millisecond)
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

	// Your initialization code here (2A, 2B, 2C).

	rf.state = FollowerState
	rf.timedOut = false

	// Persistent State
	rf.currentTerm = 0
	rf.votedFor = -1
	// Empty first log entry for indexing
	firstEntry := LogEntry{Entry: "", Term: 0}
	rf.log = Log{firstEntry}

	// Volatile State
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}
	for range rf.peers {
		rf.nextIndex = append(rf.nextIndex, len(rf.log))
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	// Extras
	rf.commitChan = make(chan int)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start goroutines for raft loops
	go rf.ticker()
	go rf.applyChRoutine(applyCh)

	return rf
}
