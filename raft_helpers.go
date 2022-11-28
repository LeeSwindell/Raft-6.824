package raft

import (
	"fmt"
	"runtime"
)

// Resets the vote while updating the term.
// Always call this while holding the raft lock.
func (rf *Raft) updateTerm(newTerm int) {
	rf.currentTerm = newTerm
	rf.votedFor = -1
}

// Used to send RPC requests to all other peers and handle replies
// Sends out a concurrent call to all peers to do input func
func (rf *Raft) sendToPeers(fn func(server int)) {
	for server := range rf.peers {
		if server != rf.me {
			go fn(server)
		}
	}
}

func (rf *Raft) becomeLeader() {
	rf.state = LeaderState
	// Reinitialize after election
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}

	//starts  a go routine to maintain each followers log.
	rf.sendToPeers(rf.maintainLogsLoop)
	go rf.commitLoop()
	go rf.heartbeatLoop()
	// fmt.Printf("%v Elected\n", rf.me)
	fmt.Printf("num goroutines: %v\n", runtime.NumGoroutine())
}

func (rf *Raft) becomeCandidate() {
	rf.state = CandidateState
}

func (rf *Raft) revertToFollower() {
	rf.state = FollowerState
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Send in a go routine so as not to block
func (rf *Raft) kickApplyChan(newCommit int) {
	rf.commitChan <- newCommit
}
