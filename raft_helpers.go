package raft

import "log"

// Resets the vote while updating the term.
// Always call this while holding the raft lock.
//
// Logs an error if the new term breaks monotonicity (newTerm<current)
func (rf *Raft) updateTerm(newTerm int) {
	rf.currentTerm = newTerm
	rf.votedFor = -1
	if newTerm < rf.currentTerm {
		log.Fatalf("Raft %v tried to decrement its term, current: %v, new: %v",
			rf.me, rf.currentTerm, newTerm)
	}
}
