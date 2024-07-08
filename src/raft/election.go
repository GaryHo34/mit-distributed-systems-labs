package raft

import (
	"math/rand"
	"sync/atomic"
	"time"
)

// Source: https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf, Figure 2

type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// RequestVote RPC handler
// Restart your election timer if you grant a vote to another peer.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	// Reply false if term < currentTerm (§5.1)
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if !rf.isCallerTermValid(args.Term) {
		return
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isUpToDate(args) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.resetElectionTimer()
	}
}

func (rf *Raft) isUpToDate(args *RequestVoteArgs) bool {
	lastLog := rf.logs[len(rf.logs)-1]
	candidateIndex := args.LastLogIndex
	candidateTerm := args.LastLogTerm
	return candidateTerm > lastLog.Term || (candidateTerm == lastLog.Term && candidateIndex >= lastLog.Index)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, voteCount *int32) {
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()

	if rf.isReplyTermGreater(reply.Term) {
		return
	}

	if !reply.VoteGranted {
		DPrintf("[%d]: not received vote from %d\n", rf.me, server)
		return
	}

	DPrintf("[%d]: received vote from %d\n", rf.me, server)

	if atomic.AddInt32(voteCount, 1) > int32(len(rf.peers)/2) &&
		rf.state == CANDIDATE &&
		rf.currentTerm == args.Term {
		rf.state = LEADER
		lastLogIndex := rf.logs[len(rf.logs)-1].Index
		for i := range rf.peers {
			rf.nextIndex[i] = lastLogIndex + 1
			rf.matchIndex[i] = 0
		}
		DPrintf("[%d]: become leader to term %d\n", rf.me, rf.currentTerm)
		rf.broadcastAppendEntries(true)
	}
}

func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.state = CANDIDATE
	rf.votedFor = rf.me
	rf.resetElectionTimer()
	DPrintf("[%d]: start election, term %d", rf.me, rf.currentTerm)
	lastLog := rf.logs[len(rf.logs)-1]

	voteCount := int32(1)
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}

	for id := range rf.peers {
		if id == rf.me {
			continue
		}
		go rf.sendRequestVote(id, &args, &voteCount)
	}
}

func (rf *Raft) resetElectionTimer() {
	// Choose from 150 to 300
	ms := 150 + (rand.Int63() % 150)
	rf.electionTimeStamp = time.Now()
	rf.electionTimeout = time.Duration(ms) * time.Millisecond
}
