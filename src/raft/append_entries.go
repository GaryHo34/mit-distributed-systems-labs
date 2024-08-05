package raft

import "fmt"

// Source: https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf, Figure 2

type AppendEntriesArgs struct {
	LeaderId     int     // so follower can redirect clients
	Term         int     // leader's term
	PrevLogIndex int     // index of log entry immediately preceding new ones
	PrevLogTerm  int     // term of prevLogIndex entry
	Entries      []Entry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	CommitIndex  int     // leader's commitIndex
}

type AppendEntriesReply struct {
	Term          int  // currentTerm, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictIndex int  // the index of the first conflicting entry
}

// AppendEntries RPC handler
// Reset the election timer if you get an AppendEntries RPC from the current leader
// (i.e., if the term of the AppendEntries arguments is outdated, you should not reset your timer);
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	DPrintf("(AppendEntries) [%d] recieve from %d, Term: %d, PrevLogIndex: %d, PrevLogTerm: %d\n", rf.me, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm)

	reply.Success = false
	reply.ConflictIndex = -1
	reply.Term = rf.currentTerm

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if !rf.isCallerTermValid(args.Term) {
		return
	}

	if rf.state == CANDIDATE {
		rf.state = FOLLOWER
	}

	rf.resetElectionTimer()

	prevLogIndex := args.PrevLogIndex - rf.logs[0].Index

	if prevLogIndex < 0 {
		reply.ConflictIndex = 0
		return
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if prevLogIndex >= len(rf.logs) {
		return
	}

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	if rf.logs[prevLogIndex].Term != args.PrevLogTerm {
		// optimization
		curTerm := rf.logs[prevLogIndex].Term
		var conflictIndex int
		for i := prevLogIndex; i > 0; i-- {
			if rf.logs[i-1].Term != curTerm {
				conflictIndex = i
				break
			}
		}
		reply.ConflictIndex = conflictIndex + rf.logs[0].Index
		return
	}
	for idx, entry := range args.Entries {
		logIndex := entry.Index - rf.logs[0].Index
		if logIndex >= len(rf.logs) || rf.logs[logIndex].Term != entry.Term {
			DPrintf("(AppendEntries) [%d] append logs: %v from\n", rf.me, args.Entries)
			rf.logs = append([]Entry{}, append(rf.logs[:logIndex], args.Entries[idx:]...)...)
			break
		}
	}
	reply.Success = true
	if args.CommitIndex > rf.commitIndex {
		rf.commitIndex = args.CommitIndex
		if args.CommitIndex-rf.logs[0].Index >= len(rf.logs) {
			rf.commitIndex = rf.logs[len(rf.logs)-1].Index
		}
	}
	rf.applierCond.Signal()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	DPrintf("(AppendEntries) [%d] send to %d, Term: %d, PrevLogIndex: %d, PrevLogTerm: %d\n", rf.me, server, args.Term, args.PrevLogIndex, args.PrevLogTerm)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)

	if !ok {
		return
	}
	DPrintf("(AppendEntries) [%d] recieve reply from %d, Term: %d, Success: %v, ConflictIndex: %d\n", rf.me, server, reply.Term, reply.Success, reply.ConflictIndex)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// The term is outdated, do not reset the election timer
	if rf.isReplyTermGreater(reply.Term) {
		return
	}

	// If successful: update nextIndex and matchIndex for
	// follower (§5.3)
	if reply.Success {
		if len(args.Entries) > 0 {
			rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
		}
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		for _, log := range rf.logs {
			index := log.Index
			count := 1
			for peer := range rf.peers {
				if peer != rf.me && rf.matchIndex[peer] >= index {
					count++
				}
			}
			// If there exists an N such that N > commitIndex, a majority
			// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
			// set commitIndex = N (§5.3, §5.4).
			if count > len(rf.peers)/2 && index > rf.commitIndex && log.Term == rf.currentTerm {
				rf.commitIndex = index
			}
		}
	} else {
		rf.nextIndex[server] = reply.ConflictIndex - 1
		if rf.nextIndex[server] < 1 {
			rf.nextIndex[server] = 1
		}
	}
	DPrintf("(AppendEntries) [%d] nextIndex: %v, matchIndex: %v, commitIndex: %d\n", rf.me, rf.nextIndex, rf.matchIndex, rf.commitIndex)
	rf.applierCond.Signal()
}

func (rf *Raft) broadcastAppendEntries(isHeartBeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartBeat {
			rf.prepareAppendEntries(peer, true)
		} else {
			rf.broadcasterCond[peer].Signal()
		}
	}
}

func (rf *Raft) prepareAppendEntries(peer int, isAsync bool) {
	rf.mu.Lock()
	firstLog := rf.logs[0]
	nextIndex := rf.nextIndex[peer]
	if nextIndex > firstLog.Index {
		nextIndex -= firstLog.Index
		if nextIndex > len(rf.logs) {
			fmt.Printf("Server %d nextIndex %d >= len(rf.logs) %d\n", rf.me, nextIndex, len(rf.logs))
			nextIndex = len(rf.logs)
		}
		prevLog := rf.logs[nextIndex-1]
		logs := make([]Entry, len(rf.logs[nextIndex:]))
		copy(logs, rf.logs[nextIndex:])
		args := AppendEntriesArgs{
			LeaderId:     rf.me,
			Term:         rf.currentTerm,
			PrevLogIndex: prevLog.Index,
			PrevLogTerm:  prevLog.Term,
			Entries:      logs,
			CommitIndex:  rf.commitIndex,
		}
		rf.mu.Unlock()
		if isAsync {
			go rf.sendAppendEntries(peer, &args)
		} else {
			rf.sendAppendEntries(peer, &args)
		}
	} else {
		args := &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.logs[0].Index,
			LastIncludedTerm:  rf.logs[0].Term,
			Offset:            0,
			Data:              rf.persister.ReadSnapshot(),
			Done:              true,
		}
		rf.mu.Unlock()
		if isAsync {
			go rf.sendInstallSnapshot(peer, args)
		} else {
			rf.sendInstallSnapshot(peer, args)
		}
	}
}
