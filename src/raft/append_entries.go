package raft

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
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries RPC handler
// Reset the election timer if you get an AppendEntries RPC from the current leader
// (i.e., if the term of the AppendEntries arguments is outdated, you should not reset your timer);
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	DPrintf("Server %d received AppendEntries from %d, args: %v", rf.me, args.LeaderId, args)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.resetNewTermState(args.Term)
	}

	if rf.state == CANDIDATE {
		rf.state = FOLLOWER
	}

	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	//Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex >= len(rf.logs) {
		return
	}

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.logs = rf.logs[:args.PrevLogIndex]
		return
	}
	for _, entry := range args.Entries {
		if entry.Index >= len(rf.logs) || rf.logs[entry.Index].Term != entry.Term {
			rf.logs = append([]Entry{}, append(rf.logs[:args.PrevLogIndex+1], args.Entries...)...)
			break
		}
	}
	DPrintf("Server %d received entries from leader %d: %v", rf.me, args.LeaderId, rf.logs)
	reply.Success = true
	if args.CommitIndex > rf.commitIndex {
		if args.CommitIndex >= len(rf.logs) {
			DPrintf("Server %d commitIndex: %d, len(rf.logs): %d", rf.me, args.CommitIndex, len(rf.logs))
			rf.commitIndex = len(rf.logs) - 1
		} else {
			DPrintf("Server %d commitIndex: %d", rf.me, args.CommitIndex)
			rf.commitIndex = args.CommitIndex
		}
	}
	rf.applierCond.Signal()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	DPrintf("Server %d sendAppendEntries to %d, args: %v", rf.me, server, args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)

	if !ok {
		return
	}
	DPrintf("Server %d args %v reply %v", rf.me, args.Entries, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// The term is outdated, do not reset the election timer
	if reply.Term > rf.currentTerm {
		rf.resetNewTermState(reply.Term)
		return
	}

	// If successful: update nextIndex and matchIndex for
	// follower (§5.3)
	if reply.Success && len(args.Entries) > 0 {
		DPrintf("Server %d received success from %d, nextIndex: %v, matchIndex: %v", rf.me, server, rf.nextIndex, rf.matchIndex)
		rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		for index := range rf.logs {
			count := 1
			for peer := range rf.peers {
				if peer != rf.me && rf.matchIndex[peer] >= index {
					count++
				}
			}
			// If there exists an N such that N > commitIndex, a majority
			// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
			// set commitIndex = N (§5.3, §5.4).
			if count > len(rf.peers)/2 && index > rf.commitIndex && rf.logs[index].Term == rf.currentTerm {
				rf.commitIndex = index
			}
		}
	} else if !reply.Success {
		rf.nextIndex[server]--
		if rf.nextIndex[server] < 0 {
			rf.nextIndex[server] = 0
		}
	}

	rf.applierCond.Signal()
}

func (rf *Raft) broadcaster(peer int) {
	rf.broadcasterCond[peer].L.Lock()
	defer rf.broadcasterCond[peer].L.Unlock()
	for !rf.killed() {
		for rf.noNeedReplicating(peer) {
			rf.broadcasterCond[peer].Wait()
		}
		lastLog := rf.logs[len(rf.logs)-1]
		prevIndex := rf.nextIndex[peer] - 1
		if prevIndex > lastLog.Index {
			prevIndex = lastLog.Index
		}
		prevLog := rf.logs[prevIndex]
		args := AppendEntriesArgs{
			LeaderId:     rf.me,
			Term:         rf.currentTerm,
			PrevLogIndex: prevLog.Index,
			PrevLogTerm:  prevLog.Term,
			Entries:      rf.logs[rf.nextIndex[peer]:],
			CommitIndex:  rf.commitIndex,
		}
		DPrintf("Server %d broadcast to %d, self log: %v", rf.me, peer, rf.logs)
		rf.sendAppendEntries(peer, &args)
		DPrintf("Server %d broadcast to %d: %v done", rf.me, peer, args)
	}
}

func (rf *Raft) noNeedReplicating(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state != LEADER || rf.matchIndex[peer] >= rf.logs[len(rf.logs)-1].Index
}

func (rf *Raft) broadcastHeartbeat(peer int) {
	lastLog := rf.logs[len(rf.logs)-1]
	prevIndex := rf.nextIndex[peer] - 1
	if prevIndex > lastLog.Index {
		prevIndex = lastLog.Index
	}
	prevLog := rf.logs[prevIndex]
	args := AppendEntriesArgs{
		LeaderId:     rf.me,
		Term:         rf.currentTerm,
		PrevLogIndex: prevLog.Index,
		PrevLogTerm:  prevLog.Term,
		Entries:      rf.logs[rf.nextIndex[peer]:],
		CommitIndex:  rf.commitIndex,
	}
	go rf.sendAppendEntries(peer, &args)
}
