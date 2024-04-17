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

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// Why do we reset state even the term is equal to currentTerm?
	// Because in one term there is only one leader, so if another server has been
	// selected as leader, we should reset our state to follower.
	if args.Term >= rf.currentTerm {
		rf.resetNewTermState(args.Term)
	}

	rf.resetElectionTimer()
	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// The term is outdated, do not reset the election timer
	if reply.Term > rf.currentTerm {
		rf.resetNewTermState(reply.Term)
		return
	}
}

func (rf *Raft) broadcastAppendEntries(isHeartbeat bool) {
	lastLog := rf.logs[len(rf.logs)-1]
	for id := range rf.peers {
		if id == rf.me {
			continue
		}
		prevIndex := rf.nextIndex[id] - 1
		if prevIndex > lastLog.Index {
			prevIndex = lastLog.Index
		}
		prevLog := rf.logs[prevIndex]
		if isHeartbeat {
			args := AppendEntriesArgs{
				LeaderId:     rf.me,
				Term:         rf.currentTerm,
				PrevLogIndex: prevLog.Index,
				PrevLogTerm:  prevLog.Term,
				Entries:      make([]Entry, 0),
				CommitIndex:  rf.commitIndex,
			}
			go rf.sendAppendEntries(id, &args)
		}
	}
}
