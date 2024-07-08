package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

// InstallSnapshot RPC handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm

	if !rf.isCallerTermValid(args.Term) {
		rf.mu.Unlock()
		return
	}

	if rf.state == CANDIDATE {
		rf.state = FOLLOWER
	}

	rf.resetElectionTimer()

	if args.LastIncludedIndex <= rf.commitIndex {
		rf.mu.Unlock()
		return
	}

	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	// 2. Create new snapshot file if first chunk (offset is 0)
	// 3. Write data into snapshot file at given offset
	// 4. Reply and wait for more data chunks if done is false
	if !args.Done {
		rf.mu.Unlock()
		return
	}
	// 5. Save snapshot file, discard any existing or partial snapshot with a
	//    smaller index
	// 6. If existing log entry has same index and term as snapshot’s last
	//    included entry, retain log entries following it and reply
	// 7. Discard the entire log
	// 8. Reset state machine using snapshot contents (and load snapshot’s
	//    cluster configuration)
	firstLogIndex := rf.logs[0].Index
	if rf.logs[0].Index <= args.LastIncludedIndex {
		rf.logs = append([]Entry{}, Entry{
			Index:   args.LastIncludedIndex,
			Term:    args.LastIncludedTerm,
			Command: nil,
		})
	} else if firstLogIndex < args.LastIncludedIndex {
		trimLen := args.LastIncludedIndex - firstLogIndex
		rf.logs = append([]Entry{}, rf.logs[trimLen:]...)
		rf.logs[0].Command = nil
	}
	rf.persister.Save(rf.encodeState(), args.Data)
	rf.mu.Unlock()
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs) {
	reply := InstallSnapshotReply{}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.isReplyTermGreater(reply.Term) {
		return
	}

	if rf.currentTerm != args.Term || rf.state != LEADER || args.LastIncludedIndex != rf.logs[0].Index {
		return
	}

	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex

	rf.persister.Save(rf.encodeState(), args.Data)
}
