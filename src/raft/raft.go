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

	"sync"
	"sync/atomic"
	"time"

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
	CommandTerm  int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Term    int
	Index   int
	Command interface{}
}

type ServerState int

const (
	FOLLOWER ServerState = iota
	CANDIDATE
	LEADER
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu                sync.Mutex          // Lock to protect shared access to this peer's state
	peers             []*labrpc.ClientEnd // RPC end points of all peers
	persister         *Persister          // Object to hold this peer's persisted state
	me                int                 // this peer's index into peers[]
	dead              int32               // set by Kill()
	heartbeatTimeout  time.Duration
	electionTimeout   time.Duration
	electionTimeStamp time.Time
	applyCh           chan ApplyMsg
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	broadcasterCond []*sync.Cond
	applierCond     *sync.Cond

	// server state
	state ServerState

	// presistent state on all servers
	currentTerm int     // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int     // candidateId that received vote in current term (or null if none)
	logs        []Entry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// volatile state on all servers
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// volatile state on leaders (reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.state == LEADER
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		return -1, -1, false
	}
	newEntry := Entry{
		Term:    rf.currentTerm,
		Index:   len(rf.logs),
		Command: command,
	}
	rf.logs = append(rf.logs, newEntry)
	for peer := range rf.peers {
		if peer != rf.me {
			rf.broadcasterCond[peer].Signal()
		}
	}
	// Your code here (3B).
	return len(rf.logs) - 1, rf.currentTerm, true
}

// Warning: this function is not thread-safe. You must acquire the lock
// before calling this function.
func (rf *Raft) resetNewTermState(targetTerm int) {
	DPrintfWithCaller("[%d]: received newer term, set term to %d\n", rf.me, targetTerm)
	if rf.currentTerm < targetTerm {
		rf.votedFor = -1
	}
	rf.currentTerm = targetTerm
	rf.state = FOLLOWER // reset to follower
	rf.persist()
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

// a dedicated applier goroutine to guarantee that each log will be push into applyCh exactly once, ensuring that service's applying entries and raft's committing entries can be parallel
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		// if there is no need to apply entries, just release CPU and wait other goroutine's signal if they commit new entries
		for rf.lastApplied >= rf.commitIndex {
			rf.applierCond.Wait()
		}
		commitIndex, lastApplied := rf.commitIndex, rf.lastApplied
		entries := make([]Entry, commitIndex-lastApplied)
		copy(entries, rf.logs[lastApplied+1:commitIndex+1])
		rf.mu.Unlock()
		for _, entry := range entries {
			DPrintf("[%d]: apply entry %v\n", rf.me, entry)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandTerm:  entry.Term,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()
		// use commitIndex rather than rf.commitIndex because rf.commitIndex may change during the Unlock() and Lock()
		// use Max(rf.lastApplied, commitIndex) rather than commitIndex directly to avoid concurrently InstallSnapshot rpc causing lastApplied to rollback
		if rf.lastApplied < commitIndex {
			rf.lastApplied = commitIndex
		}
		rf.mu.Unlock()
	}
}

/**
 * Lets illustrate the time line of the ticker function
 * e: election timeout
 * h: heartbeat timeout
 *
 * ---- h ---- h ---- h ---- h ---- h ---- ...
 *
 * First, the server will wake up each fixed heartbeat timeout. This timeout is
 * relatively shorter than the election timeout. If the server is not a leader,
 * it basically do nothing about heartbeat.
 *
 * However, everytime when server wake up, it will check if the election timeout
 * is reached. It might start a new election, if it is not a leader.
 *
 *                      v election timeout found!
 * ---- h1 ---- h2 ---- h3 ---- h ---- h ---- ...
 * --------- e1 ------ e2 ------------ e ---- ...
 *
 * Reseting a new election timeout when the server receives a heartbeat or a
 * vote from another server prevents the election. One shortcomming of the
 * current implementation is that the election timeout does not trigger a new
 * election immediately. It will wait until the next heartbeat timeout.
 */
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.
		time.Sleep(rf.heartbeatTimeout)

		if rf.state == LEADER {
			for peer := range rf.peers {
				if peer != rf.me {
					rf.broadcastHeartbeat(peer)
				}
			}
		}

		// Lock must before the check of election timeout (a goroutine may
		// change the timer concurrently)
		rf.mu.Lock()
		if time.Now().After(rf.electionTimeStamp.Add(rf.electionTimeout)) &&
			rf.state != LEADER {
			rf.startElection()
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
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
	rf.applyCh = applyCh
	rf.heartbeatTimeout = 50 * time.Millisecond
	rf.resetElectionTimer()
	// Your initialization code here (3A, 3B, 3C).
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.logs = make([]Entry, 0)

	// dummy entry to make the index start from 1
	rf.logs = append(rf.logs, Entry{})

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applierCond = sync.NewCond(&rf.mu)
	rf.broadcasterCond = make([]*sync.Cond, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	lastLog := rf.logs[len(rf.logs)-1]
	for id := range peers {
		rf.nextIndex[id], rf.matchIndex[id] = lastLog.Index+1, 0
		if id != rf.me {
			rf.broadcasterCond[id] = sync.NewCond(&sync.Mutex{})
			go rf.broadcaster(id)
		}
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}
