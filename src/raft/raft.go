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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

func findKthLargest(nums []int, k int) int {
	n := len(nums)
	copy := nums[:]
	return quickselect(copy, 0, n-1, n-k)
}

func quickselect(nums []int, l, r, k int) int {
	if l == r {
		return nums[k]
	}
	partition := nums[l]
	i := l - 1
	j := r + 1
	for i < j {
		for i++; nums[i] < partition; i++ {
		}
		for j--; nums[j] > partition; j-- {
		}
		if i < j {
			nums[i], nums[j] = nums[j], nums[i]
		}
	}
	if k <= j {
		return quickselect(nums, l, j, k)
	} else {
		return quickselect(nums, j+1, r, k)
	}
}

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

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	state     int64               //0 follower 1 candidate 2 leader
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int //latest term server has seen (initialized to 0 on first boot, increases monotonically)

	votedFor int        //candidateId that received vote in current term
	logs     []LogEntry //log entries; each entry contains commandfor state machine, and term when entry was received by leader

	//volatile state on all servers
	commitIndex int //index of highest log entry known to be
	lastApplied int // index of highest log entry applied to state machine

	//volatile state on leaders
	nextIndex  []int //for each server, index of the next log entryto send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	// mydata
	ch             chan ApplyMsg
	reset          chan bool
	majority       int // majority of servers
	selectionTimer *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.isState(Leader)
	rf.mu.Unlock()
	return term, isleader
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {

	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

const (
	Follower = iota
	Candidate
	Leader
)

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type AppendEntriesArgs struct {
	// Your data here (3A, 3B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

const heartbeatTimeout = 157

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	// Your data here (3A).
	Term    int
	Success bool
	Index   int //rejectOpt
}

// wrap log at external
func (rf *Raft) getLastLog() LogEntry {

	return rf.logs[len(rf.logs)-1]
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if rf.currentTerm < args.Term {
		rf.enterNewTerm(reply.Term)
		rf.setState(Follower)
	}
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		rf.mu.Unlock()
		return //implementation 1
	}
	lastLog := rf.getLastLog()
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (lastLog.Index >= args.LastLogIndex && lastLog.Term > args.LastLogTerm) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.mu.Unlock()
		rf.reset <- true
	}
	rf.mu.Unlock()

}

// minInt returns the minimum value among the provided integers.
func minInt(nums ...int) int {
	if len(nums) == 0 {
		panic("minInt requires at least one argument")
	}
	min := nums[0]
	for _, num := range nums[1:] {
		if num < min {
			min = num
		}
	}
	return min
}

// example AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	reply.Success = true
	reply.Term = rf.currentTerm //todo check
	inconsistentTerm := -1
	if !rf.checkAppendEntriesArgs(args, &inconsistentTerm) { //implementation 2
		reply.Success = false
		reply.Index = rf.optReject(inconsistentTerm)
		rf.mu.Unlock()
		return
	}
	if rf.currentTerm < args.Term { //implementation 1
		rf.enterNewTerm(reply.Term) //reset term and votedfor
		rf.setState(Follower)
	}
	// start appending entries
	for index, log := range args.Entries {
		if log.Index >= len(rf.logs) {
			rf.logs = append(rf.logs, args.Entries[index:]...)
			break
		}
		assert(rf.logs[log.Index].Index == log.Index, "log index must be equal")
		if rf.logs[log.Index].Term != log.Term {
			rf.logs = rf.logs[:log.Index]
		}
		if log.Index >= len(rf.logs) {
			rf.logs = append(rf.logs, args.Entries[index:]...)
			break
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = minInt(args.LeaderCommit, len(rf.logs)-1)
	}
	rf.mu.Unlock()
	rf.reset <- true
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
// must ok if no ok indefinitely retry
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	assert(rf.isState(Candidate), "sendRequestVote must be called by candidate")
	for {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		if ok {
			return ok
		}
	}
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	assert(rf.isState(Leader), "sendAppendEntries must be called by leader")
	for {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		if ok {
			return ok
		}
	}
}
func assert(condition bool, message string) {
	if !condition {
		panic(message)
	}
}
func (rf *Raft) MakeAppendEntriesArgs(server int, isHeartBeat bool) AppendEntriesArgs {
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevLogTerm:  rf.logs[rf.nextIndex[server]-1].Term,
		LeaderCommit: rf.commitIndex,
	}
	if isHeartBeat {
		args.Entries = nil
	} else {
		args.Entries = rf.logs[rf.nextIndex[server]:]
	}
	rf.mu.Unlock()
	return args
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
	// Your code here (3B).
	if !rf.isState(Leader) {
		return -1, -1, false
	}
	rf.mu.Lock()
	log := LogEntry{Term: rf.currentTerm, Index: len(rf.logs) - 1, Command: command}
	rf.logs = append(rf.logs, log)
	lastLog := rf.getLastLog()
	index = lastLog.Index
	term = rf.currentTerm
	assert(lastLog.Index == log.Index, "log index must be equal")
	assert(lastLog.Term == log.Term, "log term must be equal")
	assert(lastLog.Command == log.Command, "log command must be equal")
	assert(len(rf.logs) == lastLog.Index+1, "log length must be equal")
	rf.lastApplied++
	rf.mu.Unlock()
	var successReplicate int64 = 1

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			for !rf.killed() {
				time.Sleep(10 * time.Millisecond)
				args := rf.MakeAppendEntriesArgs(i, false)
				reply := AppendEntriesReply{}
				rf.mu.Lock()
				if rf.nextIndex[i] > rf.getLastLog().Index {
					rf.mu.Unlock()
					break
				}
				rf.mu.Unlock()
				rf.sendAppendEntries(i, &args, &reply)
				if reply.Success {
					atomic.AddInt64(&successReplicate, 1)
					rf.mu.Lock()
					rf.matchIndex[i] = len(rf.logs) - 1
					rf.nextIndex[i] = len(rf.logs)
					rf.mu.Unlock()
					break
				} else {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.enterNewTerm(reply.Term)
						rf.setState(Follower)
					} else {
						rf.nextIndex[i] = reply.Index
					}
					rf.mu.Unlock()
				}
			}
		}(i)
	}
	go func(successReplicate *int64) {
		for !rf.killed() {
			time.Sleep(10 * time.Millisecond)
			if !rf.isState(Leader) {
				break
			}
			if atomic.LoadInt64(successReplicate) > int64(rf.majority) {
				rf.mu.Lock()
				if rf.currentTerm == rf.logs[index].Term {
					rf.commitIndex = index
				}
				N := findKthLargest(rf.matchIndex, rf.majority)
				if rf.logs[N].Term == rf.currentTerm && N > rf.commitIndex {
					rf.commitIndex = N
				}
				rf.mu.Unlock()
			}
		}
	}(&successReplicate)
	return index, term, isLeader
}

func (rf *Raft) isState(state int) bool {
	return atomic.LoadInt64(&rf.state) == int64(state)
}
func (rf *Raft) setState(state int) {
	atomic.StoreInt64(&rf.state, int64(state))
}

// no lock should be wrap by lock
func (rf *Raft) optReject(term int) int {
	if term == -1 {
		return -1
	}
	for index, log := range rf.logs {
		assert(log.Index == index, "log index must be equal")
		if log.Term == term {
			return log.Index
		}
	}
	assert(false, "must find a log with term")
	return -1
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

// no lock should be wrapped bt lock
func (rf *Raft) enterNewTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
}
func (rf *Raft) StartElection(timer *time.Timer) {
	rf.mu.Lock()

	rf.setState(Candidate)
	rf.enterNewTerm(rf.currentTerm + 1)
	rf.votedFor = rf.me
	var voted int64 = 1
	rf.selectionTimer = time.NewTimer(time.Duration(50+(rand.Int63()%300)) * time.Millisecond)
	lastLog := rf.getLastLog()
	lastLogIndex := lastLog.Index
	lastLogTerm := lastLog.Term
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
	reply := RequestVoteReply{}
	rf.mu.Unlock()
	// send RequestVote RPCs to all other servers
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue //skip self
		}
		go func(index int) {
			rf.sendRequestVote(index, &args, &reply)
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.enterNewTerm(reply.Term)
				rf.setState(Follower)
			}
			rf.mu.Unlock()
			if reply.VoteGranted {
				atomic.AddInt64(&voted, 1)
			}
		}(i)
	}
	//wait for the votes
	win := false
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		if win {
			break
		}
		select {
		case <-rf.selectionTimer.C: //have expired
			return
		default:
			if atomic.LoadInt64(&voted) > int64(rf.majority) {
				win = true
			}
		}
		if !rf.isState(Candidate) {
			return
		}
	}
	rf.becameLeader()
}

func (rf *Raft) becameLeader() {
	rf.mu.Lock()
	rf.setState(Leader)
	last_log := rf.getLastLog()
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = last_log.Index + 1
		rf.matchIndex[i] = 0
	}
	rf.mu.Unlock()
	go func() { //start sending heartbeat
		for !rf.killed() {
			time.Sleep(10 * time.Millisecond)
			time.Sleep(heartbeatTimeout * time.Microsecond)
			rf.sendHeartBeat()
		}
	}()
}

// checkAppendEntries no lock
func (rf *Raft) checkAppendEntriesArgs(args *AppendEntriesArgs, term *int) bool {
	flag := true
	if args.Term < rf.currentTerm {
		flag = false
		return flag
	}
	if len(rf.logs) >= args.PrevLogIndex {
		DPrintf("should not happen len(rf.logs)=%d,args.PrevLogIndex=%d", len(rf.logs), args.PrevLogIndex)
		flag = false
	}
	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		flag = false
		*term = rf.logs[args.PrevLogIndex].Term
	}
	return flag
}
func (rf *Raft) ticker() {
	ms := 50 + (rand.Int63() % 300)
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds
		rf.selectionTimer = time.NewTimer(time.Duration(ms) * time.Millisecond)

		select {
		case <-rf.selectionTimer.C:
			// time to start a new election
			go rf.StartElection(rf.selectionTimer)
		case <-rf.reset:
			rf.selectionTimer.Stop()
			rf.setState(Follower)
			continue // 收到重置信号重置计时器
		}

	}

}

func (rf *Raft) sendHeartBeat() {
	// send heartbeat to all servers
	if !rf.isState(Leader) {
		return
	}
	reply := AppendEntriesReply{}

	for i := 0; i < len(rf.peers); i++ {
		go func(index int) {
			args := rf.MakeAppendEntriesArgs(index, true)
			rf.sendAppendEntries(index, &args, &reply)
		}(i)
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
	// Your initialization code here (3A, 3B, 3C).
	rf.dead = 0
	rf.ch = applyCh
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 1)
	rf.logs[0] = LogEntry{Term: 0, Index: 0, Command: nil}
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.majority = len(rf.peers)/2 + 1
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = rf.logs[len(rf.logs)-1].Index + 1
	}
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
