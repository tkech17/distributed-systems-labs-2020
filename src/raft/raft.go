package raft

import (
	"../labgob"
	"../labrpc"
	"bytes"
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type State string

const (
	LEADER    State = "LEADER"
	FOLLOWER  State = "FOLLOWER"
	CANDIDATE State = "CANDIDATE"
)

type LogEntry struct {
	Term    int
	Command interface{}
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	SnapShot     []byte
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int

	state       State
	curTerm     int
	lastVote    int
	log         []LogEntry
	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	applyCh chan ApplyMsg
	ch      chan bool

	LastIncludedIndex int
	LastIncludedTerm  int
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

type InstallSnapShotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapShotReply struct {
	Term int
}

func reset(ch chan bool) {
	select {
	case <-ch:
	default:
	}
	ch <- true
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.curTerm, rf.state == LEADER
}

func (rf *Raft) encode() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.curTerm)
	e.Encode(rf.lastVote)
	e.Encode(rf.log)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)
	return w.Bytes()
}

func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.encode())
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	d.Decode(&rf.curTerm)
	d.Decode(&rf.lastVote)
	d.Decode(&rf.log)
	d.Decode(&rf.LastIncludedIndex)
	d.Decode(&rf.LastIncludedTerm)

	rf.commitIndex = rf.LastIncludedIndex
	rf.lastApplied = rf.LastIncludedIndex
}

func (rf *Raft) StartSnapShot(snapShot []byte, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.LastIncludedIndex {
		return
	}
	log := make([]LogEntry, 0)
	log = append(log, rf.log[index-rf.LastIncludedIndex:]...)
	rf.log = log
	rf.LastIncludedIndex = index
	rf.LastIncludedTerm = rf.log[index-rf.LastIncludedIndex].Term
	rf.persister.SaveStateAndSnapshot(rf.encode(), snapShot)
}

func (rf *Raft) changeState(state State) {

	switch state {
	case FOLLOWER:
		rf.state = FOLLOWER
		rf.lastVote = -1
		rf.persist()
	case CANDIDATE:
		rf.state = CANDIDATE
		rf.lastVote = rf.me
		rf.curTerm++
		rf.persist()
		reset(rf.ch)
		go rf.electForLeader()
	case LEADER:
		if rf.state == CANDIDATE {
			rf.makeCandidateAsLeader()
		}

	}
}

func (rf *Raft) makeCandidateAsLeader() {
	rf.state = LEADER
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.lastLogInx() + 1
	}
	for i := 0; i < len(rf.matchIndex); i++ {
		rf.matchIndex[i] = -1
	}
}

func (rf *Raft) logsCount() int {
	return len(rf.log) + rf.LastIncludedIndex
}
func (rf *Raft) lastLogInx() int {
	return rf.logsCount() - 1
}

func (rf *Raft) getLastLogTerm() int {
	index := rf.lastLogInx()
	if index < rf.LastIncludedIndex {
		return -1
	}
	return rf.log[index-rf.LastIncludedIndex].Term
}

func (rf *Raft) getPrevLogTerm(i int) int {

	index := rf.nextIndex[i] - 1
	if index < rf.LastIncludedIndex {
		return -1
	}
	return rf.log[index-rf.LastIncludedIndex].Term
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) electForLeader() {
	args := func(rf *Raft) RequestVoteArgs {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		return RequestVoteArgs{rf.curTerm, rf.me, rf.lastLogInx(), rf.getLastLogTerm()}
	}(rf)

	var votes int32 = 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(index int) {
			reply := &RequestVoteReply{}
			response := rf.sendRequestVote(index, &args, reply)
			if !response {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.curTerm {
				rf.curTerm = reply.Term
				rf.changeState(FOLLOWER)
				return
			} else if rf.state != CANDIDATE || rf.curTerm != args.Term {
				return
			} else if reply.VoteGranted {

				if atomic.AddInt32(&votes, 1) > int32(len(rf.peers)/2) {
					rf.changeState(LEADER)
					rf.appendLogEntries()
					reset(rf.ch)
				}
			}

		}(i)
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.curTerm < args.Term {
		rf.curTerm = args.Term
		rf.changeState(FOLLOWER)
	}

	reply.VoteGranted = false
	reply.Term = rf.curTerm

	if rf.curTerm > args.Term {
		return
	}
	if (rf.lastVote == -1 || rf.lastVote == args.CandidateId) &&
		(args.LastLogTerm > rf.getLastLogTerm() || ((args.LastLogTerm == rf.getLastLogTerm()) && (args.LastLogIndex >= rf.lastLogInx()))) {

		reply.VoteGranted = true
		rf.lastVote = args.CandidateId
		rf.state = FOLLOWER
		rf.persist()
		reset(rf.ch)

	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) appendLogEntries() {

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(index int) {
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				return
			}

			if rf.nextIndex[index]-rf.LastIncludedIndex < 1 {
				rf.transmitSnapShot(index)
				return
			}
			args := AppendEntriesArgs{rf.curTerm, rf.me, rf.nextIndex[index] - 1, rf.getPrevLogTerm(index),
				append(make([]LogEntry, 0), rf.log[rf.nextIndex[index]-rf.LastIncludedIndex:]...), rf.commitIndex}

			rf.mu.Unlock()

			reply := &AppendEntriesReply{}
			respond := rf.sendAppendEntries(index, &args, reply)
			rf.mu.Lock()
			if !respond || rf.state != LEADER || rf.curTerm != args.Term {
				rf.mu.Unlock()
				return
			}
			if reply.Term > rf.curTerm {
				rf.curTerm = reply.Term
				rf.changeState(FOLLOWER)
				rf.mu.Unlock()
				return
			}
			if reply.Success {
				rf.matchIndex[index] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[index] = rf.matchIndex[index] + 1
				rf.matchIndex[rf.me] = rf.logsCount() - 1
				copyMatchIndex := make([]int, len(rf.matchIndex))
				copy(copyMatchIndex, rf.matchIndex)
				sort.Sort(sort.Reverse(sort.IntSlice(copyMatchIndex)))
				n := copyMatchIndex[len(copyMatchIndex)/2]
				if n > rf.commitIndex && rf.log[n-rf.LastIncludedIndex].Term == rf.curTerm {
					rf.commitIndex = n
					rf.applyChanges()
				}
				rf.mu.Unlock()
				return
			} else {
				rf.nextIndex[index] = reply.ConflictIndex
				if reply.ConflictTerm != -1 {
					c := 0
					for i := rf.LastIncludedIndex; i < rf.logsCount(); i++ {
						if rf.log[i-rf.LastIncludedIndex].Term == reply.ConflictTerm {
							c = i
						}
					}
					rf.nextIndex[index] = c + 1
				}
				rf.mu.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.curTerm < args.Term {
		rf.curTerm = args.Term
		rf.changeState(FOLLOWER)
	}
	reply.Success = false
	reply.Term = rf.curTerm
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1
	reset(rf.ch)

	if args.Term < rf.curTerm {
		return
	}

	if args.PrevLogIndex >= rf.LastIncludedIndex && args.PrevLogIndex < rf.logsCount() {

		if args.PrevLogTerm != rf.log[args.PrevLogIndex-rf.LastIncludedIndex].Term {
			reply.ConflictTerm = rf.log[args.PrevLogIndex-rf.LastIncludedIndex].Term
			for i := rf.LastIncludedIndex; i < rf.logsCount(); i++ {
				if rf.log[i-rf.LastIncludedIndex].Term == reply.ConflictTerm {
					reply.ConflictIndex = i
					break
				}
			}
			return
		}
	} else {
		reply.ConflictIndex = rf.logsCount()
		return
	}

	index := args.PrevLogIndex
	for i := 0; i < len(args.Entries); i++ {
		index++
		if index >= rf.logsCount() {
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}
		if rf.log[index-rf.LastIncludedIndex].Term != args.Entries[i].Term {
			rf.log = rf.log[:index-rf.LastIncludedIndex]
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}
	}
	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.logsCount()-1)))
		rf.applyChanges()
	}
	reply.Success = true
}

func (rf *Raft) applyChanges() {
	rf.commitIndex = int(math.Max(float64(rf.commitIndex), float64(rf.LastIncludedIndex)))
	rf.lastApplied = int(math.Max(float64(rf.lastApplied), float64(rf.LastIncludedIndex)))
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		currLog := rf.log[rf.lastApplied-rf.LastIncludedIndex]
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      currLog.Command,
			CommandIndex: rf.lastApplied,
			SnapShot:     nil,
		}
		rf.applyCh <- applyMsg
	}
}

func (rf *Raft) sendInstallSnapShot(server int, args *InstallSnapShotArgs, reply *InstallSnapShotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapShot(args *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.curTerm
	if args.Term < rf.curTerm {
		return
	}
	if args.Term > rf.curTerm {
		rf.curTerm = args.Term
		rf.changeState(FOLLOWER)
	}
	reply.Term = rf.curTerm
	reset(rf.ch)
	if args.LastIncludedIndex <= rf.LastIncludedIndex {
		return
	}
	msg := ApplyMsg{CommandValid: false, SnapShot: args.Data}
	if args.LastIncludedIndex < rf.logsCount()-1 {
		rf.log = append(make([]LogEntry, 0), rf.log[args.LastIncludedIndex-rf.LastIncludedIndex:]...)
	} else {
		rf.log = []LogEntry{{args.LastIncludedTerm, nil}}
	}

	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm
	rf.persister.SaveStateAndSnapshot(rf.encode(), args.Data)
	rf.commitIndex = int(math.Max(float64(rf.commitIndex), float64(rf.LastIncludedIndex)))
	rf.lastApplied = int(math.Max(float64(rf.lastApplied), float64(rf.LastIncludedIndex)))
	if rf.lastApplied > rf.LastIncludedIndex {
		return
	}
	rf.applyCh <- msg
}

func (rf *Raft) transmitSnapShot(server int) {
	args := InstallSnapShotArgs{rf.curTerm, rf.me, rf.LastIncludedIndex, rf.LastIncludedTerm, rf.persister.ReadSnapshot()}
	rf.mu.Unlock()
	reply := &InstallSnapShotReply{}
	respond := rf.sendInstallSnapShot(server, &args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !respond || rf.state != LEADER || rf.curTerm != args.Term {
		return
	}
	if reply.Term > rf.curTerm {
		rf.curTerm = reply.Term
		rf.changeState(FOLLOWER)
		return
	}

	rf.matchIndex[server] = rf.LastIncludedIndex
	rf.nextIndex[server] = rf.LastIncludedIndex + 1

	rf.matchIndex[rf.me] = rf.logsCount() - 1
	copyMatchIndex := make([]int, len(rf.matchIndex))
	copy(copyMatchIndex, rf.matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(copyMatchIndex)))
	N := copyMatchIndex[len(copyMatchIndex)/2]
	if N > rf.commitIndex && rf.log[N-rf.LastIncludedIndex].Term == rf.curTerm {
		rf.commitIndex = N
		rf.applyChanges()
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.curTerm
	isLeader := rf.state == LEADER

	if isLeader {
		index = rf.logsCount()
		newLogEntry := LogEntry{
			Term:    rf.curTerm,
			Command: command,
		}
		rf.log = append(rf.log, newLogEntry)
		rf.persist()
		rf.appendLogEntries()
	}

	return index, term, isLeader
}

func (rf *Raft) Kill() {

}

func raftTimer(rf *Raft) func() {
	heartbeatTime := time.Duration(100) * time.Millisecond

	for {
		electionTimeout := time.Duration(rand.Intn(222)+333) * time.Millisecond

		state := func(rf *Raft) State {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			return rf.state
		}(rf)

		switch state {
		case FOLLOWER, CANDIDATE:
			select {
			case <-rf.ch:
			case <-time.After(electionTimeout):
				func(rf *Raft) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					rf.changeState(CANDIDATE)
				}(rf)
			}
		case LEADER:
			time.Sleep(heartbeatTime)
			rf.appendLogEntries()
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int, persistent *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persistent
	rf.me = me

	rf.state = FOLLOWER
	rf.curTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = make([]LogEntry, 1)

	rf.lastVote = -1

	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	for i := 0; i < len(rf.matchIndex); i++ {
		rf.matchIndex[i] = -1
	}

	rf.ch = make(chan bool, 1)
	rf.ch = make(chan bool, 1)

	rf.readPersist(persistent.ReadRaftState())
	go raftTimer(rf)

	return rf
}
