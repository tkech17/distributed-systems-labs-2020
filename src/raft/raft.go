package raft

import (
	"math/rand"
	"sync"
	"time"
)
import "../labrpc"

type RaftServerState string

const (
	CANDIDATE RaftServerState = "CANDIDATE"
	FOLLOWER  RaftServerState = "FOLLOWER"
	LEADER    RaftServerState = "LEADER"
)

type AppendEntriesArgs struct {
	Term int64
}

type AppendEntriesReply struct {
	Term    int64
	Success bool
}

type LogEntry struct {
	Index int64
	Term  int64
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int
	dead      int32

	logs []LogEntry

	state                   RaftServerState
	votesGrantedFromServers int64
	currTerm                int64
	lastVote                int64

	voteChan        chan bool
	winElectionChan chan bool
	heartBeatChan   chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return int(rf.currTerm), rf.state == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

type RequestVoteArgs struct {
	Candidate    int64
	Term         int64
	LastLogIndex int64
	LastLogTerm  int64
}

type RequestVoteReply struct {
	Term  int64
	Voted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currTerm
	reply.Voted = false

	if args.Term > rf.currTerm {
		rf.becomeFollower(args.Term)
	}

	term, index := rf.getLastLogTerm(), rf.getLastLogIndex()
	if rf.canVote(args) && (args.LastLogTerm > term || (args.LastLogTerm == term && args.LastLogIndex >= index)) {

		rf.lastVote = args.Candidate
		reply.Voted = true
		rf.voteChan <- true

	}

}

func (rf *Raft) canVote(args *RequestVoteArgs) bool {
	return rf.lastVote == -1 || rf.lastVote == args.Candidate
}

func (rf *Raft) requestLeader(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.state != CANDIDATE || rf.currTerm != args.Term {
		return
	}

	if rf.currTerm < reply.Term {
		rf.becomeFollower(reply.Term)
	} else if reply.Voted {
		rf.tryBecomeLeader()
	}

}

func (rf *Raft) tryBecomeLeader() {
	rf.votesGrantedFromServers++
	if rf.votesGrantedFromServers*2 > int64(len(rf.peers)) {
		rf.state = LEADER
		rf.winElectionChan <- true
	}
}

func (rf *Raft) becomeFollower(term int64) {
	rf.state = FOLLOWER
	rf.currTerm = term
	rf.lastVote = -1
}

func (rf *Raft) getLastLogIndex() int64 {
	return rf.logs[rf.getLogsCount()-1].Index
}

func (rf *Raft) getLastLogTerm() int64 {
	return rf.logs[rf.getLogsCount()-1].Term
}

func (rf *Raft) getLogsCount() int {
	return len(rf.logs)
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	//atomic.StoreInt32(&rf.dead, 1) TODO[TK]
	// Your code here, if desired.
}

//func (rf *Raft) killed() bool {
//	z := atomic.LoadInt32(&rf.dead)
//	return z == 1
//}

func (rf *Raft) stateChecker() {
	for {
		state := rf.getCurrentState()

		switch state {
		case FOLLOWER:
			select {
			case <-rf.voteChan:
				DPrintf("FOLLOWER %d got vote", rf.me)
			case <-rf.heartBeatChan:
				DPrintf("FOLLOWER %d got heartbeat", rf.me)
			case <-waitTillHeartBeatTimeout():
				rf.updateState(func() {
					rf.state = CANDIDATE
				})
			}
		case LEADER:
			DPrintf("Leader send heart beats")
			go rf.sendHeartBeats()
			time.Sleep(time.Millisecond * 100)
		case CANDIDATE:
			rf.updateState(func() {
				rf.currTerm++
				rf.lastVote = int64(rf.me)
				rf.votesGrantedFromServers = 1
			})

			go rf.broadcastRequestVote()

			select {
			case <-rf.heartBeatChan:
				rf.updateState(func() {
					rf.state = FOLLOWER
					DPrintf("Leader elected before server %d could become leader", rf.me)
				})
			case <-rf.winElectionChan:
				DPrintf("Server %d won election", rf.me)
			case <-waitTillHeartBeatTimeout():
			}
		}
	}
}

func (rf *Raft) getCurrentState() RaftServerState {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func waitTillHeartBeatTimeout() <-chan time.Time {
	return time.After(time.Millisecond * time.Duration(rand.Intn(150)+500))
}

func (rf *Raft) broadcastRequestVote() {
	args := &RequestVoteArgs{}

	rf.mu.Lock()
	args.Term = rf.currTerm
	args.Candidate = int64(rf.me)
	args.LastLogIndex = rf.getLastLogIndex()
	args.LastLogTerm = rf.getLastLogTerm()
	rf.mu.Unlock()

	for server := range rf.peers {
		if rf.state == CANDIDATE && server != rf.me {
			go rf.requestLeader(server, args, &RequestVoteReply{})
		}
	}
}

func (rf *Raft) sendHeartBeats() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for server := range rf.peers {
		if server == rf.me || rf.state != LEADER {
			break
		}

		args := &AppendEntriesArgs{}
		args.Term = rf.currTerm

		go rf.sendAppendEntries(server, args, &AppendEntriesReply{})

	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currTerm {
		reply.Term = rf.currTerm
		reply.Success = false
		return
	} else if args.Term > rf.currTerm {
		rf.becomeFollower(args.Term)
		reply.Success = true
		rf.heartBeatChan <- true
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.state != LEADER || args.Term != rf.currTerm {
		return
	}

	if reply.Term > rf.currTerm {
		rf.becomeFollower(reply.Term)
		return
	}

}

func (rf *Raft) updateState(fun func()) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fun()
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = FOLLOWER
	rf.votesGrantedFromServers = 0

	rf.currTerm = 0
	rf.lastVote = -1
	rf.logs = append(rf.logs, LogEntry{
		Index: 0,
		Term:  0},
	)

	rf.voteChan = make(chan bool, 100)
	rf.winElectionChan = make(chan bool, 100)
	rf.heartBeatChan = make(chan bool, 100)

	rf.readPersist(persister.ReadRaftState())

	go rf.stateChecker()

	return rf
}
