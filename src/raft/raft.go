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
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
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

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogItem struct {
	Command    interface{}
	Term	   int
}

type ServerState int
const (
	Follower = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	serverState ServerState		  //server状态字段
	heartbeatTime time.Time    		  //上一次心跳的时间
	electionTimeout time.Duration   //选举超时时间
	applyCh chan ApplyMsg         //Raft发送消息告知上层服务器，让其执行命令

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	/*需要持久化在所有Server中的状态*/
	currentTerm int				  //server能看到的最后一个任期
	votedFor int				  //投给的候选人票对应的候选人Id
	log[] LogItem					  //日志条目

	/*所有服务器中的易失状态*/
	commitIndex int				  //已知的最高被提交的日志条目
	lastApplied int				  //最高被应用到状态机的日志条目
	condApply *sync.Cond

	/*leader中存储的易失状态（再每次选举之后需要重新初始化）*/
	nextIndex[] int				  //对于每一个Server而言，发送到该服务器的下一个日志条目的索引
	matchIndex[] int			  //对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增

	snapShot          []byte
	lastIncludedIndex int
	lastIncludedTerm int

	leaderId int
}

const (
	electionTimeoutMin time.Duration = 300 * time.Millisecond
	electionTimeoutMax time.Duration = 450 * time.Millisecond

	replicateInterval time.Duration = 250 * time.Millisecond
)


func (rf *Raft) resetElectionTimerLocked() {
	rf.heartbeatTime = time.Now()
	randRange := int64(electionTimeoutMax - electionTimeoutMin)
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63()%randRange)
}

func (rf *Raft) contextLostLocked(role ServerState, term int) bool {
	return !(rf.currentTerm == term && rf.serverState == role)
}

func (rf *Raft) newTermLocked(term int) {
	rf.currentTerm = term
	rf.serverState = Follower
	rf.votedFor = -1

	rf.persist()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	term := rf.currentTerm
	isleader := rf.serverState == Leader
	rf.mu.Unlock()

	return term, isleader
}

func (rf *Raft) GetLeaderId() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.leaderId
}

func (rf *Raft) serializeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.log)
	return w.Bytes()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
/*
	当任期发生变化、有新增的Log、VoteFor发生变化时需要进行持久化，以防止服务器重启丢失状态的问题
*/
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	raftstate := rf.serializeState()
	rf.persister.Save(raftstate, rf.snapShot)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogItem
	var lastIncludedIndex int
	var lastIncludedTerm int
	
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil ||
	   d.Decode(&lastIncludedIndex) != nil ||
	   d.Decode(&lastIncludedTerm) != nil ||
	   d.Decode(&log) != nil {
		Log(dError, "%d:read persist error. currentTerm=%d,votedFor=%d", rf.me,currentTerm,votedFor)
		return
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log

		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
	}
}

func (rf *Raft) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	
	rf.snapShot = snapshot
	Log(dClient,"S%d: read snapshot.",rf.me)
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.commitIndex && index > rf.lastIncludedIndex {
		newLogIndex := index - rf.lastIncludedIndex
		rf.lastIncludedIndex = index
		rf.lastIncludedTerm = rf.log[newLogIndex].Term
		
		newLog := make([]LogItem, 1)
		newLog = append(newLog, rf.log[newLogIndex + 1:]...)
		rf.log = newLog
		rf.snapShot = snapshot

		//进行持久化
		raftstate := rf.serializeState()
		rf.persister.Save(raftstate, snapshot)
		
		Log(dClient, "S%d: has saved a snapshot, rf.lastIncludedIndex=%d, rf.lastIncludedTerm=%d", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm)

	}

}

type InstallSnapshotArgs struct {
	//Leader的任期
	Term int
	//Leader的id
	LeaderId int
	//快照中包含的最后日志条目的索引值
	LastIncludedIndex int
	//快照中包含的最后日志条目的任期号
	LastIncludedTerm int

	Snapshot  []byte
}

type InstallSnapshotReply struct {
	//当前任期号（currentTerm），便于领导人更新自己
	Term int
	Success bool
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	
	reply.Term = rf.currentTerm

	if args.Snapshot == nil || len(args.Snapshot) < 1 {
		return
	}
	
	Log(dClient, "S%d:InstallSnapshot,args.Term=%d, rf.currentTerm=%d,args.LastIncludedIndex=%d,rf.lastIncludedIndex=%d.",rf.me, args.Term,rf.currentTerm,args.LastIncludedIndex,rf.lastIncludedIndex)
	if args.Term < rf.currentTerm || args.LastIncludedIndex <= rf.lastIncludedIndex{	
		return
	}

	//重置选举时间
	rf.resetElectionTimerLocked()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.persist()
	}

	if rf.serverState != Follower {
		rf.serverState = Follower	
	}

	Log(dClient, "S%d:has install snapshot from S%d!", rf.me, args.LeaderId)

	rf.leaderId = args.LeaderId
	

	trueLogIndex := args.LastIncludedIndex - rf.lastIncludedIndex
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	if trueLogIndex >= len(rf.log) || rf.log[trueLogIndex].Term != args.Term{
		//情况1：快照会包含没有在接收者日志中存在的信息,Follower会丢弃掉所有的日志
		newLog := make([]LogItem, 1)
		rf.log = newLog
	} else {
		//情况2:接收到的快照是自己日志的前面部分（由于网络重传或者错误）,那么被快照包含的条目将会被全部删除，但是快照后面的条目仍然有效，必须保留。
		rf.log = rf.log[trueLogIndex+1:]
	}

	//应用snapshot到状态机
	applyMsg := ApplyMsg{SnapshotValid:true,
						Snapshot:args.Snapshot,
						SnapshotTerm: args.LastIncludedTerm,
						SnapshotIndex: args.LastIncludedIndex}
	rf.applyCh <- applyMsg
	
	rf.snapShot = args.Snapshot
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex
	rf.nextIndex[rf.me] = rf.lastIncludedIndex + 1
	reply.Success = true

	raftstate := rf.serializeState()
	rf.persister.Save(raftstate, args.Snapshot)

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	//候选者的任期
	Term int 
	//候选者的序号
	CandidateId int
	//候选者最后一条日志条目的序号
	LastLogIndex int
	//候选者最后一条日志条目的任期
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	//是否给予投票
	VoteGranted bool
	//投票者通过候选者的任期更新自己任期的结果
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).	
	reply.VoteGranted = false	
	
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Log(dVote, "S%d:processing vote from S%d! args.Term=%d, rf.currentTerm=%d",rf.me, args.CandidateId, args.Term, rf.currentTerm)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		// 已经是新一轮的term, 之前的投票记录作废
		rf.newTermLocked(args.Term)
	}

	//(check) to do 
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		//满足条件给予投票
		localLastLogIndex := rf.nextIndex[rf.me] - 1
		localLastLogIndexRel := rf.convertIndexRelative(localLastLogIndex)
		localLastLogTerm := 0
		if localLastLogIndexRel > 0 {
			localLastLogTerm = rf.log[localLastLogIndexRel].Term
		} else if localLastLogIndexRel == 0 {
			localLastLogTerm = rf.lastIncludedTerm
		}

		// Log(dClient, "S%d:vote info, args=%d, localLastLogTerm=%d, localLastLogIndex=%d", rf.me,args,localLastLogTerm,localLastLogIndex)

		if args.LastLogTerm > localLastLogTerm || (args.LastLogTerm == localLastLogTerm && args.LastLogIndex >= localLastLogIndex){
			rf.votedFor = args.CandidateId
			rf.currentTerm = args.Term //更新任期
			reply.VoteGranted = true
			rf.persist()
			Log(dVote, "S%d: S%d vote to S%d!",rf.me, rf.me, args.CandidateId)
		} else {
			Log(dVote, "S%d: S%d can't vote to S%d! reaseon: serverState=%d, votedFor=%d",rf.me, rf.me, args.CandidateId, rf.serverState, rf.votedFor)
		}
	} else {
		Log(dVote, "S%d: S%d can't vote to S%d! reaseon: serverState=%d, votedFor=%d, term=%d",rf.me, rf.me, args.CandidateId, rf.serverState, rf.votedFor, rf.currentTerm)
	}

	rf.resetElectionTimerLocked()
	reply.Term = rf.currentTerm
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


type AppendEntriesArgs struct{
	Term  int 
	LearderId int
	PrevLogIndex int

	PrevLogTerm int
	Entries[] LogItem

	LeaderCommit int
}

type AppendEntriesReply struct{
	Term int
	Success bool
	XTerm int
	XIndex int
	XLen int
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		
		for rf.commitIndex <= rf.lastApplied {
			rf.condApply.Wait()
		}

		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied += 1
			lastAppliedRel := rf.convertIndexRelative(rf.lastApplied)

			applyMsg := ApplyMsg{CommandValid: true,
				Command: rf.log[lastAppliedRel].Command,
				CommandIndex: rf.lastApplied,}
			
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		}
		Log(dClient, "S%d: apply logindex=%d", rf.me, rf.lastApplied)
		rf.mu.Unlock()
	}
}

func (rf *Raft) appendFailInfoLocked(args *AppendEntriesArgs, reply *AppendEntriesReply){
	reply.Success = false

	if args.PrevLogIndex >= rf.nextIndex[rf.me] {
		//Follwer中对应PrevLogIndex处的日志是空白，返回空白日志长度
		reply.XLen = rf.nextIndex[rf.me]
	} else {
		//返回Follower中与Leader冲突的Log对应的任期号，以及log index
		argsPrevLogIndexRel := rf.convertIndexRelative(args.PrevLogIndex) 
		conflictIndexRel := argsPrevLogIndexRel
		for conflictIndexRel > 0 && rf.log[conflictIndexRel].Term == rf.log[argsPrevLogIndexRel].Term {
			conflictIndexRel--
		} 
		conflictIndexRel += 1
		reply.XIndex = rf.convertIndexTrue(conflictIndexRel)
		if conflictIndexRel > 1 {
			reply.XTerm = rf.log[conflictIndexRel].Term
		} else {
			reply.XTerm = rf.lastIncludedTerm
		}
		reply.XLen = -1
		// Log(dClient, "S%d: log conflict, args.PrevLogIndex=%d,XIndex=%d,XTerm=%d", rf.me,args.PrevLogIndex,reply.XIndex,reply.XTerm)
	}
	Log(dClient, "S%d: log conflict, args.PrevLogIndex=%d, reply=%d, log =%d", rf.me,args.PrevLogIndex,reply, rf.log)

}

func (rf *Raft)applyLogFollowerLocked(args *AppendEntriesArgs) {
	//提交Follower中已经在Leader中被提交的Log，同时更新rf.commitIndex和rf.commitIndex
	if rf.commitIndex < args.LeaderCommit &&  rf.commitIndex < args.PrevLogIndex {
		// Log(dClient,"S%d: need to apply some logs. PrevLogIndex=%d, args.LeaderCommit=%d, rf.commitIndex=%d. isHeartbeat=%t", rf.me, args.PrevLogIndex, args.LeaderCommit, rf.commitIndex, len(args.Entries)==0)

		//新的rf.commitIndex应该是args.LeaderCommit和Leader发送PrevLogIndex之间的最小值
		commitIndex := args.LeaderCommit
		if commitIndex > args.PrevLogIndex + len(args.Entries) {
			commitIndex = args.PrevLogIndex + len(args.Entries)
		}
		rf.commitIndex = commitIndex
		rf.condApply.Signal()
		Log(dClient, "S%d: commitLogIndex=%d, log=%d", rf.me, rf.commitIndex, rf.log)
	}
}


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//to do：消息不按序到达


	reply.Success = false
	rf.mu.Lock()
	defer rf.mu.Unlock()


	//新Leader处理来自旧Leader的消息
	if rf.currentTerm > args.Term {
		// Log(dClient, "S%d: serverstate change %d -> %d", rf.me, rf.serverState, Follower)
		reply.Term = rf.currentTerm
		return
	} else if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.persist()
	}
	//旧Leader处理来自新Leader的消息，Candidate处理来自新Leader的消息
	if rf.serverState != Follower {
		rf.serverState = Follower	
	}

	//重置随机选举时间
	rf.resetElectionTimerLocked()
	
	rf.leaderId = args.LearderId


	//处理消息时心跳的情况
	argsPrevLogIndexRel := rf.convertIndexRelative(args.PrevLogIndex)
	argsPrevLogTerm := rf.lastIncludedTerm
	if argsPrevLogIndexRel > 0 && argsPrevLogIndexRel < len(rf.log){
		argsPrevLogTerm = rf.log[argsPrevLogIndexRel].Term
	}

	if len(args.Entries) == 0 {
		if args.PrevLogIndex == 0 || (args.PrevLogIndex < rf.nextIndex[rf.me] && argsPrevLogTerm == args.PrevLogTerm) {
			reply.Success = true
			rf.applyLogFollowerLocked(args)
		} else {
			rf.appendFailInfoLocked(args, reply)
		}
		// Log(dClient,"S%d: S%d receive a heartbeate from S%d. log=%d", rf.me, rf.me, args.LearderId, rf.log)
		return
	}

	//处理消息中包含日志的情况
	if args.PrevLogIndex == 0 || (args.PrevLogIndex < rf.nextIndex[rf.me] && argsPrevLogTerm == args.PrevLogTerm)  {
		// Log(dClient,"S%d: receive a entry from S%d. PrevLogIndex=%d PrevLogTerm=%d, args.Term = %d", rf.me, args.LearderId, args.PrevLogIndex ,rf.log[args.PrevLogIndex].Term, args.Term)
		
		logIndex := args.PrevLogIndex + 1
		logIndexRel := rf.convertIndexRelative(logIndex)
		//首先需要清除旧的日志，然后将Leader的新日志逐条添加上
		rf.log = rf.log[:logIndexRel]

		for i := 0; i < len(args.Entries); i++ {
			
			rf.log = append(rf.log, args.Entries[i])
			// Log(dClient, "S%d: receive logindex=%d", rf.me, logIndex + i)
		}
		rf.persist()
		rf.nextIndex[rf.me] = logIndex + len(args.Entries)
		reply.Success = true

		Log(dClient, "S%d: receive logindex %d~%d, log=%d", rf.me, args.PrevLogIndex + 1, rf.nextIndex[rf.me] - 1, rf.log)
		rf.applyLogFollowerLocked(args)
	} else {
	// Log(dClient, "S%d:args.PrevLogIndex=%d, XLen=%d", rf.me,args.PrevLogIndex, reply.XLen)
		if (argsPrevLogIndexRel < len(rf.log)) {
			Log(dClient, "S%d:---log conflict, rf.lastIncludedTerm=%d,rf.lastIncludedIndex=%d, argsPrevLogIndexRel=%d, argsPrevLogIndexTerm=%d, args.PrevLogTerm=%d", rf.me,rf.lastIncludedTerm, rf.lastIncludedIndex,argsPrevLogIndexRel,argsPrevLogTerm, args.PrevLogTerm)
		}

		rf.appendFailInfoLocked(args, reply)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool{
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool{
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
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
	
	index := rf.nextIndex[rf.me]
	term := rf.currentTerm
	isLeader := rf.serverState == Leader

	if !isLeader {
		return index, term, isLeader
	}

	//Leader本地存储日志
	logItem := LogItem{Command: command, Term: term}
	//切片必须使用append才能动态增长
	rf.log = append(rf.log, logItem)
	rf.persist()

	rf.nextIndex[rf.me] = rf.nextIndex[rf.me] + 1
	rf.matchIndex[rf.me] = rf.matchIndex[rf.me] + 1
	
	Log(dLeader, "S%d has saved a log entry. command = %d", rf.me, logItem.Command)
	// Your code here (3B).

	return index, term, isLeader
}

func (rf *Raft) sendLog2AllServers() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendLog2Server(i)
	}
}

func (rf *Raft) convertIndexRelative(index int) int {
	return index - rf.lastIncludedIndex
}

func (rf *Raft) convertIndexTrue(index int) int {
	return index + rf.lastIncludedIndex
}

func (rf *Raft) sendLog2Server(server int) {

	for rf.killed() == false {
		
		rf.mu.Lock()
		if rf.serverState != Leader {
			rf.mu.Unlock()
			return
		}

		nextSendIndex := rf.nextIndex[server]
		if nextSendIndex <= rf.lastIncludedIndex {
			snapshot := rf.persister.ReadSnapshot()
			args := InstallSnapshotArgs{Term: rf.currentTerm,
										LeaderId: rf.me,
										LastIncludedIndex: rf.lastIncludedIndex,
										LastIncludedTerm: rf.lastIncludedTerm,
										Snapshot: snapshot}
			reply := InstallSnapshotReply{}
		
			rf.mu.Unlock()
			go func(){
				Log(dLeader, "S%d: S%d need to install snapshot!", rf.me, server)
				ok := rf.sendInstallSnapshot(server, &args, &reply)
				
				rf.mu.Lock()
				defer rf.mu.Unlock()
				
				if ok && reply.Success{
										
					if rf.contextLostLocked(Leader, args.Term) {
						return
					}

					if reply.Term > rf.currentTerm {
						rf.newTermLocked(reply.Term)
						return
					}

					rf.matchIndex[server] = args.LastIncludedIndex
					rf.nextIndex[server] = args.LastIncludedIndex + 1
				}
			}()
			
			time.Sleep(100 * time.Millisecond)
			continue
		}


		prevLogIndexTrue := nextSendIndex - 1
		prevLogIndexRel := rf.convertIndexRelative(prevLogIndexTrue)
		prevLogTerm := 0
		if prevLogIndexRel > 0  {
			prevLogTerm = rf.log[prevLogIndexRel].Term
		} else {
			prevLogTerm = rf.lastIncludedTerm
		}
		entries := make([]LogItem, 0)
		if nextSendIndex < rf.nextIndex[rf.me] {
			entries = rf.log[prevLogIndexRel + 1:]
		}
		args := AppendEntriesArgs{Term: rf.currentTerm,
			LearderId: rf.me,
			PrevLogIndex: prevLogIndexTrue,
			PrevLogTerm: prevLogTerm,
			Entries: entries,
			LeaderCommit: rf.commitIndex,
		}
		if len(entries) != 0 {
			Log(dLeader, "S%d: send a entry to S%d, log index = %d, PrevLogIndex =%d, nextIndex=%d, log=%d", rf.me, server, nextSendIndex, args.PrevLogIndex, rf.nextIndex, rf.log)
		} else {
			Log(dLeader, "S%d: send a heartbeat to S%d, nextIndex=%d, log=%d.", rf.me, server, rf.nextIndex, rf.log)
		}
		rf.mu.Unlock()

		//在协程中处理结果
		go func(){
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, &args, &reply)

			rf.mu.Lock()

			//检查Leader身份和Term是否发生变化
			if rf.contextLostLocked(Leader, args.Term) {
				rf.mu.Unlock()
				return
			}
			
			if ok {
				// Log(dLeader, "S%d: receive a reply from %d. args.PrevLogIndex=%d, reply=%d", rf.me, server, args.PrevLogIndex, reply)
				if reply.Term > rf.currentTerm {
					rf.newTermLocked(reply.Term)
					rf.mu.Unlock()
					return
				}


				if reply.Success {
					rf.dealHaveEntryLogLocked(server, &args)
				} else {
					if reply.XLen != -1 {
						rf.nextIndex[server] = reply.XLen
					} else {
						newLogIndex := args.PrevLogIndex
						newLogIndexRel := rf.convertIndexRelative(newLogIndex)
						xIndexRel := rf.convertIndexRelative(reply.XIndex)
						for rf.log[newLogIndexRel].Term > reply.XTerm && newLogIndexRel > xIndexRel {
							newLogIndexRel--
						}
						if rf.log[newLogIndexRel].Term == reply.XTerm {
							rf.nextIndex[server] = rf.convertIndexTrue(newLogIndexRel) + 1
						} else {
							rf.nextIndex[server] = reply.XIndex
						}
					}
				}

			}
			rf.mu.Unlock()
		}()
		if len(entries) == 0 {
			time.Sleep(100 * time.Millisecond)
		} else {
			time.Sleep(25 * time.Millisecond)
		}
			
	}
}

func maxIndexBeyondOneHalf(origin []int) int {
	copyIndex := make([]int, len(origin))
	copy(copyIndex, origin)
	sort.Ints(copyIndex)
	mid := len(origin) / 2
	return copyIndex[mid]
}


func (rf *Raft) dealHaveEntryLogLocked(server int, args *AppendEntriesArgs) {
	if args.PrevLogIndex + len(args.Entries) <= rf.matchIndex[server] {
		return
	}
	rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
	rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
	
	targetIndex := maxIndexBeyondOneHalf(rf.matchIndex)
	targetIndexRel := rf.convertIndexRelative(targetIndex)
	//Leader不能直接提交任期不等于Current Term的日志
	if targetIndex > rf.commitIndex && rf.log[targetIndexRel].Term == rf.currentTerm{
		//注意这里不选择index := args.PrevLogIndex + 1的原因
		rf.commitIndex = targetIndex
		rf.condApply.Signal()
		Log(dLeader, "S%d: commitLogIndex=%d, log=%d", rf.me, rf.commitIndex, rf.log)
	}
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

func (rf *Raft) requestVote(server int, args *RequestVoteArgs, sumTickets *int) {
	reply := RequestVoteReply{}
	// Log(dVote, "S%d: S%d -> S%d request to vote! rf.term=%d", rf.me, rf.me, server, args.Term)
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

		//需要再次检查一下Server的role和Term是否已经改变
	if rf.contextLostLocked(Candidate, args.Term) {
		return
	}

	//RPC请求成功后计票
	if ok {
		if reply.VoteGranted {

			if *sumTickets > int(len(rf.peers) / 2) {
				return
			}

			*sumTickets = *sumTickets + 1
			//查票

			if *sumTickets > int(len(rf.peers) / 2) {

				Log(dLeader, "S%d: S%d(%d tickets)will be a leader! log=%d", rf.me, rf.me, *sumTickets, rf.log)
		
				rf.serverState = Leader
		
				//成为Leader后重置nextIndex
				for i := 0; i < len(rf.nextIndex); i++ {
					if i == rf.me {
						continue
					}
					rf.nextIndex[i] = rf.nextIndex[rf.me]

				}
				rf.leaderId = rf.me
				
				rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1

				//面向所有Follower，不需要等待返回结果的空包心跳
				// go rf.longerHeartBeater()
		
				//开始发送日志（包含了发送心跳的程序）
				go rf.sendLog2AllServers()
			}
		} else if reply.Term > rf.currentTerm {
			rf.newTermLocked(reply.Term)
		}
	} else {
		Log(dVote, "S%d: S%d -> S%d RequestVote rpc fail!", rf.me, rf.me, server)
	}
}

func (rf *Raft) sendRequestVoteToAll() {

	lastLogIndex := rf.nextIndex[rf.me] - 1
	lastLogIndexRel := rf.convertIndexRelative(lastLogIndex)
	lastLogTerm := 0
	if lastLogIndexRel > 0 {
		lastLogTerm = rf.log[lastLogIndexRel].Term
	} else {
		lastLogTerm = rf.lastIncludedTerm
	}

	args := RequestVoteArgs{
		Term : rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm: lastLogTerm}

	var sumTickets int = 1

	//请求其他人投票
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.requestVote(i, &args, &sumTickets)
		}
	}
}


func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.serverState != Leader && time.Since(rf.heartbeatTime) > rf.electionTimeout {

			Log(dClient,"S%d: S%d will be a candidate. log =%d", rf.me, rf.me, rf.log)
			rf.serverState = Candidate
			rf.currentTerm += 1
			rf.votedFor = rf.me

			rf.persist()

			rf.sendRequestVoteToAll()
			
			rf.resetElectionTimerLocked()
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 200 + (rand.Int63() % 150)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) longerHeartBeater() {
	for rf.killed() == false {
		rf.mu.Lock()
		term := rf.currentTerm
		if rf.serverState != Leader {
			rf.mu.Unlock()
				return
		}
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			server := i
			go func () {
				rf.mu.Lock()
				if rf.contextLostLocked(Leader, term) {
					rf.mu.Unlock()
					return
				}

				nextSendIndex := rf.nextIndex[server]
				prevLogIndex := nextSendIndex - 1
				prevLogTerm := 0
				if prevLogIndex != 0  {
					prevLogTerm = rf.log[prevLogIndex].Term
				}
				entries := make([]LogItem, 0)
				args := AppendEntriesArgs{Term: rf.currentTerm,
					LearderId: rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm: prevLogTerm,
					Entries: entries,
					LeaderCommit: rf.commitIndex,	
				}
				rf.mu.Unlock()

				reply := AppendEntriesReply{}
				rf.sendAppendEntries(server, &args, &reply)
			}()
		}
		time.Sleep(150 * time.Millisecond)
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

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogItem, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.serverState = Follower
	rf.condApply = sync.NewCond(&rf.mu)

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.leaderId = -1

	rf.resetElectionTimerLocked()
	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())
	//初始化nextIndex数组
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.lastIncludedIndex + len(rf.log)
	}
	
	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}
