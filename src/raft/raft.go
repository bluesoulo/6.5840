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

	/*leader中存储的易失状态（再每次选举之后需要重新初始化）*/
	nextIndex[] int				  //对于每一个Server而言，发送到该服务器的下一个日志条目的索引
	matchIndex[] int			  //对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增
}

const (
	electionTimeoutMin time.Duration = 250 * time.Millisecond
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

	rf.persist(false)
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
func (rf *Raft) persist(logChange bool) {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	raftstate := make([]byte, 0)
	if !logChange {
		e.Encode(rf.currentTerm)
		e.Encode(rf.votedFor)
		oldRaftState := rf.persister.Copy().raftstate
		raftstate = append(raftstate, w.Bytes()...)
		if (len(oldRaftState) >= 8) {
			raftstate = append(w.Bytes(), oldRaftState[8:]...)
		}
	} else {
		e.Encode(rf.log[0:len(rf.log)])
		oldRaftState := rf.persister.Copy().raftstate
		raftstate = append(oldRaftState[0:8], w.Bytes()...)
	}
	rf.persister.Save(raftstate, nil)
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
	
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil || 
	   d.Decode(&log) != nil {
		Log(dError, "%d:read persist error. currentTerm=%d,votedFor=%d", rf.me,currentTerm,votedFor)
		return
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
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
		rf.persist(false)
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
		localLastLogTerm := 0
		if localLastLogIndex > 0 {
			localLastLogTerm = rf.log[localLastLogIndex].Term
		}

		if args.LastLogTerm > localLastLogTerm || (args.LastLogTerm == localLastLogTerm && args.LastLogIndex >= localLastLogIndex){
			rf.votedFor = args.CandidateId
			rf.currentTerm = args.Term //更新任期
			reply.VoteGranted = true
			rf.persist(false)
			Log(dVote, "S%d: S%d vote to S%d!",rf.me, rf.me, args.CandidateId)
		} else {
			Log(dVote, "S%d: S%d can't vote to S%d! reaseon: serverState=%d, votedFor=%d",rf.me, rf.me, args.CandidateId, rf.serverState, rf.votedFor)
		}
		// if args.LastLogIndex >= rf.commitIndex {
		// 	rf.votedFor = args.CandidateId
		// 	rf.currentTerm = args.Term //更新任期
		// 	reply.VoteGranted = true
		// 	Log(dVote, "S%d: S%d vote to S%d!",rf.me, rf.me, args.CandidateId)
		// } else {
		// 	Log(dVote, "S%d: S%d can't vote to S%d! reaseon: serverState=%d, votedFor=%d",rf.me, rf.me, args.CandidateId, rf.serverState, rf.votedFor)
		// }
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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//保存当前心跳的时间
	

	reply.Success = false
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.resetElectionTimerLocked()

	//旧leader苏醒后发送心跳到新leader，停止旧的leader
	if rf.currentTerm > args.Term {
		// Log(dClient, "S%d: serverstate change %d -> %d", rf.me, rf.serverState, Follower)
		reply.Term = rf.currentTerm
		return
	}

	if rf.serverState != Follower && rf.currentTerm <= args.Term {
		rf.serverState = Follower	
	}

	rf.newTermLocked(args.Term)



	//提交Follower中已经在Leader中被提交的Log，同时更新rf.commitIndex和rf.commitIndex
	if rf.commitIndex < args.LeaderCommit &&  rf.commitIndex < args.PrevLogIndex && rf.nextIndex[rf.me] > args.PrevLogIndex && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
		// Log(dClient,"S%d: need to apply some logs. PrevLogIndex=%d, args.LeaderCommit=%d, rf.commitIndex=%d. isHeartbeat=%t", rf.me, args.PrevLogIndex, args.LeaderCommit, rf.commitIndex, len(args.Entries)==0)

		//新的rf.commitIndex应该是args.LeaderCommit和Leader发送PrevLogIndex之间的最小值
		commitIndex := args.LeaderCommit
		if commitIndex > args.PrevLogIndex {
			commitIndex = args.PrevLogIndex
		}

		//在Follwer中提交这些日志
		for i := rf.commitIndex + 1; i <= commitIndex; i++ {
			applyMsg := ApplyMsg{CommandValid: true,
								Command: rf.log[i].Command,
								CommandIndex: i,}
			rf.mu.Unlock()

			rf.applyCh <- applyMsg
			
			rf.mu.Lock()
			Log(dClient, "S%d: apply logindex=%d", rf.me, i)
		}
		rf.commitIndex = commitIndex
		rf.lastApplied = commitIndex
	}



	if len(args.Entries) == 0 {
		reply.Success = true
		// Log(dClient,"S%d: S%d receive a heartbeate from S%d.", rf.me, rf.me, args.LearderId)
		return
	}


	//Leader发送日志的PrevLogIndex大于Follower最长日志index
	if args.PrevLogIndex >= rf.nextIndex[rf.me] {
		reply.Success = false
		return
	}


	//Leader发送日志的PrevLogIndex的Term与rf.log[args.PrevLogIndex].Term相同，可以增加日志
	if args.PrevLogIndex == 0 || (args.PrevLogIndex < rf.nextIndex[rf.me] && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm)  {
		// Log(dClient,"S%d: receive a entry from S%d. PrevLogIndex=%d PrevLogTerm=%d, args.Term = %d", rf.me, args.LearderId, args.PrevLogIndex ,rf.log[args.PrevLogIndex].Term, args.Term)
		
		logIndex := args.PrevLogIndex + 1
		//两种情况:（1）当前下标还没有日志，直接append（2）已经有日志那么直接覆盖
		for i := 0; i < len(args.Entries); i++ {
			if logIndex + i < len(rf.log) {
				rf.log[logIndex + i] = args.Entries[i]
			} else {
				rf.log = append(rf.log, args.Entries[i])
			}
			
			Log(dClient, "S%d: receive logindex=%d", rf.me, logIndex + i)
		}
		rf.persist(true)
		rf.nextIndex[rf.me] = logIndex + len(args.Entries)
		reply.Success = true
	} else {
		reply.Success = false
		if args.PrevLogIndex >= rf.nextIndex[rf.me] {
			//Follwer中对应PrevLogIndex处的日志是空白，返回空白日志长度
			reply.XLen = args.PrevLogIndex - rf.nextIndex[rf.me] + 1
		} else {
			//返回Follower中与Leader冲突的Log对应的任期号，以及log index
			conflictIndex := args.PrevLogIndex
			for conflictIndex > 0 && rf.log[conflictIndex].Term == rf.log[args.PrevLogIndex].Term {
				conflictIndex--
			} 
			conflictIndex += 1
			reply.XIndex = conflictIndex
			reply.XTerm = rf.log[conflictIndex].Term
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool{
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	rf.persist(true)

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

func (rf *Raft) sendLog2Server(server int) {

	for rf.killed() == false {
		
		rf.mu.Lock()
		if rf.serverState != Leader {
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
		if nextSendIndex < rf.nextIndex[rf.me] {
			endLogIndex := nextSendIndex
			for endLogIndex < rf.nextIndex[rf.me] && rf.log[endLogIndex].Term == rf.log[nextSendIndex].Term {
				entries = append(entries, rf.log[endLogIndex])				
				endLogIndex++
			}
			// entries = append(entries, rf.log[nextSendIndex])
		}
		args := AppendEntriesArgs{Term: rf.currentTerm,
			LearderId: rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm: prevLogTerm,
			Entries: entries,
			LeaderCommit: rf.commitIndex,
		}
		if len(entries) != 0 {
			Log(dLeader, "S%d: send a entry to S%d, log index = %d, PrevLogIndex =%d", rf.me, server, nextSendIndex, args.PrevLogIndex)
		} 
		// else {
		// 	Log(dLeader, "S%d: send a heartbeat to S%d", rf.me, server)

		// }
		rf.mu.Unlock()

		//发送日志
		if len(entries) != 0 {
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, &args, &reply)

			rf.mu.Lock()
			//检查Leader身份和Term是否发生变化
			if rf.contextLostLocked(Leader, args.Term) {
				rf.mu.Unlock()
				return
			}

			if ok {
				if reply.Term > rf.currentTerm {
					Log(dClient, "S%d: serverstate change %d -> %d", rf.me, rf.serverState, Follower)
					rf.newTermLocked(reply.Term)
				}
				
				if reply.Success {
					rf.dealHaveEntryLogLocked(server, &args)
				} else {
					//根据不同冲突情况重置rf.nextIndex[server]
					if reply.XLen != 0 {
						rf.nextIndex[server] = rf.nextIndex[server] - reply.XLen + 1
					} else {
						newLogIndex := rf.nextIndex[server] - 1
						for rf.log[newLogIndex].Term != reply.XTerm && newLogIndex >= reply.XIndex {
							newLogIndex--
						}
						rf.nextIndex[server] = newLogIndex + 1
					}
					
				}		
			}

			rf.mu.Unlock()
		} else {
			//发送心跳
			go func ()  {
				reply := AppendEntriesReply{}
				ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					
					if reply.Term > rf.currentTerm {
						rf.newTermLocked(reply.Term)
					}
				}
			}()
			// time.Sleep(50 * time.Millisecond)
		}
		time.Sleep(50 * time.Millisecond)
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
	rf.nextIndex[server] = rf.nextIndex[server] + len(args.Entries)
	rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
	
	targetIndex := maxIndexBeyondOneHalf(rf.matchIndex)
	if targetIndex > rf.commitIndex {
		index := args.PrevLogIndex + 1
		rf.commitIndex = targetIndex
		rf.lastApplied = targetIndex
		for index <= targetIndex {
			applyMsg := ApplyMsg{CommandValid: true,
				Command: rf.log[index].Command,
				CommandIndex: index,}
			//发送消息到上层应用
			rf.mu.Unlock()

			rf.applyCh <- applyMsg

			rf.mu.Lock()
			Log(dLeader, "S%d: apply logindex=%d", rf.me, index)
			index++
		}
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
	Log(dVote, "S%d: S%d -> S%d request to vote! rf.term=%d", rf.me, rf.me, server, args.Term)
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

				Log(dLeader, "S%d: S%d(%d tickets)will be a leader!", rf.me, rf.me, *sumTickets)
		
				rf.serverState = Leader
		
				//成为Leader后重置nextIndex
				for i := 0; i < len(rf.nextIndex); i++ {
					if i == rf.me {
						continue
					}
					rf.nextIndex[i] = rf.nextIndex[rf.me]

				}
				
				rf.matchIndex[rf.me] = rf.commitIndex
				// for i := 0; i < len(rf.matchIndex);i++ {
				// 	rf.matchIndex[i] = rf.commitIndex
				// }
		
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
	lastLogTerm := 0
	if lastLogIndex > 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
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

			Log(dClient,"S%d: S%d will be a candidate", rf.me, rf.me)
			rf.serverState = Candidate
			rf.currentTerm += 1
			rf.votedFor = rf.me

			rf.persist(false)

			rf.sendRequestVoteToAll()
		}
		rf.resetElectionTimerLocked()
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 200 + (rand.Int63() % 150)
		time.Sleep(time.Duration(ms) * time.Millisecond)
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

	rf.resetElectionTimerLocked()
	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//初始化nextIndex数组
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = len(rf.log)
	}
	
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
