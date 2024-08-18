### 1. Raft 状态定义

```
type ServerState int
const (
	Follower = iota
	Candidate
	Leader
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	// mu deadlock.Mutex
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	serverState ServerState		  //server状态字段:Follower, Leader, Candidate
	heartbeatTime time.Time    		  //上一次心跳的时间
	electionTimeout time.Duration   //选举超时时间
	applyCh chan ApplyMsg         //Raft发送消息告知上层服务器，让其执行命令



	/*需要持久化在所有Server中的状态*/
	currentTerm int				  //server能看到的最后一个任期
	votedFor int				  //投给的候选人票对应的候选人Id
	log[] LogItem					  //日志条目

	/*所有服务器中的易失状态*/
	commitIndex int				  //已知的最高被提交的日志条目
	lastApplied int				  //最高被应用到状态机的日志条目
	condApply *sync.Cond		  //applier中使用

	/*leader中存储的易失状态（再每次选举之后需要重新初始化）*/
	nextIndex[] int				  //对于每一个Server而言，发送到该服务器的下一个日志条目的索引
	matchIndex[] int			  //对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增

	//快照状态
	snapShot          []byte
	lastIncludedIndex int		//最后一条被快照的日志序号
	lastIncludedTerm int		//最后一条被快照的日志序号

	heartTimers []*time.Time

}
```



### 2. Leader选举

```
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.serverState != Leader && time.Since(rf.heartbeatTime) > rf.electionTimeout {

			Log(dClient,"S%d: S%d will be a candidate. log =%d", rf.me, rf.me)
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
```

​		ticker是一个无限循环函数，每隔一段时间就会判断选举时间是否超时，如果**选举时间超时**那么就会<u>重置ServerState为Candidate，并将currentTerm加1，并投票给自己</u>；然后再向其他Follower发送投票请求，最后重置选举超时时间。

```
func (rf *Raft) sendRequestVoteToAll() {
	
	// rf.nextIndex保存没有修正的结果
	lastLogIndex := rf.nextIndex[rf.me] - 1
	//Sanpshot后，需要修正lastLogIndex
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
		LastLogIndex: lastLogIndex, //最新一条日志的序号
		LastLogTerm: lastLogTerm}	//最新一条日志的任期
	
	//统计票数（自己会投给自己）
	var sumTickets int = 1

	//请求其他人投票
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.requestVote(i, &args, &sumTickets)
		}
	}
}
```

​		RequestVoteArgs中存储了Candidate当前的**任期currentTerm，CandidateId，最后一条日志的下标以及对应的任期**。sumTickets变量用来统计投票情况。

```
func (rf *Raft) requestVote(server int, args *RequestVoteArgs, sumTickets *int) {
	reply := RequestVoteReply{}
	
	//执行请求投票的RPC
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
			
			//已经有超过一半的票，直接返回
			if *sumTickets > int(len(rf.peers) / 2) {
				return
			}

			*sumTickets = *sumTickets + 1
			//查票
			if *sumTickets > int(len(rf.peers) / 2) {

				Log(dLeader, "S%d: S%d(%d tickets)will be a leader! log=%d", rf.me, rf.me, *sumTickets)
		
				rf.serverState = Leader
		
				//成为Leader后重置nextIndex数组
				for i := 0; i < len(rf.nextIndex); i++ {
					if i == rf.me {
						continue
					}
					rf.nextIndex[i] = rf.nextIndex[rf.me]
				}
				
				rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1

				//面向所有Follower，不需要等待返回结果的空包心跳
				// go rf.longerHeartBeater()
		
				//开始发送日志（包含了发送心跳的程序）
				go rf.sendLog2AllServers()
			}
		} else if reply.Term > rf.currentTerm {
			//发现这次选举过程已经是旧的过程，则重置为currentTerm新的Term
			rf.newTermLocked(reply.Term)
		}
	} else {
		Log(dVote, "S%d: S%d -> S%d RequestVote rpc fail!", rf.me, rf.me, server)
	}
}
```

**RequestVote RPC实现**：

```
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

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

		//满足两个投票条件之一投票
		if args.LastLogTerm > localLastLogTerm || 
		(args.LastLogTerm == localLastLogTerm && args.LastLogIndex >= localLastLogIndex){
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
```

### 3. Log传输

启动多个协程为Follower发送日志

```
func (rf *Raft) sendLog2AllServers() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendLog2Server(i)
	}
}

```

nextIndex <= lastIncludedIndex时，需要安装Sanpshot；否则就判断是否发送日志

```
func (rf *Raft) sendLog2Server(server int) {

	for rf.killed() == false {

		<- rf.heartTimers[server].C
		
		rf.mu.Lock()
		if rf.serverState != Leader {
			rf.mu.Unlock()
			return
		}

		nextSendIndex := rf.nextIndex[server]
		//判断是否需要安装Snapshot
		if nextSendIndex <= rf.lastIncludedIndex {
			Log(dLeader, "S%d: S%d need to install snapshot! rf.lastIncludedIndex=%d,nextSendIndex=%d", rf.me, server,rf.lastIncludedIndex,nextSendIndex)

			snapshot := rf.persister.ReadSnapshot()
			args := InstallSnapshotArgs{Term: rf.currentTerm,
										LeaderId: rf.me,
										LastIncludedIndex: rf.lastIncludedIndex,
										LastIncludedTerm: rf.lastIncludedTerm,
										Snapshot: snapshot}
			reply := InstallSnapshotReply{}
			rf.mu.Unlock()
			
			//发送InstallSanpshot RPC请求
			go rf.requestInstallSanpshot(server, &args, &reply);
		} else {
			prevLogIndexTrue := nextSendIndex - 1
			prevLogIndexRel := rf.convertIndexRelative(prevLogIndexTrue)
			prevLogTerm := 0


			if prevLogIndexRel > 0 {
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
			rf.mu.Unlock()

			reply := AppendEntriesReply{}
			//发送AppendEntries RPC
			go rf.requestAppendEntries(server, &args, &reply);
		}
		rf.resetHeartTimer(server, HeartBeatTimeOut)
	}
}
```

InstallSnapshot RPC返回结果处理：

```
func (rf *Raft) requestInstallSanpshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	//发送InstallSnapshot RPC请求
	ok := rf.sendInstallSnapshot(server, args, reply)
				
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
}
```

AppendEntries RPC返回结果处理：

```
func (rf *Raft) requestAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply){
	//发送AppendEntries RPC
	ok := rf.sendAppendEntries(server, args, reply)

	rf.mu.Lock()

	//检查Leader身份和Term是否发生变化
	if rf.contextLostLocked(Leader, args.Term) {
		rf.mu.Unlock()
		return
	}
	
	if ok {
		Log(dLeader, "S%d: receive a reply from S%d. args.PrevLogIndex=%d, reply=%v", rf.me, server, args.PrevLogIndex, reply)
		
		if reply.Term > rf.currentTerm {
			rf.newTermLocked(reply.Term)
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			rf.dealHaveEntryLogLocked(server, args)
		} else {
			//增加日志失败后修正nextIndex
			if reply.XLen != -1 {
				rf.nextIndex[server] = reply.XLen
			} else {
				newLogIndex := args.PrevLogIndex
				newLogIndexRel := rf.convertIndexRelative(newLogIndex)
				xIndexRel := rf.convertIndexRelative(reply.XIndex)

				//处理XIndex在Leader中信息已经不存在的异常的情况的
				if xIndexRel <= 0 {
					rf.nextIndex[server] = reply.XIndex
				} else {
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
	}
	rf.mu.Unlock()
}
```

AppendEntries RPC成功：

```
func (rf *Raft) dealHaveEntryLogLocked(server int, args *AppendEntriesArgs) {
	if args.PrevLogIndex + len(args.Entries) <= rf.matchIndex[server] {
		return
	}
	//修改nextIndex[server]，matchIndex[server]
	rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
	rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
	
	targetIndex := maxIndexBeyondOneHalf(rf.matchIndex)
	targetIndexRel := rf.convertIndexRelative(targetIndex)
	//Leader不能直接提交任期不等于Current Term的日志
	if targetIndex > rf.commitIndex && rf.log[targetIndexRel].Term == rf.currentTerm{
		//注意这里不选择index := args.PrevLogIndex + 1的原因
		rf.commitIndex = targetIndex
		rf.condApply.Signal()
		Log(dLeader, "S%d: commitLogIndex=%d, match=%v, log=%d", rf.me, rf.commitIndex, rf.matchIndex)
	}
}
```

**AppendEntries RPC主要做两件事**：

1. 维持心跳或者新增日志；(心跳或者新增日志成功，返回True；失败则以任期回退的方式告知Leader下一次要发送log的下标)
2. 根据Leader 的commitIndex更新 Follower的commitIndex;

```
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	reply.Success = false
	rf.mu.Lock()
	defer rf.mu.Unlock()


	//新Leader处理来自旧Leader的消息
	if rf.currentTerm > args.Term {
		// Log(dClient, "S%d: serverstate change %d -> %d", rf.me, rf.serverState, Follower)
		reply.Term = rf.currentTerm
		return
	} else if rf.currentTerm < args.Term {
		rf.newTermLocked(args.Term)
		rf.persist()
	}

	//旧Leader处理来自新Leader的消息，Candidate处理来自新Leader的消息
	if rf.serverState != Follower {
		rf.serverState = Follower	
	}

	//重置随机选举时间
	rf.resetElectionTimerLocked()
	
	//解决bug：旧的请求到达后，cut log 使得已经提交的日志被cut
	if args.PrevLogIndex < rf.commitIndex {
		reply.Success = false
		reply.XLen = rf.commitIndex + 1
		return
	}

	//处理消息时心跳的情况
	argsPrevLogIndexRel := rf.convertIndexRelative(args.PrevLogIndex)
	argsPrevLogTerm := rf.lastIncludedTerm
	if argsPrevLogIndexRel < 0 {
		//处理心跳落后于snapShot的情况
		reply.Success = false
		reply.XLen = rf.lastIncludedIndex + 1
		return
	} else if argsPrevLogIndexRel > 0 && argsPrevLogIndexRel < len(rf.log){
		argsPrevLogTerm = rf.log[argsPrevLogIndexRel].Term
	}


	if len(args.Entries) == 0 {
		if args.PrevLogIndex == 0 || (args.PrevLogIndex < rf.nextIndex[rf.me] && argsPrevLogTerm == args.PrevLogTerm) {
			reply.Success = true
			//根据Leader中的CommitIndex更新本地Follower的CommitIndex
			rf.applyLogFollowerLocked(args)
		} else {
			rf.appendFailInfoLocked(args, reply)
		}
		return
	}


	//处理消息中包含日志的情况
	if args.PrevLogIndex == 0 || (args.PrevLogIndex < rf.nextIndex[rf.me] && argsPrevLogTerm == args.PrevLogTerm)  {
		// Log(dClient,"S%d: receive a entry from S%d. PrevLogIndex=%d PrevLogTerm=%d, args.Term = %d", rf.me, args.LearderId, args.PrevLogIndex ,rf.log[args.PrevLogIndex].Term, args.Term)
		
		logIndex := args.PrevLogIndex + 1
		logIndexRel := rf.convertIndexRelative(logIndex)

		endIndexRel := logIndexRel + len(args.Entries)
		//处理消息不按序到达问题：
		//2种情况需要作截断处理（针对请求不按序到达使得旧请求截断新完整的日志）：
		//情况1：新增日志后的日志长度比当前长
		//情况2: 新增日志后的日志长度比当前短，且末尾日志任期不同
		if endIndexRel >= len(rf.log) || args.Entries[len(args.Entries)-1].Term != rf.log[endIndexRel].Term{
			rf.log = rf.log[:logIndexRel]
			rf.log = append(rf.log, args.Entries...)
			rf.persist()
			rf.nextIndex[rf.me] = logIndex + len(args.Entries)
		}

		reply.Success = true
		rf.applyLogFollowerLocked(args)
	} else {
		rf.appendFailInfoLocked(args, reply)
	}
}
```

AppendEntries RPC中心跳成功或日志新增成功：

```
func (rf *Raft)applyLogFollowerLocked(args *AppendEntriesArgs) {
	//提交Follower中已经在Leader中被提交的Log，同时更新rf.commitIndex
	if rf.commitIndex < args.LeaderCommit &&  rf.commitIndex < args.PrevLogIndex {
	
		//新的rf.commitIndex应该是args.LeaderCommit和Leader发送PrevLogIndex之间的最小值
		commitIndex := args.LeaderCommit
		if commitIndex > args.PrevLogIndex + len(args.Entries) {
			commitIndex = args.PrevLogIndex + len(args.Entries)
		}
		rf.commitIndex = commitIndex
		rf.condApply.Signal()
		Log(dClient, "S%d: commitLogIndex=%d, log=%d", rf.me, rf.commitIndex)
	}
}
```

AppendEntries RPC中发生日志冲突后回复：

```
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
	Log(dClient, "S%d: log conflict, args.PrevLogIndex=%d, reply=%d, log =%d", rf.me,args.PrevLogIndex,reply)

}
```

### 4.Snapshot

InstallSnapshot RPC 实现：

```
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm

	if args.Snapshot == nil || len(args.Snapshot) < 1 {
		return
	}
	
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


	trueLogIndex := args.LastIncludedIndex - rf.lastIncludedIndex
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	//在logIndex=0位置创建一个占位log
	newLog := make([]LogItem, 1)
	if trueLogIndex >= len(rf.log) || rf.log[trueLogIndex].Term != args.Term{
		//情况1：快照会包含没有在接收者日志中存在的信息,Follower会丢弃掉所有的日志
		rf.log = newLog
	} else {
		//情况2:接收到的快照是自己日志的前面部分（由于网络重传或者错误）,那么被快照包含的条目将会被全部删除，但是快照后面的条目仍然有效，必须保留。
		rf.log = rf.log[trueLogIndex+1:]
		rf.log = append(newLog, rf.log...)
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
	
	Log(dClient, "S%d:has install snapshot from S%d! rf.lastIncludedIndex=%v", rf.me, args.LeaderId, rf.lastIncludedIndex)
}
```

#### 补充1：

如果根据matchIndex数组确定commitIndex？

```
//对matchIndex排序，中间位置为commitIndex
func maxIndexBeyondOneHalf(origin []int) int {
	copyIndex := make([]int, len(origin))
	copy(copyIndex, origin)
	sort.Ints(copyIndex)
	mid := len(origin) / 2
	return copyIndex[mid]
}
```

#### 补充2：

有新日志到来时加速发送日志实现

```
func (rf *Raft) resetHeartTimer(server int, timeStamp int) {
	rf.heartTimers[server].Reset(time.Duration(timeStamp) * time.Millisecond)
}
```

```
func (rf *Raft) sendLog2Server(server int) {
	for rf.killed() == false {
		
		//Timer不为0前将会阻塞
        <- rf.heartTimers[server].C
        ...
        //每一次发送完成后重置Timer为HeartBeatTimeOut
        rf.resetHeartTimer(server, HeartBeatTimeOut)
	}
}
```

```
func (rf *Raft) Start(command interface{}) (int, int, bool) {
		...
		
	//每当有新日志到来时加速发送心跳
	defer func() {
		for i := 0; i < len(rf.heartTimers); i++ {
			//有新日志到来时，重置Timer为1，马上开始发送日志；
			rf.resetHeartTimer(i, 1)
		}
	}()

	return index, term, isLeader
}
```



### BUG总结：

#### 1.Leader不能直接提交任期不等于CurrentTerm的日志

具体原因查看Raft Paper！

#### 2. 因为使用了SnapShot后，导致log index出现“相对”和“真实”两种情况，进而导致数组越界问题：

requestAppendEntries函数中处理AppendEntries请求添加日志失败的的情况：

```
//xIndexRel位置的日志已经做了Snapshot，需要进行特判！
if xIndexRel <= 0 {
	rf.nextIndex[server] = reply.XIndex
} else {
	for rf.log[newLogIndexRel].Term > reply.XTerm && newLogIndexRel > xIndexRel {
		newLogIndexRel--
	}
    if rf.log[newLogIndexRel].Term == reply.XTerm {
        rf.nextIndex[server] = rf.convertIndexTrue(newLogIndexRel) + 1
    } else {
        rf.nextIndex[server] = reply.XIndex
    }
}
```

#### 3.AppendEntries RPC不按序到达的情况

**情况1**：旧请求到达后，args.PrevLogIndex < rf.commitIndex (args.PrevLogIndex已经做了SnapShot)

```
	//解决bug：旧的请求到达后，cut log 使得已经提交的日志被cut
	if args.PrevLogIndex < rf.commitIndex {
		reply.Success = false
		reply.XLen = rf.commitIndex + 1
		return
	}

	//处理消息时心跳的情况
	argsPrevLogIndexRel := rf.convertIndexRelative(args.PrevLogIndex)
	argsPrevLogTerm := rf.lastIncludedTerm
	if argsPrevLogIndexRel < 0 {
		//处理心跳落后于snapShot的情况
		reply.Success = false
		reply.XLen = rf.lastIncludedIndex + 1
		return
	} else if argsPrevLogIndexRel > 0 && argsPrevLogIndexRel < len(rf.log){
		argsPrevLogTerm = rf.log[argsPrevLogIndexRel].Term
	}
```

**情况2**：旧请求到达后，截断新请求增加的日志

```
//处理消息不按序到达问题：
//2种情况需要作截断处理（针对请求不按序到达使得旧请求截断新完整的日志）：
//情况1：新增日志后的日志长度比当前长
//情况2: 新增日志后的日志长度比当前短，且末尾日志任期不同
if endIndexRel >= len(rf.log) || args.Entries[len(args.Entries)-1].Term != rf.log[endIndexRel].Term{
    rf.log = rf.log[:logIndexRel]
    rf.log = append(rf.log, args.Entries...)
    rf.persist()
    rf.nextIndex[rf.me] = logIndex + len(args.Entries)
}
```

#### 4.InstallSnapshot RPC不按序到达的情况

```
//在logIndex=0位置创建一个占位log
	newLog := make([]LogItem, 1)
	if trueLogIndex >= len(rf.log) || rf.log[trueLogIndex].Term != args.Term{
		//情况1：快照会包含没有在接收者日志中存在的信息,Follower会丢弃掉所有的日志
		rf.log = newLog
	} else {
		//情况2:接收到的快照是自己日志的前面部分（由于网络重传或者错误）,那么被快照包含的条目将会被全部删除，但是快照后面的条目仍然有效，必须保留。
		rf.log = rf.log[trueLogIndex+1:]
		rf.log = append(newLog, rf.log...)
	}
```

#### 5.每一个任期只能选一个Candidate

AppendEntries RPC中，rf.currentTerm <= args.Term的情况下，都重置了VoteFor=-1；如果有两个Candidate，Candidate 1选举为Leader后，发送了AppendEntries 到Follower，那么会重置VoteFor=-1，则是如果Candidate 2的消息由于网络原因到达后，又可能会投票到Candidate 2
