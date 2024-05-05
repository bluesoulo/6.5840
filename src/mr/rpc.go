package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type TaskType int
//
// example to show how to declare the arguments
// and reply for an RPC.
//
const (
	MapTask TaskType = 1
	ReduceTask TaskType = 2
	Done TaskType = 3
)

/*
 *Workers向Master请求任务的RPC
*/
type TaskGetArgs struct {
	
}

type TaskGetReply struct {
	//什么类型的任务
	TaskType TaskType

	//map或reduce的任务总数目
	TaskNum int

	//Map任务需要去读取哪一个文件
	MapFiles string

	//Map任务需要将中间结果写入到哪一个文件
	NReduceTasks int

	//need for reduce task(to know how many intermediate files to read)
	NMapTasks int 
}

/*
 *Workers完成任务后，向Master发送结果的RPC
 */

type TaskFinishArgs struct {
	//任务类型
	TaskType TaskType

	//哪一个任务完成
	TaskNum int 
}

type TaskFinishReply struct {

}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
