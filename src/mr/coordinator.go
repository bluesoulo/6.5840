package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)


type Coordinator struct {
	//互斥锁保证多个Worker同时调用GetTask时线程安全
	mutex sync.Mutex

	//len(mapFiles) == nMap
	mapFiles []string
	nMapTasks int
	nReduceTasks int

	//追踪分配的map和reduce任务
	mapTasksFinished []bool
	mapTasksIssued []time.Time

	reduceTasksFinished []bool
	reduceTasksIssued []time.Time

	//所有的reduce任务被完成之后，设置isDone=true
	isDone bool

	cond *sync.Cond

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (c *Coordinator) HandleGetTask(args *TaskGetArgs, reply * TaskGetReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	reply.NMapTasks = c.nMapTasks
	reply.NReduceTasks = c.nReduceTasks
	//1.Map任务分配
	for {
		//flag作为标记，标记是否还有任务未完成
		flag := false
		for i:= 0; i < c.nMapTasks; i++{
			if !c.mapTasksFinished[i] {
				if c.mapTasksIssued[i].IsZero() ||
					time.Since(c.mapTasksIssued[i]).Seconds() > 10{
					//map任务重新分配
					reply.MapFiles = c.mapFiles[i]
					reply.TaskType = MapTask
					reply.TaskNum = i
					c.mapTasksIssued[i] = time.Now()
					return nil
				} else {
					flag = true
				}
			}
		}
		if !flag {
			//flag=false时代表所有的map任务都已经完成，则可以跳出for循环
			break
		} else {
			//阻塞，防止忙等待
			c.cond.Wait()
		}
	}
	
	//2.Reduce任务分配
	for {
		flag := false
		for i:= 0; i < c.nReduceTasks; i++{
			if !c.reduceTasksFinished[i] {
				if c.reduceTasksIssued[i].IsZero() ||
					time.Since(c.reduceTasksIssued[i]).Seconds() > 10 {
					reply.TaskType = ReduceTask
					reply.TaskNum = i
					c.reduceTasksIssued[i] = time.Now()
					return nil
				} else {
					flag = true
				}
			}
		}
		if !flag {
			break
		} else {
			c.cond.Wait()
		}
	}
	//3.所有任务都完成
	c.isDone = true
	reply.TaskType = Done
	return nil
}

func (c *Coordinator) HandleFinishTask(args *TaskFinishArgs, reply * TaskFinishReply) error{
	c.mutex.Lock()
	defer c.mutex.Unlock()

	switch args.TaskType {
	case MapTask:
		c.mapTasksFinished[args.TaskNum] = true
	case ReduceTask:
		c.reduceTasksFinished[args.TaskNum] = true
	default:
		log.Fatalf("Bad Finish Task? %v", args.TaskType)
	}
	//每当有任务完成后都尝试唤醒所有在HandleGetTask阻塞的go routine，再次判断一是否所有的任务已经完成
	c.cond.Signal()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	ret = c.isDone
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// 初始化Coordinator
	c.mapFiles = files
	c.nMapTasks = len(files)
	c.nReduceTasks = nReduce

	c.mapTasksFinished = make([]bool, len(files))
	c.mapTasksIssued = make([]time.Time, len(files))

	c.reduceTasksFinished = make([]bool, nReduce)
	c.reduceTasksIssued = make([]time.Time, nReduce)	
	c.cond = sync.NewCond(&c.mutex)

	fmt.Printf("create Coordinator!")
	//在Coordinator的创建过程中随之创建一个协程，每隔固定的时间尝试唤醒所有在HandleGetTask阻塞的go routine，再次判断是否所有任务完成
	go func() {
		for {
			c.mutex.Lock()
			c.cond.Broadcast()
			c.mutex.Unlock()
			time.Sleep(5 * time.Second)
		}
	}()

	c.server()
	return &c
}
