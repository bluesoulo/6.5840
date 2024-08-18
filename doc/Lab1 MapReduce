## Lab1: MapReduce

### 1.1 整体流程图

<img src="C:\Users\48176\AppData\Roaming\Typora\typora-user-images\image-20240724205320510.png" alt="image-20240724205320510" style="zoom: 67%;" />

MapReduce服务端由Coordinator(Master)和Worker两部分组成，Worker通过向Coordinator发送HandleGetTask RPC请求来获取

Map和Recude任务，Worker完成任务之后通过向Coordinator发送HandleFinishTask请求通知Coordinator已经完成的任务。

### 1.2 Coordinator实现

##### 1.2.1 Coordinator定义

```go
type Coordinator struct {
	//互斥锁保证多个Worker同时调用GetTask时线程安全
	mutex sync.Mutex

	//len(mapFiles) == nMap
	mapFiles []string
	nMapTasks int
	nReduceTasks int

	//追踪分配的map和reduce任务的完成情况
	mapTasksFinished []bool
	mapTasksIssued []time.Time //记录map任务何时开始，如若超时该任务将会被重新分配

	reduceTasksFinished []bool
	reduceTasksIssued []time.Time

	//所有的reduce任务被完成之后，设置isDone=true
	isDone bool
	
	cond *sync.Cond

}
```

##### 1.2.2 HandleGetTask RPC 实现

```go
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
            //发现任务i没有完成
			if !c.mapTasksFinished[i] {
                //判断任务i是否已经被提交给某个Worker，如果已经被提交，那么时候超时还没完成
				if c.mapTasksIssued[i].IsZero() ||
					time.Since(c.mapTasksIssued[i]).Seconds() > 10{
					//任务超时未能完成，map任务重新分配
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
			//任务未能完成，但还未超时则阻塞，防止忙等待
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
```

HandleGetTask实现思路为：

（1）Map任务分配。Coordinator会通过mapTasksFinished检测是否所有的Map任务都完成，如果所有的Map任务都完成，则进入Reduce任务分配阶段；如果mapTasksFinished中有任务未能完成，则检测该任务是否已经超时，如果已经超时那么将该任务重新分配，否则阻塞；

（2）Recude任务分配。Reduce任务分配在所有Map任务完成之后进行，具体过程同Map任务分配相同

##### 1.2.3 HandleFinishTask RPC实现

```go
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
```

每当有Worker向Coordinator发送HandleFinishTask RPC时，Coordinator根据参数设置mapTasksFinished和reduceTasksFinished状态，***每当有任务完成后都尝试唤醒所有在HandleGetTask阻塞的go routine，再次判断一是否所有的任务已经完成***；

##### 1.2.4 MakeCoordinator 初始化

```
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
```

**cond.Signal()和cond.Broadcast()区别**：

`Signal()` 方法唤醒一个等待中的 goroutine。如果没有 goroutine 在等待，那么调用 `Signal()` 不会有任何效果。

`Broadcast()` 方法会唤醒所有正在等待的 goroutine。如果没有 goroutine 在等待，那么调用 `Broadcast()` 也不会有任何效果。

### 1.3 Worker实现

##### 1.3.1 Worker

```go
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		args := TaskGetArgs{}
		reply := TaskGetReply{}
		call("Coordinator.HandleGetTask", &args, &reply)
		switch reply.TaskType{
		case MapTask:
			//do something
			performMapTask(reply.MapFiles, reply.TaskNum, reply.NReduceTasks, mapf)
		case ReduceTask:
			//do something
			performReduceTask(reply.TaskNum, reply.NMapTasks, reducef)
		case Done:
			os.Exit(0)
		default:
			log.Fatalf("Bad Finish Task? %v", reply.TaskType)
		}

		//告诉Master任务已经完成
		finishArgs := TaskFinishArgs{
			TaskType : reply.TaskType,
			TaskNum:reply.TaskNum,
		}
		finishReply := TaskFinishReply{}
		call("Coordinator.HandleFinishTask", &finishArgs, &finishReply)
	}

}
```

Worker通过循环的方式不断通过HandleGetTask拉取任务，任务完成后通过HandleFinishTask通知Coordinator.

##### 1.3.2performMapTask函数实现

```go
//执行Map任务
func performMapTask(filename string, taskNum int, nReduceTasks int, mapf func(string, string) []KeyValue){
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	
	//获取到执行map function的结果：kv键值对
	kva := mapf(filename, string(content))
	
	temFiles := []*os.File{}
	temFilenames := []string{}
	encoders := []*json.Encoder{}
	for i := 0; i < nReduceTasks; i++ {
		temFile, err := ioutil.TempFile("","")
		if err != nil {
			log.Fatalf("cannot open tem file")
		}
		temFiles = append(temFiles, temFile)
		temFilenames = append(temFilenames, temFile.Name())
		enc := json.NewEncoder(temFile)
		encoders = append(encoders, enc)
	}
	for _, kv := range kva {
		//通过hash映射结果到不同的文件
		index := ihash(kv.Key) % nReduceTasks
		encoders[index].Encode(&kv) 
	}

	for _, f := range temFiles {
		f.Close()
	}

	//将临时文件修改为map-x-y的格式
	for i := 0; i < nReduceTasks; i++ {
		outputName := "map-" + strconv.Itoa(taskNum) + "-" + strconv.Itoa(i)
		err := os.Rename(temFilenames[i], outputName)
		if err != nil {
			log.Fatalf("cannot rename filename: %v -> %v", temFilenames[i], outputName)
		}
	}
}
```

##### 1.3.3performReduceTask函数实现

```go
//执行Reduce任务
func performReduceTask(taskNum int, nMapTasks int, reducef func(string, []string) string){
	kva := []KeyValue{}
    //读取map-x(0到nMapTasks)-y中kv数据, y = taskNum(reduce任务的序号)
	for i := 0; i < nMapTasks; i++ {
		filename := "map-" + strconv.Itoa(i) + "-" + strconv.Itoa(taskNum)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			} 
			kva = append(kva, kv)
		}
		file.Close()
	}

	//排序
	sort.Sort(ByKey(kva))

	temFile, err := ioutil.TempFile("","")
	if err != nil {
		log.Fatalf("cannot open tem file")
	}
    //统计所有的序号为n的reduce任务对应的map结果文件，调用Recude函数，然后将结果写入到Recude-n文件中；
	temFileName := temFile.Name()
	begin := 0
	for begin < len(kva) {
		end := begin
		values := []string{}
		for end < len(kva) && kva[begin].Key == kva[end].Key {
			values = append(values, kva[end].Value)
			end++
		}
		//立马将结果执行reducef然后保存起来
		output := reducef(kva[begin].Key, values)
		fmt.Fprintf(temFile, "%v %v\n", kva[begin].Key, output)

		begin = end
	}
	temFile.Close()
	//文件重命名
	outputName := "reduce-" + strconv.Itoa(taskNum)
	err = os.Rename(temFileName, outputName)
	if err != nil {
		log.Fatalf("cannot rename filename: %v -> %v", temFileName, outputName)
	}
}
```

