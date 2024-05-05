package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }




//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	/**
	 *ihash 函数
	*/
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


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
//执行Reduce任务
func performReduceTask(taskNum int, nMapTasks int, reducef func(string, []string) string){
	kva := []KeyValue{}
	//读取map-x(0到nMapTasks)-y中kv数据
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

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	// the "Coordinator.Example" tells the
// 	// receiving server that we'd like to call
// 	// the Example() method of struct Coordinator.
// 	ok := call("Coordinator.Example", &args, &reply)
// 	if ok {
// 		// reply.Y should be 100.
// 		fmt.Printf("reply.Y %v\n", reply.Y)
// 	} else {
// 		fmt.Printf("call failed!\n")
// 	}
// }

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
