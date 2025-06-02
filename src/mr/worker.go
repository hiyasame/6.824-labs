package mr

import (
	"fmt"
	"io"
	"os"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	var prevTaskIdx = 0
	var prevTaskType = ""

	// Your worker implementation here.
	for {
		args := AskForTaskArgs{
			WorkerId: os.Getpid(),
		}

		if prevTaskType != "" {
			args.LastTaskIndex = prevTaskIdx
			args.LastTaskType = prevTaskType
		}

		reply := AskForTaskReply{}

		ok := call("Coordinator.AskForTask", &args, &reply)

		if !ok {
			log.Fatalf("worker %v: AskForTask RPC failed\n", os.Getpid())
		}

		if reply.Type == MAP {
			file, err := os.Open(reply.MapInputFile)
			if err != nil {
				log.Fatalf("worker %v: open map input file failed\n", os.Getpid())
			}
			all, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("worker %v: read map input file failed\n", os.Getpid())
			}
			kvs := mapf(reply.MapInputFile, string(all))
			buckets := make(map[int][]KeyValue)
			for _, kv := range kvs {
				i := ihash(kv.Key) % reply.ReduceNum
				buckets[i] = append(buckets[i], kv)
			}
			// write intermediate file
			for i, kvs := range buckets {
				fileName := tmpMapOutputFile(os.Getpid(), reply.TaskIndex, i)
				file, err := os.Create(fileName)
				if err != nil {
					log.Fatalf("worker %v: create map temp out file failed\n", os.Getpid())
				}
				for _, kv := range kvs {
					_, err := fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
					if err != nil {
						log.Fatalf("worker %v: write map temp out file failed\n", os.Getpid())
					}
				}
				err = file.Close()
				if err != nil {
					log.Fatalf("worker %v: close map temp out file failed\n", os.Getpid())
				}
			}
			err = file.Close()
			if err != nil {
				log.Fatalf("worker %v: close map input file failed\n", os.Getpid())
			}

			// rename phase
			for i, _ := range buckets {
				err := os.Rename(tmpMapOutputFile(os.Getpid(), reply.TaskIndex, i), finalMapOutputFile(reply.TaskIndex, i))
				if err != nil {
					log.Fatalf(
						"Failed to rename map output file `%s` as final: %e\n",
						tmpMapOutputFile(args.WorkerId, args.LastTaskIndex, i), err)
				}
			}
		} else if reply.Type == REDUCE {
			kvs := make(map[string][]string, 0)
			for i := 0; i < reply.MapNum; i++ {
				fileName := finalMapOutputFile(i, reply.TaskIndex)
				_, err := os.Stat(fileName)
				// 不存在就跳过
				if os.IsNotExist(err) {
					continue
				}
				file, err := os.Open(fileName)
				if err != nil {
					log.Fatalf("worker %v: open reduce temp out file failed\n", os.Getpid())
				}
				var key string
				var value string
				for {
					_, err = fmt.Fscanf(file, "%v %v\n", &key, &value)
					if err == io.EOF {
						break
					}
					if err != nil {
						log.Fatalf("worker %v: scan map temp out file failed\n", os.Getpid())
					}
					kvs[key] = append(kvs[key], value)
				}
				_ = file.Close()
			}
			reduceOutFileName := tmpReduceOutputFile(os.Getpid(), reply.TaskIndex)
			reduceOutFile, _ := os.Create(reduceOutFileName)
			for k, values := range kvs {
				value := reducef(k, values)
				_, err := fmt.Fprintf(reduceOutFile, "%v %v\n", k, value)
				if err != nil {
					log.Fatalf("worker %v: write reduce temp out file failed\n", os.Getpid())
				}
			}
			_ = reduceOutFile.Close()

			err := os.Rename(
				tmpReduceOutputFile(os.Getpid(), reply.TaskIndex),
				finalReduceOutputFile(reply.TaskIndex),
			)
			if err != nil {
				log.Fatalf(
					"Failed to rename reduce output file `%s` as final: %e\n",
					tmpReduceOutputFile(args.WorkerId, args.LastTaskIndex), err)
			}
		} else {
			// 任务结束 worker 退出
			log.Printf("worker %v exited.\n", args.WorkerId)
			return
		}

		prevTaskIdx = reply.TaskIndex
		prevTaskType = reply.Type
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
