package mr

import (
	"fmt"
	"log"
	"math"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	lock sync.Mutex

	stage          string
	nMap           int
	nReduce        int
	tasks          map[int]Task // TaskIndex:Task
	availableTasks chan Task
}

type Task struct {
	TaskType    string // MAP or REDUCE
	TaskIndex   int
	MapFileName string
	Deadline    time.Time
	WorkerId    int
}

func tmpMapOutputFile(workerId int, mapIdx int, reduceIdx int) string {
	return fmt.Sprintf("tmp-worker-%d-%d-%d.tmp", workerId, mapIdx, reduceIdx)
}

func finalMapOutputFile(mapIdx int, reduceIdx int) string {
	return fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
}

func tmpReduceOutputFile(workerId int, reduceIndex int) string {
	return fmt.Sprintf("tmp-worker-%d-out-%d.tmp", workerId, reduceIndex)
}

func finalReduceOutputFile(reduceIndex int) string {
	return fmt.Sprintf("mr-out-%d", reduceIndex)
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AskForTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	reply.MapNum = c.nMap
	reply.ReduceNum = c.nReduce
	//log.Printf("AskForTask call: %v, %v, %v\n", args.LastTaskType, args.LastTaskIndex, args.WorkerId)
	if args.LastTaskType != "" {
		c.lock.Lock()
		// 如果已经被超时重新分配则忽略返回结果
		// 否则视作任务完成 从 tasks 中删除任务
		if task, exists := c.tasks[args.LastTaskIndex]; exists && args.WorkerId == task.WorkerId {
			delete(c.tasks, args.LastTaskIndex)
		}

		if len(c.tasks) == 0 {
			if c.stage == MAP {
				log.Printf("all map tasks finished.\n")
				c.stage = REDUCE

				for i := 0; i < c.nReduce; i++ {
					task := Task{
						TaskType:  REDUCE,
						TaskIndex: i,
					}
					c.tasks[i] = task
					c.availableTasks <- task
				}
			} else if c.stage == REDUCE {
				log.Printf("all reduce tasks finished.\n")
				close(c.availableTasks)
				// 标记任务完成
				c.stage = ""
			}
		}
		c.lock.Unlock()
	}

	// 获取一个新任务
	task, ok := <-c.availableTasks
	if !ok {
		return nil
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	log.Printf("Assign %s task %d to worker %d\n", task.TaskType, task.TaskIndex, args.WorkerId)
	task.WorkerId = args.WorkerId
	// 10s 超时
	task.Deadline = time.Now().Add(10 * time.Second)
	c.tasks[task.TaskIndex] = task
	reply.Type = task.TaskType
	reply.TaskIndex = task.TaskIndex
	reply.MapInputFile = task.MapFileName

	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	// Your code here.
	// 获取锁是需要借助 barrier 保证 c.stage 的可见性
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.stage == ""
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		stage:          MAP,
		nMap:           len(files),
		nReduce:        nReduce,
		tasks:          make(map[int]Task, len(files)),
		availableTasks: make(chan Task, int(math.Max(float64(nReduce), float64(len(files))))),
	}
	// Your code here.
	// assign map tasks.
	for i := 0; i < c.nMap; i++ {
		c.tasks[i] = Task{
			TaskType:    MAP,
			TaskIndex:   i,
			MapFileName: files[i],
		}
		c.availableTasks <- c.tasks[i]
	}

	c.server()

	// 自动回收超时 task 并重新分配
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)

			c.lock.Lock()
			for _, task := range c.tasks {
				if task.WorkerId != 0 && time.Now().After(task.Deadline) {
					// 放回队列
					log.Printf(
						"Re-assign %s task %d from worker %d.",
						task.TaskType, task.TaskIndex, task.WorkerId)
					task.WorkerId = 0
					c.availableTasks <- task
				}
			}
			c.lock.Unlock()
		}
	}()

	return &c
}
