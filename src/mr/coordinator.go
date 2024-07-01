package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

type TaskState int

const (
	Idle       TaskState = iota // Idle represents a task that has not started yet.
	InProgress                  // InProgress represents a task that is currently being worked on.
	Completed                   // Completed represents a task that has been finished.
)

type Task struct {
	state TaskState
	file  []string
}

type Coordinator struct {
	Lock             sync.Mutex
	nReduce          int
	nMap             int
	nMapCompleted    int
	nReduceCompleted int
	ReduceTask       []Task
	MapTask          []Task
	// Your definitions here.
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//

func (c *Coordinator) ReceiveFinishedTask(args *CoordinatorArgs, reply *CoordinatorReply) error {

	if args.TaskType == "map" {
		sort.Strings(args.Filename)

		c.Lock.Lock()
		c.nMapCompleted++
		c.MapTask[args.TaskId].state = Completed
		for i, filename := range args.Filename {
			c.ReduceTask[i].file = append(c.ReduceTask[i].file, filename)
		}


		c.Lock.Unlock()
	} else if args.TaskType == "reduce" {
		c.Lock.Lock()
		c.nReduceCompleted++
		c.ReduceTask[args.TaskId].state = Completed
		c.Lock.Unlock()
	} else {
		println("unknown task type")
		return errors.New("unknown task type")

	}
	return nil
}

func (c *Coordinator) AskTask(args *CoordinatorArgs, reply *CoordinatorReply) error {

	c.Lock.Lock()
	if c.nMapCompleted < c.nMap {
		allocate := -1
		for i, task := range c.MapTask {
			if task.state == Idle {
				reply.TaskId = i
				allocate = i
				reply.Filename = task.file
				reply.TaskType = "map"
				reply.ReduceNum = c.nReduce
				c.MapTask[i].state = InProgress
				go func() {
					time.Sleep(time.Duration(10) * time.Second)
					c.Lock.Lock()
					if c.MapTask[allocate].state == InProgress {
						c.MapTask[allocate].state = Idle
					}
					c.Lock.Unlock()
				}()
				c.Lock.Unlock()
				return nil
			}

		}
		c.Lock.Unlock()
		reply.TaskId = -1
		return nil
	} else if c.nReduceCompleted < c.nReduce {
		allocate := -1
		for i, task := range c.ReduceTask {
			if task.state == Idle {
				reply.TaskId = i
				allocate = i
				reply.Filename = task.file
				reply.TaskType = "reduce"
				task.state = InProgress
				c.ReduceTask[i].state = InProgress
				go func() {
					time.Sleep(time.Duration(10) * time.Second)
					c.Lock.Lock()
					if c.ReduceTask[allocate].state == InProgress {
						c.ReduceTask[allocate].state = Idle
					}
					c.Lock.Unlock()
				}()

				c.Lock.Unlock()
				return nil
			}

		}
		c.Lock.Unlock()
		reply.TaskId = -1
		return nil
	}
	c.Lock.Unlock()
	reply.TaskId = -2
	return nil
}
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	c.Lock.Lock()
	if c.nMapCompleted == c.nMap && c.nReduce == c.nReduceCompleted {
		c.Lock.Unlock()
		return true
	}
	c.Lock.Unlock()
	// Your code here.

	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	for _, filename := range files {
		print(filename)
	}
	println("map task num", len(files), "reduce task num", nReduce)
	m := Coordinator{}
	// Your code here.
	m.nMap = len(files)
	m.nReduce = nReduce
	m.MapTask = make([]Task, m.nMap)
	m.nMapCompleted = 0
	m.nReduceCompleted = 0
	m.Lock = sync.Mutex{}
	for i, file := range files {
		m.MapTask[i] = Task{state: Idle, file: []string{file}}
	}
	m.ReduceTask = make([]Task, m.nReduce)
	m.server()
	// Your code here.
	return &m
}
