package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskStatus int
type TaskId int

const (
	Ready TaskStatus = iota
	InProgress
	Completed
)

type Task struct {
	Type      TaskType
	Status    TaskStatus
	Filenames []string
	StartTime time.Time
}

type Coordinator struct {
	mu      sync.Mutex
	files   []string
	tasks   map[TaskId]*Task
	nReduce int
}

func (c *Coordinator) MarkTaskAsCompleted(args *WorkerArgs, _ *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.TaskType == Map {
		c.tasks[args.TaskId].Status = Completed
	} else {
		c.tasks[TaskId(int(args.TaskId)+len(c.files))].Status = Completed
	}
	return nil
}

func (c *Coordinator) HandoutTask(_ *CoordinatorArgs, reply *CoordinatorReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := 0; i < len(c.files); i++ {
		taskId := TaskId(i)
		if c.tasks[taskId].Status == Ready {
			c.tasks[taskId].Status = InProgress
			c.tasks[taskId].StartTime = time.Now()
			reply.TaskId = taskId
			reply.TaskType = Map
			reply.Filenames = []string{c.files[i]}
			reply.NReduce = c.nReduce
			return nil
		}
	}

	for i := 0; i < len(c.files); i++ {
		if c.tasks[TaskId(i)].Status != Completed {
			reply.TaskType = Wait
			return nil
		}
	}

	// all map tasks are completed begin handing out reduce tasks
	offset := len(c.files)
	for i := 0; i < c.nReduce; i++ {
		taskId := TaskId(offset + i)
		if c.tasks[taskId].Status == Ready {
			c.tasks[taskId].Status = InProgress
			c.tasks[taskId].StartTime = time.Now()
			reply.TaskId = TaskId(i)
			reply.TaskType = Reduce
			reply.Filenames = c.tasks[taskId].Filenames
			reply.NReduce = c.nReduce
			return nil
		}
	}

	reply.TaskType = Wait
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
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, task := range c.tasks {
		if task.Status != Completed {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.files = files
	c.tasks = make(map[TaskId]*Task)
	c.nReduce = nReduce
	for i, file := range files {
		task := &Task{}
		task.Type = Map
		task.Status = Ready
		task.Filenames = []string{file}
		c.tasks[TaskId(i)] = task
	}

	offset := len(files)
	for i := 0; i < nReduce; i++ {
		task := &Task{}
		task.Type = Reduce
		task.Status = Ready
		// set up filenames
		for j := 0; j < offset; j++ {
			filename := fmt.Sprintf("mr-%d-%d", j, i)
			task.Filenames = append(task.Filenames, filename)
		}
		c.tasks[TaskId(i+offset)] = task
	}

	// goroutine to check for timeouts
	go func() {
		for {
			if c.Done() {
				return
			}
			time.Sleep(500 * time.Millisecond)
			c.mu.Lock()
			for _, task := range c.tasks {
				if task.Status == InProgress && time.Since(task.StartTime).Seconds() > 10 {
					task.Status = Ready
				}
			}
			c.mu.Unlock()
		}
	}()
	c.server()
	return &c
}
