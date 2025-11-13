package mr

import (
	"errors"
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
	Id        TaskId
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
	c.tasks[args.TaskId].Status = Completed
	return nil
}

func (c *Coordinator) HandoutTask(_ *CoordinatorArgs, reply *CoordinatorReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Done() {
		return errors.New("coordinator terminated")
	}

	for i := 0; i < len(c.files); i++ {
		if c.tasks[TaskId(i)].Status == Ready {
			c.tasks[TaskId(i)].Status = InProgress
			c.tasks[TaskId(i)].StartTime = time.Now()
			reply.TaskId = TaskId(i)
			reply.TaskType = Map
			reply.Filenames = c.tasks[TaskId(i)].Filenames
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
		if c.tasks[TaskId(offset+i)].Status == Ready {
			c.tasks[TaskId(offset+i)].Status = InProgress
			c.tasks[TaskId(offset+i)].StartTime = time.Now()
			reply.TaskId = TaskId(offset + i)
			reply.TaskType = Reduce
			reply.Filenames = c.tasks[TaskId(offset+i)].Filenames
			fmt.Println("reply filenames len:", len(reply.Filenames))
			reply.NReduce = c.nReduce
			return nil
		}
	}

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
		task.Id = TaskId(i)
		task.Type = Map
		task.Status = Ready
		task.Filenames = []string{file}
		c.tasks[task.Id] = task
	}

	offset := len(files)
	for i := 0; i < nReduce; i++ {
		task := &Task{}
		task.Id = TaskId(offset + i)
		task.Type = Reduce
		task.Status = Ready
		// set up filenames
		for j := 0; j < nReduce; j++ {
			filename := fmt.Sprintf("mr-%d-%d", j, i)
			task.Filenames = append(task.Filenames, filename)
		}
		c.tasks[task.Id] = task
	}

	// goroutine to check for timeouts
	go func() {
		for !c.Done() {
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
