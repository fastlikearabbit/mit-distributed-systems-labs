package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type TaskType int

const (
	Map TaskType = iota
	Reduce
	Wait
)

type CoordinatorArgs struct {
}

type CoordinatorReply struct {
	TaskType  TaskType
	TaskId    TaskId
	NReduce   int
	Filenames []string
}

type WorkerArgs struct {
	TaskType TaskType
	TaskId   TaskId
}

// done task
type WorkerReply struct {
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
