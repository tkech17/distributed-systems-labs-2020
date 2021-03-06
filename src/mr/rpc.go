package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "../mr/commons/task"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type RequestTask struct {
}

type RequestTaskReplay struct {
	Task task.Task
}

type UpdateMapTask struct {
	TaskId         string
	LastTaskResult []string
}

type UpdateMapTaskReplay struct{}

type UpdateReduceTask struct {
	TaskId string
}

type UpdateReduceTaskReplay struct{}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
