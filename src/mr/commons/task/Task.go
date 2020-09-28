package task

import (
	sync "sync"
	"time"
)

type TaskState string
type TaskType string

const (
	PENDING    TaskState = "PENDING"
	PROCESSING TaskState = "PROCESSING"
	DONE       TaskState = "DONE"
)

const (
	MAP    TaskType = "MAP"
	REDUCE TaskType = "REDUCE"
	IDLE   TaskType = "IDLE"
)

type Task struct {
	FileNamesToProcess     []string
	TaskId                 string
	TaskType               TaskType
	NReduceTasks           int //for MAP tasks
	State                  TaskState
	taskStartTime          *time.Time
	mutex                  *sync.Mutex
	MapFuncResultFileNames []string //for MAP tasks
}

func newTask(taskId string, fileNamesToProcess []string, nReduceTasks int, taskType TaskType) Task {
	return Task{
		FileNamesToProcess: fileNamesToProcess,
		TaskId:             taskId,
		TaskType:           taskType,
		NReduceTasks:       nReduceTasks,
		State:              PENDING,
		mutex:              &sync.Mutex{},
	}
}

func idleTask() Task {
	return Task{
		TaskType: IDLE,
	}
}
