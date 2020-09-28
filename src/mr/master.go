package mr

import (
	"fmt"
	"log"
)
import "net"
import "os"
import "../mr/commons/task"
import "net/rpc"
import "net/http"

type Master struct {
	taskService *task.TasksService
}

func (m *Master) server() {
	fmt.Println("Registering Master Server")
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.taskService.IsReduceTaskDone()
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	fmt.Printf("FileNames[%v]\n", files)
	m := Master{
		taskService: task.NewTaskService(files, nReduce),
	}

	m.server()

	return &m
}

func (m *Master) GetTask(req *RequestTask, resp *RequestTaskReplay) error {
	fmt.Println("Calling Master.GetTask")
	tsk := m.taskService.GetTask()

	fmt.Printf("Task State Is [%v][%v] \n", tsk.TaskId, tsk.State)

	resp.Task = tsk

	return nil
}

func (m *Master) UpdateMapTask(req *UpdateMapTask, resp *UpdateMapTaskReplay) error {
	fmt.Println("Calling Master.UpdateMapTask")
	m.taskService.UpdateMapTask(req.TaskId, req.LastTaskResult)

	return nil
}

func (m *Master) UpdateReduceTask(req *UpdateReduceTask, resp *UpdateReduceTaskReplay) error {
	fmt.Println("Calling Master.UpdateMapTask")
	m.taskService.UpdateReduceTask(req.TaskId)

	return nil
}
