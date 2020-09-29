package task

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

const taskDoingDurationSecs = 10

type TasksService struct {
	tasks     []*Task
	doneTasks int
	mutex     *sync.Mutex
	doOnce    *sync.Once
}

func NewTaskService(mapFileNamesToProcess []string, nReduceTasks int) *TasksService {
	var tasks []*Task = createMapTasks(mapFileNamesToProcess, nReduceTasks)

	return &TasksService{
		tasks:  tasks,
		mutex:  &sync.Mutex{},
		doOnce: &sync.Once{},
	}
}

func createMapTasks(fileNamesToProcess []string, nReduceTasks int) []*Task {
	if fileNamesToProcess == nil {
		return []*Task{}
	}
	var tasks []*Task
	for i, fileName := range fileNamesToProcess {
		var task Task = newTask("map-"+strconv.Itoa(i), []string{fileName}, nReduceTasks, MAP)
		tasks = append(tasks, &task)
	}
	fmt.Printf("Created [%v] \"MAP\" tasks", len(fileNamesToProcess))
	return tasks
}

func (receiver *TasksService) IsMapFinished() bool {
	receiver.mutex.Lock()
	defer receiver.mutex.Unlock()

	if len(receiver.tasks) == 0 {
		log.Fatal("IsMapFinished shows that task is empty")
	}

	var firstTask Task = *receiver.tasks[0]
	if firstTask.TaskType == REDUCE {
		log.Fatal("IsMapFinished shows that task contains REDUCE task")
	}

	return receiver.doneTasks == len(receiver.tasks)
}

func (receiver *TasksService) GetTask() Task {
	receiver.mutex.Lock()
	defer receiver.mutex.Unlock()

	var now time.Time = time.Now()

	var processingTimeOutedTask *Task
	var processingTimeOutedTaskIndex int = -1

	for i, task := range receiver.tasks {
		var taskState TaskState = task.State
		if taskState == PENDING {

			task.State = PROCESSING
			task.taskStartTime = &now
			return *task

		} else if taskState == PROCESSING && processingTimeOutedTaskIndex == -1 {

			var diff time.Duration = now.Sub(*task.taskStartTime)

			if diff.Seconds() > taskDoingDurationSecs {
				processingTimeOutedTask = task
				processingTimeOutedTaskIndex = i
			}
		}
	}

	if processingTimeOutedTaskIndex != -1 {
		processingTimeOutedTask.taskStartTime = &now
		receiver.tasks[processingTimeOutedTaskIndex] = processingTimeOutedTask
		return *processingTimeOutedTask
	}

	return idleTask()
}

func (receiver *TasksService) tryEndMapPhase() {
	fmt.Println("Trying to end map phase")
	for !receiver.IsMapFinished() {
		time.Sleep(5 * time.Second)
	}

	receiver.mutex.Lock()
	defer receiver.mutex.Unlock()

	fmt.Printf("Started ending map phase\n")

	receiver.doneTasks = 0
	var reduceTasks []*Task = createReduceTasks(receiver.tasks)
	fmt.Printf("Num reduce tasks [%v]", len(reduceTasks))
	receiver.tasks = reduceTasks

}

func createReduceTasks(tasks []*Task) []*Task {
	var reduceTasks []*Task
	var mapResultFileNamesPerReducer map[string][]string = getMapResultFileNamesPerReducer(tasks)

	for key := range mapResultFileNamesPerReducer {
		var reduceTask Task = createReduceTaskFromMapTask(key, mapResultFileNamesPerReducer[key], tasks[0].NReduceTasks)
		reduceTasks = append(reduceTasks, &reduceTask)
	}

	return reduceTasks
}

func getMapResultFileNamesPerReducer(tasks []*Task) map[string][]string {
	var mapResultFileNamesPerReducer map[string][]string = map[string][]string{}

	for _, task := range tasks {
		for _, fileName := range task.MapFuncResultFileNames {
			fileNameSplit := strings.Split(fileName, "-")
			var reducerIndex string = fileNameSplit[2]
			mapResultFileNamesPerReducer[reducerIndex] = append(mapResultFileNamesPerReducer[reducerIndex], fileName)
		}
	}

	return mapResultFileNamesPerReducer
}

func createReduceTaskFromMapTask(index string, mapResultFileNames []string, nReduceTasks int) Task {
	return newTask(
		"reduce-"+index,
		mapResultFileNames,
		nReduceTasks,
		REDUCE,
	)
}

func (receiver *TasksService) UpdateMapTask(taskId string, mapFuncResultFileNames []string) {
	receiver.mutex.Lock()
	defer receiver.mutex.Unlock()

	for _, task := range receiver.tasks {
		if taskId == task.TaskId {

			if task.State != DONE {

				task.State = DONE
				task.MapFuncResultFileNames = mapFuncResultFileNames
				receiver.doneTasks += 1

				if receiver.doneTasks == len(receiver.tasks) {
					go receiver.tryEndMapPhase()
				}
			}
			return
		}
	}

}

func (receiver *TasksService) UpdateReduceTask(taskId string) {
	receiver.mutex.Lock()
	defer receiver.mutex.Unlock()

	for _, task := range receiver.tasks {
		if taskId == task.TaskId {

			if task.State == DONE {
				return
			}

			task.State = DONE
			receiver.doneTasks += 1

			return
		}
	}

	log.Fatalf("UpdateMapTask did not find task by task id [%v]", taskId)
}

func (receiver *TasksService) IsReduceTaskDone() bool {
	receiver.mutex.Lock()
	defer receiver.mutex.Unlock()

	if len(receiver.tasks) == 0 {
		log.Fatal("IsReduceTaskDone shows that task is empty")
	}

	var firstTask Task = *receiver.tasks[0]

	return firstTask.TaskType == REDUCE && receiver.doneTasks == len(receiver.tasks)
}
