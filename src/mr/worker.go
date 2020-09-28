package mr

import (
	"../mr/commons/functional"
	"../mr/commons/task"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

const (
	MAP    = "MAP"
	REDUCE = "REDUCE"
	EXIT   = "EXIT"
	IDLE   = "IDLE"
)

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// Task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	fmt.Println("Started worker")

	var workerId int = time.Now().Nanosecond()

	for true {
		fmt.Printf("workerId [%v]\n", workerId)
		var taskToProcess task.Task = getTaskToProcess()
		taskType := taskToProcess.TaskType

		switch taskType {

		case EXIT:
			break

		case MAP:
			processMapTask(taskToProcess, mapf, workerId)

		case REDUCE:
			processReduceTask(taskToProcess, reducef)

		}
		time.Sleep(5 * time.Second)
	}
}

func processMapTask(taskToProcess task.Task, mapf func(string, string) []KeyValue, workerId int) {
	fmt.Println("Processing map function")

	var content []byte = readFileForMap(taskToProcess)
	var mapFResult []KeyValue = mapf(taskToProcess.FileNamesToProcess[0], string(content))
	reduceData := getKeyValuesMapForReduce(mapFResult, taskToProcess.NReduceTasks)
	mapFnResultFileNames := writeMapFnResultFiles(taskToProcess, reduceData, workerId)

	updateTask := UpdateMapTask{}
	updateTask.TaskId = taskToProcess.TaskId
	updateTask.LastTaskResult = mapFnResultFileNames

	fmt.Printf("Done processing map function, TaskId[%v]", taskToProcess.TaskId)
	call("Master.UpdateMapTask", &updateTask, &UpdateMapTaskReplay{})
}

func writeMapFnResultFiles(taskToProcess task.Task, data map[int][]KeyValue, workerId int) []string {
	var mapResultFileNames []string

	for reducerIndex := range data {

		fileName := taskToProcess.TaskId + "-" + strconv.Itoa(reducerIndex) + "-workerId-" + strconv.Itoa(workerId)
		file, err := os.Create(fileName)
		if err != nil {
			fmt.Println("Error while opening file " + fileName)
		}
		for _, pair := range data[reducerIndex] {
			fmt.Fprintf(file, "%v %v\n", pair.Key, pair.Value)
		}
		file.Close()
		mapResultFileNames = append(mapResultFileNames, file.Name())
	}

	return mapResultFileNames
}

func getKeyValuesMapForReduce(keyValues []KeyValue, nReduceTask int) map[int][]KeyValue {
	res := map[int][]KeyValue{}
	for _, elem := range keyValues {
		index := ihash(elem.Key) % nReduceTask
		res[index] = append(res[index], elem)
	}
	return res
}

func processReduceTask(taskToProcess task.Task, reducef func(string, []string) string) {
	fmt.Println("Processing \"REDUCE\" function")

	var intermediate []KeyValue

	for _, fileName := range taskToProcess.FileNamesToProcess {
		contentFiltered := readMapTaskFileContentLines(taskToProcess, fileName)

		for _, line := range contentFiltered {
			index := strings.Index(line, " ")
			str := line[0:index]
			intermediate = append(intermediate, KeyValue{
				Key:   str,
				Value: line[index+1:],
			})
		}
	}

	sort.Sort(ByKey(intermediate))
	file := createTempFile()
	defer file.Close()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		_, _ = fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)
		// this is the correct format for each line of Reduce output.

		i = j
	}

	filename := getResultFileName(taskToProcess)

	err := os.Rename(file.Name(), filename)
	if err != nil {
		log.Fatal("Error occurred renaming temp file", err)
	}

	updateTask := UpdateReduceTask{}
	updateTask.TaskId = taskToProcess.TaskId

	fmt.Printf("Done processing reduce function [%v]", taskToProcess.TaskId)
	call("Master.UpdateReduceTask", &updateTask, &UpdateReduceTask{})
}

func getResultFileName(taskToProcess task.Task) string {
	wd, err1 := os.Getwd()
	if err1 != nil {
		log.Panic("ერორი")
	}
	sprintStr := strings.Split(taskToProcess.TaskId, "-")
	reducer := sprintStr[1]

	filename := wd + "/mr-out-" + reducer
	return filename
}

func readMapTaskFileContentLines(taskToProcess task.Task, fileName string) []string {
	file, err := ioutil.ReadFile(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", taskToProcess.FileNamesToProcess)
	}

	content := strings.Split(string(file), "\n")
	contentFiltered := functional.Filter(content, func(str string) bool {
		return strings.TrimSpace(str) != ""
	})
	return contentFiltered
}

func createTempFile() *os.File {
	wd, err := os.Getwd()
	if err != nil {
		log.Panic("ერორი")
	}
	file, err := ioutil.TempFile(wd, "*")
	if err != nil {
		log.Fatal("Error occurred creating temp file", err)
	}
	return file
}

func readFileForMap(taskToProcess task.Task) []byte {
	file, err := os.Open(taskToProcess.FileNamesToProcess[0])
	if err != nil {
		log.Fatalf("cannot open %v", taskToProcess.FileNamesToProcess)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", taskToProcess.FileNamesToProcess)
	}
	file.Close()

	return content
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func getTaskToProcess() task.Task {
	fmt.Println("Sending \"Master.GetTask\" request")
	args := RequestTask{}
	reply := RequestTaskReplay{}

	// send the RPC request, wait for the reply.
	call("Master.GetTask", &args, &reply)

	fmt.Printf("reply.TaskId %v\n", reply.Task.TaskId)
	fmt.Printf("reply.TaskType %v\n", reply.Task.TaskType)
	return reply.Task
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
