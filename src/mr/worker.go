package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type WorkerState int

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	workerType string
	workerTask []string
	TaskId     int //
	nReduce    int
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	m_worker := worker{}

	for {
		reply := AskTask(&m_worker)
		if reply.TaskId == -1 {
			continue
		} else if reply.TaskId == -2 {
			println("the task has done kill the worker")
			os.Exit(0)
		}

		m_worker.workerTask = reply.Filename
		m_worker.nReduce = reply.ReduceNum
		m_worker.TaskId = reply.TaskId
		if reply.TaskType == "map" {
			m_worker.workerType = reply.TaskType

			content := openFile(reply.Filename[0])
			kva := mapf(reply.Filename[0], string(*content))
			sort.Sort(ByKey(kva))
			filenames := emit(&m_worker, &kva)
			CompleteTask(&m_worker, filenames)
		} else if reply.TaskType == "reduce" {

			m_worker.workerType = reply.TaskType
			oname := fmt.Sprintf("mr-out-%v", m_worker.TaskId)
			ofile, _ := os.CreateTemp("", "mr-temp-out")
			data := []KeyValue{}
			for _, reduceFile := range reply.Filename {
				content, _ := ReadFile(reduceFile)
				data = append(data, *content...)
			}

			sort.Sort(ByKey(data))
			i := 0
			for i < len(data) {
				j := i + 1
				for j < len(data) && (data)[j].Key == (data)[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, (data)[k].Value)
				}
				output := reducef((data)[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", (data)[i].Key, output)

				i = j
			}
			os.Rename(ofile.Name(), oname)
			ofile.Close()
			filename := []string{oname}
			CompleteTask(&m_worker, &filename)
		} else {
			println("receive unknown task type")
		}
	}

}
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}
func emit(w *worker, data *[]KeyValue) *[]string {
	filedata := make([][]KeyValue, w.nReduce)
	filenames := []string{}
	for _, d := range *data {
		reduceTaskNum := ihash(d.Key) % w.nReduce
		filedata[reduceTaskNum] = append(filedata[reduceTaskNum], d)
	}
	for i, data := range filedata {
		filename := fmt.Sprintf("mr-%v-%v", w.TaskId, i)
		WriteFile(filename, &data)
		filenames = append(filenames, filename)
	}
	return &filenames
}

func AskTask(m_worker *worker) CoordinatorReply {
	args := CoordinatorArgs{
		TaskId: -1,
	}
	reply := CoordinatorReply{}
	ok := call("Coordinator.AskTask", &args, &reply)
	if ok {
		return reply
	} else {
		reply.TaskId = -2
		println("AskTask the master may be done")
		return reply
	}
}

func CompleteTask(m_worker *worker, filename *[]string) CoordinatorReply {
	args := CoordinatorArgs{
		TaskId:   m_worker.TaskId,
		TaskType: m_worker.workerType,
		Filename: *filename,
	}
	reply := CoordinatorReply{}
	ok := call("Coordinator.ReceiveFinishedTask", &args, &reply)
	if ok {
		return reply
	} else {
		reply.TaskId = -2
		println("ReceiveFinishedTask the master may be done")
		return reply
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
func WriteFile(filename string, data *[]KeyValue) error {

	file, err := os.CreateTemp("", "mr-*")
	if err != nil {
		return err
	}
	defer file.Close()
	enc := json.NewEncoder(file)
	for _, record := range *data {
		err := enc.Encode(&record)
		if err != nil {
			println("writetempfile failed")
			return err
		}
	}
	err = os.Rename(file.Name(), filename)
	if err != nil {
		println("rename failed")
	}
	return nil
}
func ReadFile(filename string) (*[]KeyValue, error) {
	file, err := os.Open(filename)
	if err != nil {
		println("ReadFile open file failed")
		os.Exit(-1)
		return nil, err
	}
	defer file.Close()

	var data []KeyValue
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		data = append(data, kv)
	}
	return &data, nil
}

func openFile(filename string) *[]byte {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return &content
}
