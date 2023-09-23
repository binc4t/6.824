package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func getFileContent(path string) []byte {
	file, err := os.Open(path)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", path)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", path)
	}
	return content
}

type Work struct {
	file    string
	id      int
	nReduce int
	retList map[int][]KeyValue

	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

func NewWork(nReduce int, mapf func(string, string) []KeyValue, reducef func(string, []string) string) *Work {
	return &Work{
		nReduce: nReduce,
		retList: make(map[int][]KeyValue, nReduce),
		mapf:    mapf,
		reducef: reducef,
	}
}

func (w *Work) AddKV(kv KeyValue) {
	idx := ihash(kv.Key) % w.nReduce
	if w.retList[idx] == nil {
		w.retList[idx] = make([]KeyValue, 0)
	}
	w.retList[idx] = append(w.retList[idx], kv)
}

func (w *Work) Save() error {
	for i, l := range w.retList {
		f, err := os.OpenFile(genPath(i), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return err
		}
		enc := json.NewEncoder(f)
		for _, kv := range l {
			err = enc.Encode(&kv)
			if err != nil {
				return err
			}
		}
		f.Close()
	}
	return nil
}

func (w *Work) Load() error {
	path := genPath(w.id)
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	if w.retList[w.id] == nil {
		w.retList[w.id] = make([]KeyValue, 0)
	}

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Bytes()

		kv := KeyValue{}
		err = json.Unmarshal(line, &kv)
		if err != nil {
			return err
		}
		w.retList[w.id] = append(w.retList[w.id], kv)
	}
	return nil
}

func (w *Work) DoMap() error {
	b := getFileContent(w.file)
	kvs := w.mapf(w.file, string(b))
	for _, kv := range kvs {
		w.AddKV(kv)
	}
	err := w.Save()
	if err != nil {
		return err
	}
	return nil
}

func (w *Work) DoReduce() error {
	err := w.Load()
	if err != nil {
		return err
	}

	intermediate := w.retList[w.id]
	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(w.id)
	ofile, err := os.Create(oname)
	if err != nil {
		return err
	}

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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
		output := w.reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	return nil
}

func genPath(idx int) string {
	return "map-" + strconv.Itoa(idx)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		t := GetTask()
		if t == nil {
			// fmt.Println("no task to do")
			continue
			//return
		}
		w := NewWork(t.nReduce, mapf, reducef)
		if t.taskType == TaskTypeMap {
			w.file = t.file
			err := w.DoMap()
			if err != nil {
				//fmt.Printf("DoMap err, err: %v\n", err)
			}
		} else if t.taskType == TaskTypeReduce {
			w.id = t.id
			err := w.DoReduce()
			if err != nil {
				//fmt.Printf("DoReduce err, err: %v\n", err)
			}
		}
		FinishTask(t)
		time.Sleep(time.Millisecond * 200)
	}
}

func GetTask() *Task {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)

	if ok && reply.TaskType != "" {
		//fmt.Printf("task get success, task %d, type: %s\n", reply.TaskID, reply.TaskType)
		return &Task{
			id:       reply.TaskID,
			taskType: reply.TaskType,
			file:     reply.File,
			nReduce:  reply.NReduce,
		}
	}
	return nil
}

func FinishTask(t *Task) {
	if t == nil {
		return
	}
	args := TaskDoneArgs{
		TaskType: t.taskType,
		TaskID:   t.id,
	}
	reply := TaskDoneReply{}
	ok := call("Coordinator.TaskDone", &args, &reply)
	if ok && reply.OK {
		//fmt.Printf("task done success, task %d, type: %s\n", t.id, t.taskType)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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
