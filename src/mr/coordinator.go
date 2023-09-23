package mr

import (
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	TaskStateInit = iota
	TaskStateDoing
	TaskStateTimeout
	TaskStateDone
)

type Task struct {
	id        int
	state     int
	startTime time.Time
	timer     *time.Timer
	taskType  string

	nReduce int

	file string
	l    sync.RWMutex
}

func (t *Task) CheckTimeout() {
	for {
		select {
		case <-t.timer.C:
			t.Timeout()
			return
		}
	}
}

func (t *Task) Done() {
	t.l.Lock()
	defer t.l.Unlock()
	t.state = TaskStateDone
	t.timer.Stop()
	t.timer = nil
}

func (t *Task) Timeout() {
	t.l.Lock()
	defer t.l.Unlock()
	t.state = TaskStateTimeout
	fmt.Printf("timeout!, task id: %d, type: %s\n", t.id, t.taskType)
	t.timer = nil
}

func (t *Task) Start(timeout time.Duration) {
	t.l.Lock()
	defer t.l.Unlock()
	t.state = TaskStateDoing
	t.startTime = time.Now()
	t.timer = time.NewTimer(timeout)
	go t.CheckTimeout()
}

type Tasks map[int]*Task

func (ts Tasks) FinishTask(id int) {
	t := ts[id]
	if t == nil {
		// dup
		return
	}
	t.Done()
	delete(ts, id)
}

func (ts Tasks) GetTaskNotDoing() *Task {
	for _, t := range ts {
		if t.state != TaskStateDoing {
			return t
		}
	}
	return nil
}

type Coordinator struct {
	// Your definitions here.
	mapTasks    Tasks
	reduceTasks Tasks
	c           chan *Task
	nReduce     int
	timeout     time.Duration

	l sync.RWMutex
}

func rmFile(pattern string) error {
	fs, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}
	for _, f := range fs {
		os.Remove(f)
	}
	return nil
}

func (c *Coordinator) Clean() error {
	if err := rmFile(`map-*`); err != nil {
		return err
	}
	return nil
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	var cur *Task
	c.l.Lock()
	c.l.Unlock()
	cur = c.mapTasks.GetTaskNotDoing()
	if cur == nil && len(c.mapTasks) == 0 {
		cur = c.reduceTasks.GetTaskNotDoing()
	}
	if cur == nil {
		return nil
	}

	reply.TaskType = cur.taskType
	reply.TaskID = cur.id
	reply.File = cur.file
	reply.NReduce = c.nReduce

	cur.Start(c.timeout)
	return nil
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.l.Lock()
	defer c.l.Unlock()
	switch args.TaskType {
	case TaskTypeMap:
		c.mapTasks.FinishTask(args.TaskID)
	case TaskTypeReduce:
		c.reduceTasks.FinishTask(args.TaskID)
	}
	fmt.Printf("task done, %d, %s\n", args.TaskID, args.TaskType)
	reply.OK = true
	return nil
}

func (c *Coordinator) checkDone() bool {
	c.l.RLock()
	defer c.l.RUnlock()
	if len(c.mapTasks) == 0 && len(c.reduceTasks) == 0 {
		fmt.Println("no jobs, coordinator should exit!")
		return true
	}
	return false
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
	return c.checkDone()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:     nReduce,
		timeout:     time.Second * 10,
		mapTasks:    make(map[int]*Task, len(files)),
		reduceTasks: make(map[int]*Task, nReduce),
	}

	// init task
	for i, f := range files {
		t := &Task{
			id:       i,
			state:    TaskStateInit,
			file:     f,
			taskType: TaskTypeMap,
		}
		c.mapTasks[i] = t
	}

	for i := 0; i < c.nReduce; i++ {
		t := &Task{
			id:       i,
			taskType: TaskTypeReduce,
		}
		c.reduceTasks[i] = t
	}

	// Your code here.

	err := c.Clean()
	if err != nil {
		log.Fatal(err)
		return nil
	}
	c.server()
	return &c
}
