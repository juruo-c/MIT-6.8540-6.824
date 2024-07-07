package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

// task status
const (
	WAITING = iota
	RUNNING
	FINISHED
)

type FinishedNum struct {
	num int
	mu sync.Mutex
}

type TaskStatus struct {
	task map[int]int
	tick map[int]int
	mu sync.Mutex
}

type Coordinator struct {
	// Your definitions here.
	files []string
	nReduce int
	intermediateFiles []string
	mapFinished FinishedNum
	reduceFinished FinishedNum
	finishAllTask bool
	mapTask TaskStatus
	reduceTask TaskStatus
	tick int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTaskNum(task *TaskStatus, n int) int{
	task.mu.Lock()
	defer task.mu.Unlock()

	for i := 0; i < n; i ++ {
		if task.task[i] == WAITING {
			task.tick[i] = 0
			task.task[i] = RUNNING
			return i
		}
	}
	return -1
}

func (c *Coordinator) HandleWorkerAsk(args *AskFromWorker, reply *ReplyToWorkerAsk) error {
	// map phase
	c.mapFinished.mu.Lock()
	defer c.mapFinished.mu.Unlock()
	if c.mapFinished.num < len(c.files) {
		reply.TaskNum = c.GetTaskNum(&c.mapTask, len(c.files))
		if reply.TaskNum == -1 {
			reply.WorkType = WAIT
			return nil
		}
		reply.WorkType = MAP
		reply.NReduce = c.nReduce
		reply.File = c.files[reply.TaskNum]
		return nil
	}

	// reduce phase
	c.reduceFinished.mu.Lock()
	defer c.reduceFinished.mu.Unlock()
	if c.reduceFinished.num < c.nReduce {
		reply.TaskNum = c.GetTaskNum(&c.reduceTask, c.nReduce)
		if reply.TaskNum == -1 {
			reply.WorkType = WAIT
			return nil
		}
		reply.WorkType = REDUCE
		reply.NReduce = c.nReduce
		reply.NMap = len(c.files)

		return nil
	}

	// finish all task
	reply.WorkType = NONE
	c.finishAllTask = true
	return nil
}

func (c *Coordinator) HandleWorkerReply(args *ReplyFromWorker, reply *ReplyToWorkerReply) error {
	var (
		finished *FinishedNum
		task *TaskStatus
	)
	if args.WorkType == MAP {
		finished = &c.mapFinished
		task = &c.mapTask
	} else {
		finished = &c.reduceFinished
		task = &c.reduceTask
	}
	finished.mu.Lock()
	defer finished.mu.Unlock()
	task.mu.Lock()
	defer task.mu.Unlock()
	if args.Success {
		// for crash
		if task.task[args.TaskNum] == FINISHED {
			return nil
		}
		c.intermediateFiles = append(c.intermediateFiles, args.IntermediateFiles...)
		task.task[args.TaskNum] = FINISHED
		finished.num ++
	} else {
		task.task[args.TaskNum] = WAITING
	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	c.CheckWorker()

	// Your code here.
	if c.finishAllTask {
		ret = true
	}

	return ret
}

// check crash worker
func (c *Coordinator) CheckWorker() {
	changeTaskStatus := func (taskStatus *TaskStatus) {
		taskStatus.mu.Lock()
		for id, status := range taskStatus.task {
			if status == RUNNING {
				taskStatus.tick[id] ++
				if taskStatus.tick[id] == 10 {
					taskStatus.task[id] = WAITING
				}
			}
		}
		taskStatus.mu.Unlock()
	}

	changeTaskStatus(&c.mapTask)
	changeTaskStatus(&c.reduceTask)
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files[:]
	c.nReduce = nReduce
	c.mapTask.task = make(map[int]int)
	c.reduceTask.task = make(map[int]int)
	c.mapTask.tick = make(map[int]int)
	c.reduceTask.tick = make(map[int]int)

	c.server()
	return &c
}
