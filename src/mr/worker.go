package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "encoding/json"
import "strconv"
import "sort"
import "time"

// task type
const (
	NONE   = iota
	MAP
	REDUCE
	WAIT
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		// ask for new Task
		reply, ok := AskForTask()
		if !ok {
			if !ReplyForTask(&ReplyFromWorker{false, reply.WorkType, reply.TaskNum, nil}) {
				break
			}
			continue
		}
		
		
		if reply.WorkType == MAP {
			// map task
			success, intermediateFiles := MapTask(&reply, mapf)
			if !success {
				if !ReplyForTask(&ReplyFromWorker{false, reply.WorkType, reply.TaskNum, nil}) {
					break
				}
				continue
			}
			ReplyForTask(&ReplyFromWorker{success, reply.WorkType, reply.TaskNum, intermediateFiles})
		} else if reply.WorkType == REDUCE {
			// reduce task
			success := ReduceTask(&reply, reducef)
			if !success {
				if !ReplyForTask(&ReplyFromWorker{false, reply.WorkType, reply.TaskNum, nil}) {
					break
				}
				continue
			}
			ReplyForTask(&ReplyFromWorker{success, reply.WorkType, reply.TaskNum, nil})
		} else if reply.WorkType == WAIT {
			// wait signal
			time.Sleep(2 * time.Second)
		} else {
			// finish signal
			break
		}

	}
}

func AskForTask() (ReplyToWorkerAsk, bool){
	args := AskFromWorker{}
	reply := ReplyToWorkerAsk{}

	ok := call("Coordinator.HandleWorkerAsk", &args, &reply)
	if !ok {
		fmt.Printf("AskForTask call failed!\n")
	}
	return reply, ok
}

func ReplyForTask(args *ReplyFromWorker) bool {
	reply := ReplyToWorkerReply{}
	
	ok := call("Coordinator.HandleWorkerReply", args, &reply)
	if !ok {
		fmt.Printf("ReplyForTask call failed\n")
	}
	return ok
}

func MapTask(reply *ReplyToWorkerAsk, 
	mapf func(string, string) []KeyValue) (bool, []string) {
	// open original file
	filename := reply.File
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return false, nil
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return false, nil
	}

	// call user's map function 
	kva := mapf(filename, string(content))

	// write to intermediate file (temp)
	tempFiles := make([]*os.File, reply.NReduce)
	for i := range(tempFiles) {
		tempFileName := "temp-" + strconv.Itoa(i) + "-"
		tempFiles[i], err = ioutil.TempFile("", tempFileName)
		if err != nil {
			log.Fatalf("cannot create temp file %v", tempFileName)
			return false, nil
		}
		defer tempFiles[i].Close()
		defer os.Remove(tempFileName)
	}
	for _, kv := range(kva) {
		tempIdx := ihash(kv.Key) % reply.NReduce
		enc := json.NewEncoder(tempFiles[tempIdx])
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot write to temp file %v", tempFiles[tempIdx].Name())
			return false, nil
		}
	}

	// rename temp file
	intermediateFiles := make([]string, reply.NReduce)
	for i := range(intermediateFiles) {
		intermediateFiles[i] = "mr-" + strconv.Itoa(reply.TaskNum) + "-" + strconv.Itoa(i)
		if err := os.Rename(tempFiles[i].Name(), intermediateFiles[i]); err != nil {
			log.Fatalf("cannot rename file %v", tempFiles[i].Name())
			return false, nil
		}
	}
	
	return true, intermediateFiles
}

func ReduceTask(reply *ReplyToWorkerAsk,
	reducef func(string, []string) string) bool {
	// read kv from intermediate file
	kva := []KeyValue{}
	for i := 0; i < reply.NMap; i ++ {
		intermediate := "mr-" + strconv.Itoa(i) + "-" +strconv.Itoa(reply.TaskNum)
		file, err := os.Open(intermediate)
		if err != nil {
			log.Fatalf("cannot open %v", intermediate)
			return false
		}
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	// sort by key
	sort.Sort(ByKey(kva))

	// create temp file
	oname := "mr-out-" + strconv.Itoa(reply.TaskNum)
	tempFileName := "temp-" + strconv.Itoa(reply.TaskNum) + "-"
	tempFile, err := ioutil.TempFile("", tempFileName)
	if err != nil {
		log.Fatalf("cannot create temp file %v", tempFileName)
		return false
	}
	defer tempFile.Close()
	defer os.Remove(tempFileName)

	// call Reduce on each distinct key in kva[],
	// and print the result to temp file.
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	// rename temp file
	if err := os.Rename(tempFile.Name(), oname); err != nil {
		log.Fatalf("cannot rename file %v", tempFile.Name())
		return false
	}

	return true
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
