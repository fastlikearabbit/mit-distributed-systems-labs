package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		reply := CoordinatorReply{}
		ok := call("Coordinator.HandoutTask", &CoordinatorArgs{}, &reply)

		// assume this means the coordinator has exited
		if !ok {
			return
		}

		switch reply.TaskType {
		case Map:
			file, _ := os.Open(reply.Filenames[0])
			content, _ := io.ReadAll(file)
			file.Close()

			kva := mapf(reply.Filenames[0], string(content))

			var outTempFiles []*os.File
			var encoders []*json.Encoder

			for i := 0; i < reply.NReduce; i++ {
				file, err := os.CreateTemp(".", "mr_tmp")
				if err != nil {
					break
				}
				outTempFiles = append(outTempFiles, file)
				encoders = append(encoders, json.NewEncoder(file))
			}

			for _, kv := range kva {
				partition := ihash(kv.Key) % reply.NReduce
				encoders[partition].Encode(kv)
			}

			for i := 0; i < reply.NReduce; i++ {
				filename := fmt.Sprintf("mr-%d-%d", reply.TaskId, i)
				os.Rename(outTempFiles[i].Name(), filename)
			}

			wargs := WorkerArgs{TaskId: reply.TaskId, TaskType: reply.TaskType}
			call("Coordinator.MarkTaskAsCompleted", &wargs, &WorkerReply{})

		case Reduce:
			var intermediate []KeyValue
			for _, filename := range reply.Filenames {
				file, err := os.Open(filename)
				if err != nil {
					continue
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					err := dec.Decode(&kv)
					if err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}

			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprintf("mr-out-%v", reply.TaskId)
			ofile, _ := os.CreateTemp(".", "mr_out_tmp")

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

				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}
			ofile.Close()
			os.Rename(ofile.Name(), oname)

			wargs := WorkerArgs{TaskId: reply.TaskId, TaskType: reply.TaskType}
			ok := call("Coordinator.MarkTaskAsCompleted", &wargs, &WorkerReply{})

			if ok {
				for _, filename := range reply.Filenames {
					err := os.Remove(filename)
					if err != nil {
						continue
					}
				}
			}

		case Wait:
			time.Sleep(100 * time.Millisecond)
		}
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
