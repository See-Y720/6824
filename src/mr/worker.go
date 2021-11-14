package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by hit key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type InterKeyValues struct {
	Key    string   `json:"key"`
	Values []string `json:"values"`
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//read/write file
func WriteToJsonFile(ikv *InterKeyValues, fileName string) error {

	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(file)
	err = enc.Encode(ikv)
	file.Close()
	return err
}

func ReadFromJSonFile(fileName string) (res []*InterKeyValues, err error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(file)
	for {
		var kv InterKeyValues
		if err = dec.Decode(&kv); err != nil {
			if err == io.EOF {
				err = nil
				break
			} else {
				return nil, err
			}
		}
		res = append(res, &kv)
	}
	file.Close()
	return res, err

}

//
// main/mrworker.go calls this function.
//
func reduceWorker(reducef func(string, []string) string, task *GetTaskReply) ( error) {

	var iKVS []*InterKeyValues
	for _, fileName := range task.FileNameForReduce {
		ikvs, err := ReadFromJSonFile(fileName)
		if err != nil {
			return err
		}
		iKVS = append(iKVS, ikvs...)

	}

	groupMap := make(map[string][]string, len(iKVS))
	for _, kvs := range iKVS {
		groupMap[kvs.Key] = append(groupMap[kvs.Key], kvs.Values...)
	}

	outFileName := "mr-out-" + strconv.Itoa(ihash(iKVS[0].Key)%task.NReduce)
	outFile, _ := os.OpenFile(outFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)

	for k, v := range groupMap {
		output := reducef(k, v)
		fmt.Fprintf(outFile, "%v %v\n", k, output)

	}

	outFile.Close()
	return nil

}
func mapWorker(mapf func(string, string) []KeyValue, task *GetTaskReply) (map[int]string, error) {

	file, err := os.Open(task.FileName)
	if err != nil {
		return nil, err
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}
	file.Close()

	kva := mapf(task.FileName, string(content))
	sort.Sort(ByKey(kva))

	i := 0
	retFileNames := make(map[int]string, task.TaskId)
	for i < len(kva) {
		//找到相同的key做聚合
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		interKeyValues := &InterKeyValues{
			Key:    kva[i].Key,
			Values: values,
		}
		hashAddr := ihash(interKeyValues.Key) % task.NReduce
		filename := "mr-" + strconv.Itoa(int(task.TaskId)) + "-" + strconv.Itoa(hashAddr) + ".json"
		//写入json文件
		WriteToJsonFile(interKeyValues, filename)
		retFileNames[hashAddr] = filename
		i = j
	}
	return retFileNames, nil

}
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	//等待五次未拿到任务则自行关闭
	WaitCnt := 0

	for WaitCnt <= 5 {
		getTaskReply := CallGetTask()

		if getTaskReply.NeedWait {
			WaitCnt++
			// fmt.Printf("wait for task,sleep 2s,waitcnt%v \n", WaitCnt)
			time.Sleep(2 * time.Second)
			continue
		}

		WaitCnt = 0
		//执行
		if getTaskReply.IsMapTask {
			// fmt.Printf("GetTask:fileName:%v,id:%v,类型：map \n", getTaskReply.FileName, getTaskReply.TaskId)
			//map task
			outPutFlies, err := mapWorker(mapf, getTaskReply)
			if err != nil {
				log.Fatalf("mapWorker err:%v", err)
				return
			}
			tda := &TaskDoneArgs{
				FileNames: outPutFlies,
				TaskId:    getTaskReply.TaskId,
				IsMapTask: true,
			}
			CallTaskDone(tda)

		} else {
			//reduce task
			// fmt.Printf("GetTask:fileName:%v,id:%v,类型：reduce \n", getTaskReply.FileNameForReduce, getTaskReply.TaskId)
			err := reduceWorker(reducef, getTaskReply)
			if err != nil {
				log.Fatalf("mapWorker err:%v", err)
				return
			}
			tda := &TaskDoneArgs{
				TaskId:   getTaskReply.TaskId,
				IsMapTask: false,
			}
			CallTaskDone(tda)
		}

	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallTaskDone(args *TaskDoneArgs) *TaskDoneReply {
	reply := &TaskDoneReply{}

	call("Coordinator.TaskDone", args, reply)

	return reply

}
func CallGetTask() *GetTaskReply {
	args := &GetTaskArgs{}
	reply := &GetTaskReply{}

	call("Coordinator.GetTask", args, reply)

	return reply

}
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
