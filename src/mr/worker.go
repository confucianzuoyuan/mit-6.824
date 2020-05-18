package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	w := worker{}
	w.mapf = mapf
	w.reducef = reducef
	w.register()
	w.run()
}

type worker struct {
	id      int
	mapf    func(string, string) []KeyValue // 用户编写的map函数
	reducef func(string, []string) string   // 用户编写的reduce函数
}

func (w *worker) run() {
	for {
		t := w.reqTask()
		if !t.Alive {
			DPrintf("worker get task not alive, exit")
			return
		}
		w.doTask(t)
	}
}

// rpc调用，请求一个任务来执行
func (w *worker) reqTask() Task {
	args := TaskArgs{}
	args.WorkerId = w.id
	reply := TaskReply{}

	if ok := call("Master.GetOneTask", &args, &reply); !ok {
		DPrintf("worker get task fail, exit")
		os.Exit(1)
	}
	DPrintf("worker get task: %+v", reply.Task)
	return *reply.Task
}

func (w *worker) doTask(t Task) {
	DPrintf("in do Task")

	switch t.Phase {
	case MapPhase:
		w.doMapTask(t)
	case ReducePhase:
		w.doReduceTask(t)
	default:
		panic(fmt.Sprintf("task phase err: %v", t.Phase))
	}
}

func (w *worker) doMapTask(t Task) {
	contents, err := ioutil.ReadFile(t.FileName)
	if err != nil {
		w.reportTask(t, false, err)
		return
	}

	// w.mapf是一个函数 file -> (w, 1), ...
	kvs := w.mapf(t.FileName, string(contents))
	reduces := make([][]KeyValue, t.NReduce)
	for _, kv := range kvs {
		// 将不同的Key发送到不同的reduce任务中去处理
		idx := ihash(kv.Key) % t.NReduce
		// kv: (w, 1)
		reduces[idx] = append(reduces[idx], kv)
	}

	for idx, l := range reduces {
		fileName := reduceName(t.Seq, idx)
		// 创建临时文件
		f, err := os.Create(fileName)
		if err != nil {
			w.reportTask(t, false, err)
			return
		}
		enc := json.NewEncoder(f)
		for _, kv := range l {
			// 将kv编码成json字符串
			if err := enc.Encode(&kv); err != nil {
				w.reportTask(t, false, err)
			}
		}
		if err := f.Close(); err != nil {
			w.reportTask(t, false, err)
		}
	}
	w.reportTask(t, true, nil)
}

func (w *worker) doReduceTask(t Task) {
	maps := make(map[string][]string)
	for idx := 0; idx < t.NMaps; idx++ {
		fileName := reduceName(idx, t.Seq)
		file, err := os.Open(fileName) // 打开临时文件
		if err != nil {
			w.reportTask(t, false, err)
			return
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			// 解码成kv对象
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0, 100)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}

	res := make([]string, 0, 100)
	for k, v := range maps {
		// 聚合操作
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}

	if err := ioutil.WriteFile(mergeName(t.Seq), []byte(strings.Join(res, "")), 0600); err != nil {
		w.reportTask(t, false, err)
	}

	w.reportTask(t, true, nil)
}

func (w *worker) reportTask(t Task, done bool, err error) {
	if err != nil {
		log.Printf("%v", err)
	}
	args := ReportTaskArgs{}
	args.Done = done
	args.Seq = t.Seq
	args.Phase = t.Phase
	args.WorkerId = w.id
	reply := ReportTaskReply{}
	if ok := call("Master.ReportTask", &args, &reply); !ok {
		DPrintf("report task fail: %+v", args)
	}
}

func (w *worker) register() {
	args := &RegisterArgs{}
	reply := &RegisterReply{}
	if ok := call("Master.RegWorker", args, reply); !ok {
		log.Fatal("reg fail")
	}
	w.id = reply.WorkerId
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
