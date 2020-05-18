package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	TaskStatusReady   = 0
	TaskStatusQueue   = 1
	TaskStatusRunning = 2
	TaskStatusFinish  = 3
	TaskStatusErr     = 4
)

const (
	MaxTaskRunTime   = time.Second * 5
	ScheduleInterval = time.Millisecond * 500
)

type TaskStat struct {
	Status    int       // 任务状态
	WorkerId  int       // worker的id
	StartTime time.Time // 任务开始时间
}

type Master struct {
	files     []string  // 待处理的文件
	nReduce   int       // reduce的任务集合
	taskPhase TaskPhase // 当前任务处于的阶段
	taskStats []TaskStat
	mu        sync.Mutex
	done      bool // 是否完成
	workerSeq int  // worker的计数器
	taskCh    chan Task
}

func (m *Master) getTask(taskSeq int) Task {
	task := Task{
		FileName: "",
		NReduce:  m.nReduce,
		NMaps:    len(m.files),
		Seq:      taskSeq,
		Phase:    m.taskPhase,
		Alive:    true,
	}
	DPrintf("m:%+v, taskseq:%d, lenfiles:%d, lents:%d", m, taskSeq, len(m.files), len(m.taskStats))
	// 如果处于map阶段，则文件名为第taskSeq个文件的文件名
	// map的任务数量等于文件的数量
	if task.Phase == MapPhase {
		task.FileName = m.files[taskSeq]
	}
	return task
}

// 启动map任务
func (m *Master) initMapTask() {
	m.taskPhase = MapPhase
	// map的任务数量是文件的数量
	m.taskStats = make([]TaskStat, len(m.files))
}

// 启动reduce任务
func (m *Master) initReduceTask() {
	m.taskPhase = ReducePhase
	m.taskStats = make([]TaskStat, m.nReduce)
}

func (m *Master) schedule() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 任务结束，直接返回
	if m.done {
		return
	}
	allFinish := true
	for index, t := range m.taskStats {
		switch t.Status {
		case TaskStatusReady: // 任务准备好，放入队列
			allFinish = false
			m.taskCh <- m.getTask(index)
			m.taskStats[index].Status = TaskStatusQueue
		case TaskStatusQueue:
			allFinish = false
		case TaskStatusRunning:
			allFinish = false
			// 如果任务超时，重新放入队列
			if time.Now().Sub(t.StartTime) > MaxTaskRunTime {
				m.taskStats[index].Status = TaskStatusQueue
				m.taskCh <- m.getTask(index)
			}
		case TaskStatusFinish: // 如果任务完成，什么都不做
		case TaskStatusErr:
			allFinish = false
			m.taskStats[index].Status = TaskStatusQueue
			m.taskCh <- m.getTask(index)
		default:
			panic("t.status err")
		}
	}
	if allFinish {
		// 如果map任务结束，开启reduce任务
		if m.taskPhase == MapPhase {
			m.initReduceTask()
		} else {
			m.done = true
		}
	}
}

// 注册一个任务
func (m *Master) regTask(args *TaskArgs, task *Task) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if task.Phase != m.taskPhase {
		panic("req Task phase neq")
	}

	m.taskStats[task.Seq].Status = TaskStatusRunning
	m.taskStats[task.Seq].WorkerId = args.WorkerId
	m.taskStats[task.Seq].StartTime = time.Now()
}

// 由worker调用GetOneTask这个rpc
func (m *Master) GetOneTask(args *TaskArgs, reply *TaskReply) error {
	task := <-m.taskCh
	reply.Task = &task

	if task.Alive {
		// 注册任务
		m.regTask(args, &task)
	}

	return nil
}

// 供worker调用的rpc服务
func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.taskPhase != args.Phase || args.WorkerId != m.taskStats[args.Seq].WorkerId {
		return nil
	}

	if args.Done {
		m.taskStats[args.Seq].Status = TaskStatusFinish
	} else {
		m.taskStats[args.Seq].Status = TaskStatusErr
	}

	go m.schedule()
	return nil
}

// RegWorker 注册一个worker，并返回workerid
func (m *Master) RegWorker(args *RegisterArgs, reply *RegisterReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workerSeq++
	reply.WorkerId = m.workerSeq
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	// 将Master注册为rpc服务
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.done
}

// 如果没结束，继续调度执行
func (m *Master) tickSchedule() {
	for !m.Done() {
		go m.schedule()
		time.Sleep(ScheduleInterval)
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.mu = sync.Mutex{}
	m.nReduce = nReduce
	m.files = files

	// 创建有缓冲的通道作为队列使用
	if nReduce > len(files) {
		m.taskCh = make(chan Task, nReduce)
	} else {
		m.taskCh = make(chan Task, len(m.files))
	}

	m.initMapTask()
	// 异步执行，不会阻塞m.server()的执行
	go m.tickSchedule()
	m.server()
	DPrintf("master init")
	return &m
}
