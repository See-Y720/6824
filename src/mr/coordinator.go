package mr

import (
	"container/list"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)


type Task struct {
	//0 map
	//1 reduce
	IsMapTask         bool
	FileName          string
	StartTime         int64
	TaskId            int64
	FileNameForReduce []string
}

type Coordinator struct {
	// Your definitions here.

	UnSchedulTasks        list.List
	InProcessTasks        list.List
	taskMap               map[int64]*list.Element
	StragglerMap          map[int64]int //冗余task共享同一个taskid
	taskId                int64         //未分配的最小taskid号
	UnFinishedTasksNum    int64
	nReduce               int
	IsCronJobRunning      bool
	IntermediateFileNames map[int][]string
	mutex                 sync.Mutex
	//fixme:idGen共用一个mutex，则122 的 idgen死锁
	idMutex         sync.Mutex
	IsInReduceStage bool
	Wg              sync.WaitGroup
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.UnSchedulTasks.Len() == 0 {
		reply.NeedWait = true
		return nil
	}

	reply.NeedWait = false
	taskPtr := c.UnSchedulTasks.Front()
	c.UnSchedulTasks.Remove(taskPtr)
	task := taskPtr.Value.(*Task)

	task.StartTime = time.Now().Unix()
	c.InProcessTasks.PushBack(task)
	c.taskMap[task.TaskId] = c.InProcessTasks.Back()

	reply.FileName = task.FileName
	reply.FileNameForReduce = task.FileNameForReduce
	reply.IsMapTask = task.IsMapTask
	reply.TaskId = task.TaskId
	reply.NReduce = c.nReduce

	//for debug
	// fmt.Printf("assign %v to worker,未调度：%v,进行中：%v \n", reply.FileName, c.UnSchedulTasks.Len(), c.InProcessTasks.Len())
	return nil
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	taskId := args.TaskId
	elemPtr := c.taskMap[taskId] //可能指向两个队列
	c.UnSchedulTasks.Remove(elemPtr)
	c.InProcessTasks.Remove(elemPtr)

	val, exist := c.StragglerMap[taskId]
	//不存在 cnt--
	//存在且val == 1 cnt--
	if exist && val == 0 {
		return nil
	}

	c.UnFinishedTasksNum -= 1
	if exist {
		c.StragglerMap[taskId] = 0
	}

	//map任务才需要记录中间文件名字
	if args.IsMapTask {
		for idx, filename := range args.FileNames {
			c.IntermediateFileNames[idx] = append(c.IntermediateFileNames[idx], filename)
		}

	}

	if c.IsInReduceStage {
		if c.UnFinishedTasksNum == 0 {
			c.IsCronJobRunning = false
		}
	} else {
		if c.UnFinishedTasksNum == 0 {
			//启动reduce
			// fmt.Println("启动reduce")
			c.IsInReduceStage = true
			for _, filenames := range c.IntermediateFileNames {
				task := &Task{
					IsMapTask:         false,
					FileNameForReduce: filenames,
					StartTime:         -1,
				}
				task.TaskId, _ = c.IdGen()
				c.UnSchedulTasks.PushBack(task)
				c.taskMap[task.TaskId] = c.UnSchedulTasks.Back()
				c.UnFinishedTasksNum += 1
				// fmt.Printf("add task:%v to Unsche \n", filenames)

			}

		}
	}
	// fmt.Printf("taskdone:未调度：%v,进行中：%v \n", c.UnSchedulTasks.Len(), c.InProcessTasks.Len())
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
		log.Fatal("listen erro:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) IdGen() (int64, error) {
	c.idMutex.Lock()
	defer c.idMutex.Unlock()
	c.taskId++
	return c.taskId, nil

}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	ret := false
	if c.IsInReduceStage && c.UnFinishedTasksNum == 0 {

		ret = true
	}
	return ret
}

func (c *Coordinator) CheckStraggler() {
	for {
		c.mutex.Lock()
		if !c.IsCronJobRunning {
			c.mutex.Unlock()
			break
		}
		c.mutex.Unlock()

		time.Sleep(10 * time.Second)

		c.mutex.Lock()
		var next *list.Element
		for s := c.InProcessTasks.Front(); s != nil; s = next {
			task := s.Value.(*Task)
			next = s.Next()
			if time.Now().Add(-10*time.Second).Unix() > task.StartTime {

				c.UnSchedulTasks.PushBack(task)
				c.taskMap[task.TaskId] = c.UnSchedulTasks.Back()
				//为什么不直接置为1
				// c.StragglerMap[task.TaskId] = 1
				//考虑这样的情况原始任务o被判定为straggler
				//c.StragglerMap[task.TaskId] = 1，启动任务c1
				//此时o完成，c.StragglerMap[task.TaskId] = 0 ，numcnt--
				//c1再次被判定为straggler
				//c.StragglerMap[task.TaskId] = 1，启动任务c2
				//c2完成 numcnt--，计数错误

				//正确做法
				_, exist := c.StragglerMap[task.TaskId]
				if !exist {
					c.StragglerMap[task.TaskId] = 1
				}

				//delete safely
				c.InProcessTasks.Remove(s)

			}

		}
		c.mutex.Unlock()

	}
	c.Wg.Done()
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.taskMap = make(map[int64]*list.Element)
	c.StragglerMap = make(map[int64]int)
	c.nReduce = nReduce
	c.IntermediateFileNames = make(map[int][]string, nReduce)
	c.UnFinishedTasksNum = 0
	c.IsInReduceStage = false

	for _, filename := range files {
		task := &Task{
			IsMapTask: true,
			FileName:  filename,
			StartTime: -1,
		}
		task.TaskId, _ = c.IdGen()
		c.UnSchedulTasks.PushBack(task)
		c.taskMap[task.TaskId] = c.UnSchedulTasks.Back()
		c.UnFinishedTasksNum += 1
	}

	c.IsCronJobRunning = true
	c.Wg.Add(1)
	go c.CheckStraggler()

	c.server()
	return &c
}
