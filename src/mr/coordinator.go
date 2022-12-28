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
	CHECK_INTERVAL = 2 * time.Second
	TASK_TIMEOUT = 10 * time.Second
)

type Coordinator struct {
	mu sync.Mutex
	cond *sync.Cond
	tasks []*task
	nMap, nReduce int	// Number of map tasks and reduce tasks
}

type task struct {
	filename string
	taskType TaskType
	taskStatus TaskStatus
	assignTime string
}

func (c *Coordinator) RPCRequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	for {
		n := 0
		for i, task := range c.tasks {
			if task.taskStatus == TaskStatusCreated {
				task.taskStatus = TaskStatusPending
				task.assignTime = fromTime(time.Now())
				reply.TaskId = i
				reply.Filename = task.filename
				reply.TaskType = task.taskType
				reply.AssignedTime = task.assignTime
				reply.NMap = c.nMap
				reply.NReduce = c.nReduce
				c.mu.Unlock()
				return nil
			} else if task.taskStatus == TaskStatusFinished {
				n++
			}
		}
		if c.tasks[0].taskType == TaskTypeRed && n == len(c.tasks) {
			reply.TaskId = -1
			reply.TaskType = TaskTypeNil
			c.mu.Unlock()
			return nil
		}
		c.cond.Wait()
	}
}

func (c *Coordinator) RPCFinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := args.TaskId
	if (id < 0 || id >= len(c.tasks)) { return nil }
	if c.tasks[id].taskStatus != TaskStatusPending || c.tasks[id].assignTime != args.AssignedTime {
		return nil 
	}
	c.tasks[id].taskStatus = TaskStatusFinished
	if c.tasks[id].taskType == TaskTypeMap {
		for _, task := range c.tasks {
			if task.taskStatus != TaskStatusFinished { return nil }
		}
		c.tasks = nil
		for i := 0; i < c.nReduce; i++ {
			c.tasks = append(c.tasks, &task{
				taskType: TaskTypeRed,
				taskStatus: TaskStatusCreated,
			})
		}
		c.cond.Broadcast()
	}
	return nil
}

func (c *Coordinator) taskHealthCheck() {
	for {
		c.mu.Lock()
		now := time.Now()
		n := 0
		for _, task := range c.tasks {
			if task.taskStatus == TaskStatusPending {
				if toTime(task.assignTime).Add(TASK_TIMEOUT).Before(now) {
					task.taskStatus = TaskStatusCreated
					n++
				}
			}
		}
		for ;n > 0;n-- { c.cond.Signal() }
		c.mu.Unlock()
		time.Sleep(CHECK_INTERVAL)
	}
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
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, task := range c.tasks {
		if task.taskType == TaskTypeMap || task.taskStatus != TaskStatusFinished { return false }
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
// Total number of tasks: len(files) + nReduce
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nMap = len(files)
	c.nReduce = nReduce
	c.cond = sync.NewCond(&c.mu)
	for _, file := range files {
		c.tasks = append(c.tasks, &task{
			filename: file,
			taskType: TaskTypeMap,
			taskStatus: TaskStatusCreated,
		})
	}
	go c.taskHealthCheck()
	c.server()
	return &c
}

func fromTime(t time.Time) string {
	return t.Format(time.UnixDate)
}

func toTime(ts string) time.Time {
	t, _ := time.Parse(time.UnixDate, ts)
	return t
}