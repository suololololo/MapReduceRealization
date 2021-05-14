package mr

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// worker状态
const (
	TaskIdle    = iota // 闲置
	TaskWorking        // 正在运行
	TaskCommit         // 完成
)

type Master struct {
	// Your definitions here.
	files   []string
	nReduce int

	mapTasks    []int
	reduceTasks []int

	mapCount     int
	workerCommit map[string]int // worker对应的状态
	allCommited  bool

	timeout time.Duration
	mu      sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) Work(args *WorkArgs, reply *WorkReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range m.files {
		if m.mapTasks[k] != TaskIdle {
			continue
		}
		reply.FileName = v
		reply.TaskId = k
		reply.MapReduce = "map"
		reply.IsFinished = false
		reply.FileNumber = m.nReduce
		m.workerCommit[args.WorkerId] = TaskWorking
		m.mapTasks[k] = TaskWorking
		ctx, _ := context.WithTimeout(context.Background(), m.timeout)
		go func() {
			select {
			case <-ctx.Done():
				{

					m.mu.Lock()
					defer m.mu.Unlock()
					if m.workerCommit[args.WorkerId] != TaskCommit && m.mapTasks[k] != TaskCommit {
						m.mapTasks[k] = TaskIdle
					}
				}
			}
		}()
		return nil
	}

	for k, v := range m.reduceTasks {
		//map is not finished
		if m.mapCount != len(m.files) {
			return nil
		}
		if v != TaskIdle {
			continue
		}
		reply.TaskId = k
		reply.MapReduce = "reduce"
		reply.FileNumber = len(m.files)
		reply.IsFinished = false
		reply.FileName = ""
		m.workerCommit[args.WorkerId] = TaskWorking
		m.reduceTasks[k] = TaskWorking
		ctx, _ := context.WithTimeout(context.Background(), m.timeout)
		go func() {
			select {
			case <-ctx.Done():
				{

					m.mu.Lock()
					defer m.mu.Unlock()
					if m.workerCommit[args.WorkerId] != TaskCommit && m.reduceTasks[k] != TaskCommit {
						m.mapTasks[k] = TaskIdle

					}
				}
			}
		}()
		return nil
	}

	for _, v := range m.workerCommit {
		if v == TaskWorking {
			reply.IsFinished = false
			return nil
		}
	}
	reply.IsFinished = true
	return errors.New("worker apply but no tasks to dispatch")

}

func (m *Master) Commit(args *CommitArgs, reply *CommitReply) error {
	//log.Println("a worker", args.WorkerId, "commit a", args.MapReduce, "task:", args.TaskId)
	m.mu.Lock()
	switch args.MapReduce {
	case "map":
		{
			m.workerCommit[args.WorkerId] = TaskCommit

			m.mapTasks[args.TaskId] = TaskCommit
			m.mapCount++
		}

	case "reduce":
		{
			m.reduceTasks[args.TaskId] = TaskCommit
			m.workerCommit[args.WorkerId] = TaskCommit
		}

	}
	m.mu.Unlock()
	for _, v := range m.mapTasks {
		if v != TaskCommit {
			return nil
		}
	}
	for _, v := range m.reduceTasks {
		if v != TaskCommit {
			return nil
		}
	}
	m.allCommited = true
	return nil
}

//
//an example RPC handler.

//the RPC argument and reply types are defined in rpc.go.

func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
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
	return m.allCommited
}


//create a Master.
//main/mrmaster.go calls this function.
//nReduce is the number of reduce tasks to use.

func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		files:        files,
		nReduce:      nReduce,
		mapTasks:     make([]int, len(files)),
		reduceTasks:  make([]int, nReduce),
		workerCommit: make(map[string]int),
		allCommited:  false,
		timeout:      10 * time.Second,
	}

	//log.Println("[init] with:", files, nReduce)
	m.server()
	return &m
}






