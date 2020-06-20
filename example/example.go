package main

import (
	"context"
	"fmt"
	njob "github.com/navi-tt/job"
	"github.com/navi-tt/job/internal/queue"
	"strconv"
	"sync"
	"time"
)

var (
	queues map[string][]string
	lock   sync.RWMutex
	q      = new(LocalQueue)
)

func init() {
	queues = make(map[string][]string)
}

func main() {
	stop := make(chan int, 0)

	testJobFeature()

	job := njob.New()
	//task任务panic的回调函数
	job.RegisterTaskPanicCallback(func(task *njob.Task, e ...interface{}) {
		fmt.Println("task_panic_callback", task.Message)
		if len(e) > 0 {
			fmt.Println("task_panic_error", e[0])
		}
	})

	bench(q, job)

	//termStop(job);
	<-stop
}

//测试job新的轮训策略
func testJobFeature() {
	job := njob.New()
	go addData(job)

	job.SetSleepy(time.Second, time.Second*10)
	job.AddFunc(q, "kxy1", Me, 1, "instanceId", "groupId")
	job.Start()
}

func addData(job *njob.Job) {
	time.Sleep(time.Second * 60)
	pushQueueData(job, "kxy1", 1, 1)
}

//压测
func bench(q queue.Queue, job *njob.Job) {
	RegisterWorkerBench(q, job)
	// 验证参数透传
	job.AddFunc(q, "hts1", Me, 5, "instanceId", "groupId")
	pushQueueData(job, "kxy1", 1000000, 10000)
	//启动服务
	job.Start()
	//启动统计脚本
	go jobStats(job)
}

//验证平滑关闭
func termStop(q queue.Queue, job *njob.Job) {
	RegisterWorker2(q, job)
	//预先生成数据到本地内存队列
	pushQueueData(job, "hts1", 10000)
	pushQueueData(job, "hts2", 10000, 10000)
	pushQueueData(job, "kxy1", 10000, 20000)

	//启动服务
	job.Start()

	//启动统计脚本
	go jobStats(job)

	//结束服务
	time.Sleep(time.Millisecond * 3000)
	job.Stop()
	err := job.WaitStop(time.Second * 10)
	fmt.Println("wait stop return", err)

	//统计数据，查看是否有漏处理的任务
	stat := job.Stats()
	fmt.Println(stat)
	count := len(queues["hts1"]) + len(queues["hts2"]) + len(queues["kxy1"])
	fmt.Println("remain count:", count)
}

func pushQueueData(job *njob.Job, topic string, args ...int) {
	ctx := context.Background()
	start := 1
	length := 1
	if len(args) > 0 {
		length = args[0]
		if len(args) > 1 {
			start = args[1]
		}
	}

	strs := make([]string, 0)
	for i := start; i < start+length; i++ {
		strs = append(strs, strconv.Itoa(i))
	}
	if len(args) > 0 {
		job.BatchEnqueue(ctx, topic, strs)
	}
}

func jobStats(job *njob.Job) {
	var stat map[string]int64
	var count int64
	var str string

	lastStat := job.Stats()
	keys := make([]string, 0)
	for k, _ := range lastStat {
		keys = append(keys, k)
	}

	for {
		stat = job.Stats()

		str = ""
		for _, k := range keys {
			count = stat[k] - lastStat[k]
			if count > 0 {
				str += k + ":" + strconv.FormatInt(count, 10) + "   "
			}
		}
		fmt.Println(time.Now().Format("2006-01-02 15:04:05"), str)

		lastStat = stat
		time.Sleep(time.Second)
	}
}

/**
 * 配置队列任务
 */
func RegisterWorkerBench(q queue.Queue, job *njob.Job) {
	w, err := job.NewWorkerWithFunc(q, "kxy1", Mock, 100)
	if err != nil {
		return
	}
	job.AddWorkerWithFunc(w)
}

func Mock(ctx context.Context, task *njob.Task) {
	time.Sleep(time.Millisecond * 5)
	task.Result = njob.Result{State: njob.StateSucceed}
}

/**
 * 配置队列任务
 */
func RegisterWorker2(q queue.Queue, job *njob.Job) {

	job.AddFunc(q, "hts1", Me, 5)
	job.AddFunc(q, "hts2", Me, 3)
	w, err := job.NewWorkerWithFunc(q, "kxy1", Mock, 100)
	if err != nil {
		return
	}
	job.AddWorkerWithFunc(w)
}

func Me(ctx context.Context, task *njob.Task) {

	ctx.Done()
	time.Sleep(time.Millisecond * 50)
	i, _ := strconv.Atoi(task.Message)
	if i%10 == 0 {
		panic("wo cuo le " + task.Message + " (" + task.Topic + ")")
	}
	s, _ := njob.JsonEncode(task)
	fmt.Println("do task", s)
	task.Result = njob.Result{State: njob.StateSucceed}
}

type LocalQueue struct{}

func (q *LocalQueue) Enqueue(ctx context.Context, key string, message string, args ...interface{}) (ok bool, err error) {
	lock.Lock()
	defer lock.Unlock()

	if _, ok = queues[key]; !ok {
		queues[key] = make([]string, 0)
	}

	queues[key] = append(queues[key], message)
	return true, nil
}

func (q *LocalQueue) BatchEnqueue(ctx context.Context, key string, messages []string, args ...interface{}) (ok bool, err error) {
	lock.Lock()
	defer lock.Unlock()

	if _, ok = queues[key]; !ok {
		queues[key] = make([]string, 0)
	}

	queues[key] = append(queues[key], messages...)
	return true, nil
}

func (q *LocalQueue) Dequeue(ctx context.Context, key string, args ...interface{}) (message string, token string, dequeueCount int64, err error) {
	lock.Lock()
	defer lock.Unlock()

	if len(queues[key]) > 0 {
		message = queues[key][0]
		queues[key] = queues[key][1:]
	}

	return
}

func (q *LocalQueue) AckMsg(ctx context.Context, key string, token string, args ...interface{}) (bool, error) {
	return true, nil
}
