package main

import (
	"context"
	"fmt"
	"github.com/navi-tt/job"
	"github.com/navi-tt/job/internal/queue"
	"time"
)

func main() {
	//实例化一个队列worker服务
	j := job.New()
	//注册worker
	RegisterWorker(q, j)

	j.Start()

	//获取运行态统计数据
	//j.Stats()

	//停止服务
	//j.Stop()
	//等待服务停止，调用停止只是将服务设置为停止状态，但是可能有任务还在跑，waitStop会等待worker任务跑完后停止当前服务。
	//第一个参数为超时时间，如果无法获取到worker全部停止状态，在超时时间后会return一个超时错误
	//j.WaitStop(time.Second * 3)
}

/**
 * 注册worker
 */
func RegisterWorker(q queue.Queue, j *job.Job) {
	//设置worker的任务投递回调函数
	j.AddFunc(q, "topic:test", test, 1)
	//设置worker的任务投递回调函数，和并发数
	j.AddFunc(q, "topic:test1", test, 2)
	//使用worker结构进行注册
	w, _ := j.NewWorkerWithFunc(q, "topic:test2", test, 1)
	j.AddWorkerWithFunc(w)
}

/**
 * 任务投递回调函数
 * 备注：任务处理逻辑不要异步化，否则无法控制worker并发数，需要异步化的需要阻塞等待异步化结束，如wg.Wait()
 */
func test(ctx context.Context, task *job.Task) {
	time.Sleep(time.Millisecond * 5)
	s, err := job.JsonEncode(task)
	if err != nil {
		//job.StateFailed 不会进行ack确认
		//job.StateFailedWithAck 会进行act确认
		//return job.TaskResult{Id: task.Id, State: job.StateFailed}
		task.Result = job.Result{State: job.StateFailedWithAck}
	} else {
		//job.StateSucceed 会进行ack确认
		fmt.Println("do task", s)
		task.Result = job.Result{State: job.StateSucceed}
	}

}

var jb *job.Job

func getJob() *job.Job {
	if jb == nil {
		jb = job.New()
		RegisterWorker(q, jb)
	}
	return jb
}
