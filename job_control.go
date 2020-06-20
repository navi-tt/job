package job

import (
	"context"
	"github.com/navi-tt/job/internal/queue"
	"time"
)

func New() *Job {
	j := new(Job)
	j.ctx = context.Background()
	j.workers = make(map[string]*WorkerWithFunc)

	j.sleepy = time.Millisecond * 10
	j.initSleepy = time.Millisecond * 10
	j.maxSleepy = time.Millisecond * 10
	j.timer = time.Millisecond * 30
	return j
}

func (j *Job) Start() {
	if j.running {
		return
	}
	j.running = true
	j.processJob()
}

/**
 * 暂停Job
 */
func (j *Job) Stop() {
	j.running = false
}

/**
 * 等待队列任务消费完成，可设置超时时间返回
 * @param timeout 如果小于0则默认10秒
 */
func (j *Job) WaitStop(timeout time.Duration) error {
	var err error
	j.stopOnce.Do(func() {
		ch := make(chan struct{})
		j.Sleep()
		if timeout <= 0 {
			timeout = time.Second * 10
		}

		go func() {
			j.wg.Wait()
			close(ch)
		}()

		select {
		case <-ch:
			return
		case <-time.After(timeout):
			err = ErrTimeout
			return
		}
	})
	return err
}

func (j *Job) AddFunc(q queue.Queue, topic string, f func(context.Context, *Task), size int, args ...interface{}) error {
	wp, err := j.NewWorkerWithFunc(q, topic, f, size, args)
	if err != nil {
		return err
	}
	return j.AddWorkerWithFunc(wp)
}

func (j *Job) AddWorkerWithFunc(w *WorkerWithFunc) error {
	if _, ok := j.workers[w.Topic()]; ok {
		return ErrTopicRegistered
	}
	j.workers[w.Topic()] = w
	return nil
}

//获取统计数据
func (j *Job) Stats() map[string]int64 {
	return map[string]int64{
		"pull":         j.pullCount,
		"pull_err":     j.pullErrCount,
		"pull_empty":   j.pullEmptyCount,
		"task":         j.taskCount,
		"task_err":     j.taskErrCount,
		"handle":       j.handleCount,
		"handle_err":   j.handleErrCount,
		"handle_panic": j.handlePanicCount,
	}
}

//设置休眠的时间 -- 碰到异常或者空消息等情况
func (j *Job) SetSleepy(sleepy time.Duration, args ...time.Duration) {
	j.sleepy = sleepy
	j.initSleepy = sleepy
	if len(args) > 0 {
		j.maxSleepy = args[0]
	} else {
		j.maxSleepy = sleepy
	}
}

//在通道传递数据时的阻塞超时时间
func (j *Job) SetTimer(timer time.Duration) {
	j.timer = timer
}

//设置任务处理前回调函数
func (j *Job) RegisterTaskBeforeCallback(f func(task *Task)) {
	j.taskBeforeCallback = f
}

//设置任务处理后回调函数
func (j *Job) RegisterTaskAfterCallback(f func(task *Task)) {
	j.taskAfterCallback = f
}

//设置任务panic回调函数：回调函数自己确保panic不会上报，否则会导致此topic的队列worker停止
func (j *Job) RegisterTaskPanicCallback(f func(task *Task, e ...interface{})) {
	j.taskPanicCallback = f
}
