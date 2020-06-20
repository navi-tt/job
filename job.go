package job

import (
	"context"
	"errors"
	"sync"
	"time"
)

const (
	//默认worker的并发数
	defaultConcurrency = 5
)

var (
	ErrQueueNotExist   = errors.New("queue is not exists")
	ErrTimeout         = errors.New("timeout")
	ErrTopicRegistered = errors.New("the key had been registered")
)

type Job struct {
	//上下文
	ctx context.Context

	stopOnce sync.Once

	workers map[string]*WorkerWithFunc

	//work并发处理的等待暂停
	wg sync.WaitGroup
	//启动状态
	running bool
	//异常状态时需要sleep时间
	sleepy time.Duration
	//设置的初始等待时间
	initSleepy time.Duration
	//设置的等待时间的上限
	maxSleepy time.Duration
	//通道定时器超时时间
	timer time.Duration

	//是否初始化
	isQueueInit bool

	//统计
	pullCount        int64
	pullEmptyCount   int64
	pullErrCount     int64
	taskCount        int64
	taskErrCount     int64
	handleCount      int64
	handleErrCount   int64
	handlePanicCount int64

	//回调函数
	//任务返回失败回调函数
	taskErrCallback func(task *Task)
	//任务panic回调函数 增加参数将panic信息传递给回调函数，方便做sentry处理
	taskPanicCallback func(task *Task, e ...interface{})
	//任务处理前回调
	taskBeforeCallback func(task *Task)
	//任务处理后回调
	taskAfterCallback func(task *Task)
}

func (j *Job) processJob() {
	for _, w := range j.workers {
		w.Run()
	}
}

//After there is no data, the job starts from initsleepy to sleep,
//and then multiplies to maxsleepy. After finding the data, it sleep from initsleepy again
func (j *Job) Sleep() {
	if j.sleepy.Nanoseconds()*2 < j.maxSleepy.Nanoseconds() {
		j.sleepy = time.Duration(j.sleepy.Nanoseconds() * 2)
	} else if j.sleepy.Nanoseconds()*2 >= j.maxSleepy.Nanoseconds() && j.sleepy != j.maxSleepy {
		if j.sleepy < j.maxSleepy {
			j.sleepy = j.maxSleepy
		}
	}
	time.Sleep(j.sleepy)
}

func (j *Job) ResetSleep() {
	if j.sleepy != j.initSleepy {
		j.SetSleepy(j.initSleepy)
	}
}
