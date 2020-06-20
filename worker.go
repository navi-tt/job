package job

import (
	"context"
	"fmt"
	"github.com/navi-tt/job/internal/log"
	"github.com/navi-tt/job/internal/queue"
	"github.com/panjf2000/ants/v2"
	"sync/atomic"
	"time"
)

/**
 * @Author: Liu xiangpeng
 * @Date: 2020/6/19 5:28 下午
 */

type Worker interface {
	Exec(context.Context, *Task)
}

type WorkerFunc func(context.Context, *Task)

func (f WorkerFunc) Exec(ctx context.Context, task *Task) {
	f(ctx, task)
}

type WorkerWithFunc struct {
	job *Job
	*ants.Pool

	q      queue.Queue // 任务来源队列
	topic  string
	worker Worker // 任务执行器

	extra   []interface{} // 为了一些特殊驱动需要额外参数
	working bool          // 是否工作中
	pipe    chan *Task    // 队列拉取任务协程与执行任务协程直接通信管道
}

func validate(q queue.Queue, topic string, f func(context.Context, *Task)) error {
	if q == nil {
		return fmt.Errorf("queue.Queue can not be nil")
	}
	if len(topic) == 0 {
		return fmt.Errorf("topic can not be\"\"")
	}
	if f == nil {
		return fmt.Errorf("work func can not be nil")
	}
	return nil
}

func (j *Job) NewWorkerWithFunc(q queue.Queue, topic string, f func(context.Context, *Task), size int, extra ...interface{}) (*WorkerWithFunc, error) {
	err := validate(q, topic, f)
	if err != nil {
		return nil, err
	}

	if size <= 0 {
		size = defaultConcurrency
	}

	w := new(WorkerWithFunc)
	w.job = j
	w.q = q
	w.topic = topic
	w.Pool, err = ants.NewPool(size, ants.WithNonblocking(false))
	if err != nil {
		return nil, err
	}

	w.worker = WorkerFunc(f)
	w.extra = extra
	w.working = true
	w.pipe = make(chan *Task, size)

	return w, nil
}

func (w *WorkerWithFunc) Job() *Job {
	return w.job
}

func (w *WorkerWithFunc) Topic() string {
	return w.topic
}

func (w *WorkerWithFunc) Queue() queue.Queue {
	return w.q
}

func (w *WorkerWithFunc) Extra() []interface{} {
	return w.extra
}

func (w *WorkerWithFunc) Worker() Worker {
	return w.worker
}

func (w *WorkerWithFunc) Close() {
	w.working = false
	close(w.pipe)
}

func (w *WorkerWithFunc) Run() {
	// 开启拉取队列数据
	w.pullTask()
	go func() {
		for w.Job().running && w.working {
			select {
			// 阻塞处理队列 task 任务
			case task, ok := <-w.pipe:
				if !ok {
					return // channel pipe关闭, 即工作任务被关闭, 结束该协程
				}

				// 协程池执行任务, 协程池可用协程为空则阻塞在此处
				if err := w.Submit(func() { w.processTask(task) }); err != nil {
					log.Error(err)
					continue
				}
			}
		}
	}()
}

func (w *WorkerWithFunc) pullTask() {
	go func() {
		for w.Job().running && w.working {
			// todo(liuxp: 考虑将出队和反序列化task任务的逻辑, 用协程处理, 提高出队效率)
			// todo(liuxp: 或考虑将Dequeue设计为阻塞, 但是性能可能不好)
			message, token, dequeueCount, err := w.Queue().Dequeue(w.Job().ctx, w.Topic(), w.Extra())
			atomic.AddInt64(&w.Job().pullCount, 1)
			if err != nil && err != queue.ErrNil {
				atomic.AddInt64(&w.Job().pullErrCount, 1)
				log.Errorf("dequeue_error: %v, %v", err, message)
				w.Job().Sleep()
				continue
			}

			if err == queue.ErrNil || message == "" {
				atomic.AddInt64(&w.Job().pullEmptyCount, 1)
				w.Job().Sleep()
				continue
			}
			atomic.AddInt64(&w.Job().taskCount, 1)

			t, err := DecodeStringTask(message)
			if err != nil {
				atomic.AddInt64(&w.Job().taskErrCount, 1)
				log.Errorf("decode_task_error: %v, %v", err, message)
				w.Job().Sleep()
				continue
			} else if t.Topic != "" {
				t.Token = token
			}
			t.DequeueCount = dequeueCount
			w.Job().ResetSleep()

			//w.pipe <- &t
			select {
			case w.pipe <- &t:
			default:
				// 这里开select是为了该协程不会因为pipe阻塞而收不到wp的Close信号
				// 但是可能导致wStop后没有把当前的task传给pipe, 导致一次数据丢失
				push := false
				for !push {
					if w.Job().running && w.working {
						return
					}
					ticker := time.NewTicker(w.Job().timer)
					select {
					case <-ticker.C:
					case w.pipe <- &t:
						push = true
					}
					ticker.Stop()
				}
			}
		}
	}()
}

func (w *WorkerWithFunc) processTask(task *Task) {
	w.Job().wg.Add(1)
	defer func() {
		w.Job().wg.Done()
		//任务panic回调函数
		if e := recover(); e != nil {
			atomic.AddInt64(&w.Job().handlePanicCount, 1)
			if w.Job().taskPanicCallback != nil {
				w.Job().taskPanicCallback(task, e)
			} else {
				log.Error("task_panic", task, e)
			}
		}
	}()

	//任务处理前回调函数
	if w.Job().taskBeforeCallback != nil {
		w.Job().taskBeforeCallback(task)
	}

	w.Worker().Exec(w.Job().ctx, task)
	result := task.Result

	atomic.AddInt64(&w.Job().handleCount, 1)
	isAck := false
	switch result.State {
	case StateSucceed:
		isAck = true
	case StateFailedWithRetryNumLimit:
		isAck = true
	case StateFailedWithAck:
		isAck = true
		atomic.AddInt64(&w.Job().handleErrCount, 1)
	case StateFailed:
		atomic.AddInt64(&w.Job().handleErrCount, 1)
	}

	//消息ACK
	if isAck && task.Token != "" {
		_, err := w.Queue().AckMsg(w.Job().ctx, w.Topic(), task.Token, w.Extra())
		if err != nil {
			log.Error("ack_error", w.Topic(), task)
			return
		}
	}
	//任务处理后回调函数
	if w.Job().taskAfterCallback != nil {
		w.Job().taskAfterCallback(task)
	}
}
