package queue

import (
	"context"
	"errors"
)

var (
	ErrNil = errors.New("return nil")
)

type Queue interface {
	Enqueue(ctx context.Context, key string, message string, args ...interface{}) (isOk bool, err error)
	Dequeue(ctx context.Context, key string, args ...interface{}) (message string, token string, dequeueCount int64, err error)
	AckMsg(ctx context.Context, key string, token string, args ...interface{}) (ok bool, err error)
	BatchEnqueue(ctx context.Context, key string, messages []string, args ...interface{}) (isOk bool, err error)
}
