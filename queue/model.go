package queue

import (
	"context"
	"errors"
	"fmt"
	"github.com/c12s/blackhole/model"
	storage "github.com/c12s/blackhole/storage"
	pb "github.com/c12s/scheme/core"
	sg "github.com/c12s/stellar-go"
)

type TokenBucket struct {
	Capacity     int64
	Tokens       int64
	FillInterval *model.FillInterval
	Notify       chan bool
	Reset        chan bool
	Delay        chan bool
	Attempt      int
	TRetry       *model.Retry
}

type TaskQueue struct {
	Namespace string
	Name      string
	Queue     storage.DB
	Bucket    *TokenBucket
	Pool      *WorkerPool
}

type Worker struct {
	ID   string
	Kill chan bool
	// Msg  chan string
}

type WorkerPool struct {
	MaxQueued     int
	MaxWorkers    int
	Pipe          chan *pb.Task
	ActiveWorkers map[string]*Worker
	done          chan string
	active        chan string
	Workers       map[string]*Worker
	Celestial     string
	Apollo        string
	Meridian      string
}

type BlackHole struct {
	Queues map[string]*TaskQueue
}

func (bh *BlackHole) GetTK(ctx context.Context, name string) (*TaskQueue, error) {
	span, _ := sg.FromGRPCContext(ctx, "queue.GetTK")
	defer span.Finish()
	fmt.Println(span)

	fmt.Println("QUEUE NAME :", name)

	if tk, ok := bh.Queues[name]; ok {
		return tk, nil
	}

	span.AddLog(&sg.KV{"queue error", "Queue does not exists"})
	return nil, errors.New("Queue does not exists!")
}

func newPool(ctx context.Context, maxqueued, maxworkers int, celestial, apollo, meridian string) *WorkerPool {
	span, _ := sg.FromContext(ctx, "newPool")
	defer span.Finish()
	fmt.Println(span)

	wp := &WorkerPool{
		MaxQueued:     maxqueued,
		MaxWorkers:    maxworkers,
		Pipe:          make(chan *pb.Task, 100),
		ActiveWorkers: map[string]*Worker{},
		done:          make(chan string),
		active:        make(chan string),
		Workers:       map[string]*Worker{},
		Celestial:     celestial,
		Apollo:        apollo,
		Meridian:      meridian,
	}
	wp.init(sg.NewTracedContext(ctx, span))
	return wp
}

func newQueue(ctx context.Context, ns, name string, tb *TokenBucket, wp *WorkerPool, db storage.DB) *TaskQueue {
	span, _ := sg.FromContext(ctx, "newQueue")
	defer span.Finish()
	fmt.Println(span)

	return &TaskQueue{
		Namespace: ns,
		Name:      name,
		Queue:     db,
		Bucket:    tb,
		Pool:      wp,
	}
}

func newBucket(ctx context.Context, capacity, tokens int64, interval *model.FillInterval, retry *model.Retry) *TokenBucket {
	span, _ := sg.FromContext(ctx, "newBucket")
	defer span.Finish()
	fmt.Println(span)

	return &TokenBucket{
		Capacity:     capacity,
		Tokens:       tokens,
		FillInterval: interval,
		Notify:       make(chan bool),
		Delay:        make(chan bool),
		Reset:        make(chan bool),
		Attempt:      1,
		TRetry:       retry,
	}
}

func New(ctx context.Context, db storage.DB, options []*model.TaskOption, celestial, apollo, meridian string) *BlackHole {
	span, _ := sg.FromContext(ctx, "queue.New")
	defer span.Finish()
	fmt.Println(span)

	q := map[string]*TaskQueue{}
	for _, opt := range options {
		tb := newBucket(sg.NewTracedContext(ctx, span), opt.Capacity, opt.Tokens, opt.FillRate, opt.TRetry)
		wp := newPool(sg.NewTracedContext(ctx, span), opt.MaxQueued, opt.MaxWorkers, celestial, apollo, meridian)
		tq := newQueue(sg.NewTracedContext(ctx, span), opt.Namespace, opt.Name, tb, wp, db)

		// Add queue to the database
		err := db.AddQueue(sg.NewTracedContext(ctx, span), opt)
		if err != nil {
			span.AddLog(
				&sg.KV{"Queue not created", err.Error()},
				&sg.KV{"Queue not created", opt.Name},
			)
			fmt.Println("Queue not created: ", opt.Name)
			continue
		}

		// Start queue
		q[opt.Name] = tq
		tq.StartQueue(sg.NewTracedContext(ctx, span))
	}
	return &BlackHole{
		Queues: q,
	}
}
