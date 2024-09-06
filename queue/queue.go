package queue

import (
	"context"
	"fmt"
	"github.com/c12s/blackhole/model"
	pb "github.com/c12s/scheme/blackhole"
	sg "github.com/c12s/stellar-go"
	"log"
	"math"
	"strconv"
	"time"
)

func (b *TokenBucket) retry() time.Duration {
	switch b.TRetry.Delay {
	case "linear":
		lVal := b.Attempt * b.TRetry.Doubling
		return time.Duration(lVal) * model.DetermineInterval(b.FillInterval)
	case "exp":
		exVal := math.Pow(float64(b.TRetry.Doubling), float64(b.Attempt))
		return time.Duration(exVal) * model.DetermineInterval(b.FillInterval)
	default:
		return time.Second
	}
}

func (b *TokenBucket) Start(ctx context.Context) {
	span, _ := sg.FromContext(ctx, "tokenbacket.start")
	defer span.Finish()
	// fmt.Println(span)

	//every fillTime we try to add new token to the bucket (execute the task)
	go func(c context.Context) {
		span, _ := sg.FromContext(c, "tokenbacket.start.daemon")
		defer span.Finish()
		// fmt.Println(span)

		interval := model.DetermineInterval(b.FillInterval)
		ticker := time.NewTicker(interval)
		for {
			select {
			case <-ticker.C:
				if b.Tokens < b.Capacity {
					b.Tokens++
				} else if b.Tokens == b.Capacity {
					b.Notify <- true
				}
			case <-c.Done():
				span.AddLog(&sg.KV{"daemon done", c.Err().Error()})
				log.Print(ctx.Err())
				ticker.Stop()
				return
			case <-b.Delay:
				// Increase attempt value
				if b.Attempt < b.TRetry.Limit {
					b.Attempt++

					//Increase ticking time based on strategy user provided
					ticker.Stop()
					ticker = time.NewTicker(b.retry())
				} else {
					ticker.Stop()
					ticker = nil // put tockenbucket in sleep state
				} // we got to Limit value, and should put timer to sleep

			case <-b.Reset:
				// if ticker is nil, than token bucket is in 'sleep state' -> not ticking
				// and need to be restarted! This is done on next task update
				if ticker == nil {
					// Reset retry
					b.Attempt = 0

					// Reset Ticker
					interval = model.DetermineInterval(b.FillInterval)
					ticker = time.NewTicker(interval)
				}
			}
		}
	}(ctx)
}

func (b *TokenBucket) TakeAll(ctx context.Context) int64 {
	span, _ := sg.FromContext(ctx, "take all")
	defer span.Finish()
	// fmt.Println(span)

	b.Tokens = 0

	span.AddLog(&sg.KV{"TakeAll capacity", strconv.FormatInt(int64(b.Capacity), 10)})
	return b.Capacity
}

func (t *TaskQueue) Loop(ctx context.Context) {
	go func(c context.Context) {
		span, _ := sg.FromContext(ctx, "loop")
		defer span.Finish()
		fmt.Println(span)

		for {
			select {
			case <-t.Bucket.Notify:
				//if pool is ready to take new tasks, take them...oterwise signal tockenbucket to increase notify time.
				//When pool is ready to take new tasks reset toke bucket to regular interval
				// if t.Pool.Ready(t.Bucket.Capacity) {
				// 	done, tokens := t.Bucket.TakeAll()
				// 	if done {
				// 		if sync := t.Sync(ctx, tokens); !sync {
				// 			t.Bucket.Delay <- true // if there are no tasks in queue. Try it for a few times, than go to sleep unitl next task submit
				// 		}
				// 	}
				// } else {
				// 	t.Bucket.Delay <- true // if pool not ready, make delay
				// }

				tokens := t.Bucket.TakeAll(sg.NewTracedContext(c, span))
				t.Sync(sg.NewTracedContext(c, span), tokens)
			case <-ctx.Done():
				log.Print(ctx.Err())
				return
			}
		}
	}(ctx)
}

func (t *TaskQueue) StartQueue(ctx context.Context) {
	span, _ := sg.FromContext(ctx, "StartQueue")
	defer span.Finish()
	// fmt.Println(span)

	t.Bucket.Start(sg.NewTracedContext(ctx, span))
	t.Loop(sg.NewTracedContext(ctx, span))
}

func (t *TaskQueue) PutTasks(ctx context.Context, req *pb.PutReq) (*pb.Resp, error) {
	span, _ := sg.FromContext(ctx, "PutTasks")
	defer span.Finish()

	// t.Bucket.Reset <- true
	return t.Queue.PutTasks(sg.NewTracedContext(ctx, span), req)
}

func (t *TaskQueue) Sync(ctx context.Context, tokens int64) bool {
	span, _ := sg.FromContext(ctx, "Sync")
	defer span.Finish()
	// fmt.Println(span)

	tasks, err := t.Queue.TakeTasks(
		sg.NewTracedContext(ctx, span),
		t.Name, t.Namespace, tokens,
	)
	if err != nil {
		span.AddLog(&sg.KV{"TakeTasks error", err.Error()})
		log.Println(err)
		return false
	}

	if len(tasks) == 0 {
		return true
	}

	for _, task := range tasks {
		t.Pool.Pipe <- task
	}

	return len(tasks) > 0 // if TakeTasks return 0 that means that there is no tasks to pick from queue.
}
