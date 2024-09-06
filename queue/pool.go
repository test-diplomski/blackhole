package queue

import (
	"context"
	"fmt"
	"github.com/c12s/blackhole/helper"
	bPb "github.com/c12s/scheme/blackhole"
	cPb "github.com/c12s/scheme/celestial"
	pb "github.com/c12s/scheme/core"
	sg "github.com/c12s/stellar-go"
	"log"
)

func (wp *WorkerPool) newWorker(ctx context.Context, jobs chan *pb.Task, done, active chan string, id int, celestial, apollo, meridian string) {
	span, _ := sg.FromContext(ctx, "newWorker")
	defer span.Finish()
	// fmt.Println(span)

	wid := fmt.Sprintf("worker_%d", id)
	span.AddLog(&sg.KV{"Worker id", wid})
	kill := make(chan bool)
	for {
		select {
		case <-ctx.Done():
			log.Print(ctx.Err())
			return
		case task := <-jobs:
			span, _ := sg.FromCustomSource(
				task.SpanContext,
				task.SpanContext.Baggage,
				"worker.pulltasks",
			)
			// fmt.Println(span)
			// fmt.Println("SERIALIZE ", span.Serialize())
			active <- wid // signal that worker is taken the job

			fmt.Println("WORKER TASK: ", task)

			mt := &cPb.MutateReq{Mutate: task}
			switch mt.Mutate.Kind {
			case bPb.TaskKind_ROLES:
				client := NewApolloClient(apollo)
				_, err := client.Mutate(
					helper.AppendToken(
						sg.NewTracedGRPCContext(nil, span),
						task.Token,
					),
					mt,
				)
				if err != nil {
					log.Println(err)
					return
				}
				fmt.Println("Otisao zahtev u apollo")
			case bPb.TaskKind_NAMESPACES:
				client := NewMeridianClient(meridian)
				_, err := client.Mutate(
					helper.AppendToken(
						sg.NewTracedGRPCContext(nil, span),
						task.Token,
					),
					mt,
				)
				if err != nil {
					log.Println(err)
					return
				}
				fmt.Println("Otisao zahtev u merdian")
			default:
				client := NewCelestialClient(celestial)
				_, err := client.Mutate(
					helper.AppendToken(
						sg.NewTracedGRPCContext(nil, span),
						task.Token,
					),
					mt,
				)
				if err != nil {
					log.Println(err)
					return
				}
				fmt.Println("Otisao zahtev u celestial")
			}

			done <- wid // signal that worker is free
			span.Finish()
		case <-kill:
			log.Print("Worker killed")
			return
		}
	}

	wp.Workers[wid] = &Worker{
		ID:   wid,
		Kill: kill,
	}
}

func (wp *WorkerPool) init(ctx context.Context) {
	span, _ := sg.FromContext(ctx, "init")
	defer span.Finish()
	// fmt.Println(span)

	for i := 0; i < wp.MaxWorkers; i++ {
		go wp.newWorker(sg.NewTracedContext(ctx, span), wp.Pipe, wp.done, wp.active, i, wp.Celestial, wp.Apollo, wp.Meridian)
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Print(ctx.Err())
				return
			case wIdx := <-wp.active:
				wp.ActiveWorkers[wIdx] = wp.Workers[wIdx]
			case wIdx := <-wp.done:
				delete(wp.ActiveWorkers, wIdx)
			}
		}
	}()
}

// We hove number_of_tokens, max_workers and active_workers
// We need to determine is there free workers to pick update
// new tasks
func (wp *WorkerPool) Ready(tokens int64) bool {
	return (int64(wp.MaxWorkers) - int64(len(wp.ActiveWorkers))) >= tokens
}
