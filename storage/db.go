package db

import (
	"context"
	"github.com/c12s/blackhole/model"
	bPb "github.com/c12s/scheme/blackhole"
	cPb "github.com/c12s/scheme/core"
)

type DB interface {
	// Put tasks in the queue (user submit). Take n tasks from queue (done by workers)
	PutTasks(ctx context.Context, req *bPb.PutReq) (*bPb.Resp, error)
	TakeTasks(ctx context.Context, name, user_id string, tokens int64) (map[string]*cPb.Task, error)

	// Add and Remove queues
	AddQueue(ctx context.Context, opt *model.TaskOption) error
	RemoveQueue(ctx context.Context, name, namespace string) error

	// Close connection to the persistent storage
	Close()
}
