package etcd

import (
	"context"
	"fmt"
	"github.com/c12s/blackhole/helper"
	"github.com/c12s/blackhole/model"
	bPb "github.com/c12s/scheme/blackhole"
	cPb "github.com/c12s/scheme/core"
	sPb "github.com/c12s/scheme/stellar"
	sg "github.com/c12s/stellar-go"
	"github.com/coreos/etcd/clientv3"
	"github.com/golang/protobuf/proto"
	"strings"
)

func parse(tags string) map[string]string {
	rez := map[string]string{}
	if len(tags) > 0 {
		for _, item := range strings.Split(tags, ";") {

			fmt.Println("SPLIT ", item)
			pair := strings.Split(item, ":")
			fmt.Println("SPLIT ", pair)
			rez[pair[0]] = pair[1]
		}
	}

	return rez
}

func (s *StorageEtcd) put(ctx context.Context, req *bPb.PutReq, num int, task *bPb.PutTask) {
	span, _ := sg.FromContext(ctx, "db.PutTasks.put")
	defer span.Finish()
	fmt.Println(span)
	fmt.Println("SERIALIZE ", span.Serialize())

	token, terr := helper.ExtractToken(ctx)
	if terr != nil {
		span.AddLog(&sg.KV{"token error", terr.Error()})
		return
	}

	ssp := span.Serialize()
	qt := &cPb.Task{
		UserId:    req.UserId,
		Kind:      req.Kind,
		Timestamp: req.Mtdata.Timestamp,
		Namespace: req.Mtdata.Namespace,
		Extras:    req.Extras,
		SpanContext: &sPb.SpanContext{
			TraceId:       ssp.Get("trace_id")[0],
			SpanId:        ssp.Get("span_id")[0],
			ParrentSpanId: ssp.Get("parrent_span_id")[0],
			Baggage:       parse(ssp.Get("tags")[0]),
		},
		Token: token,
	}

	if task != nil {
		qt.Task = task
	}

	data, err := proto.Marshal(qt)
	if err != nil {
		span.AddLog(&sg.KV{"marshalling error", err.Error()})
		fmt.Println(err)
	}

	var key = ""
	if req.Mtdata.Namespace == "" && req.Mtdata.Queue == "" {
		key = TaskKey(qdefault, qdefault, req.Mtdata.TaskName, req.Mtdata.Timestamp, num)
	} else if req.Mtdata.ForceNamespaceQueue {
		key = TaskKey(req.Mtdata.Namespace, req.Mtdata.Namespace, req.Mtdata.TaskName, req.Mtdata.Timestamp, num)
	} else {
		fmt.Println("DESIO SE ELSE")
		key = TaskKey(req.Mtdata.Namespace, req.Mtdata.Queue, req.Mtdata.TaskName, req.Mtdata.Timestamp, num)
	}

	child := span.Child("etcd task put")
	_, err = s.Kv.Put(ctx, key, string(data))
	if err != nil {
		fmt.Println("ERRORCINA ", err)
		child.AddLog(&sg.KV{"etcd task put error", err.Error()})
		fmt.Println(err) //TODO: this should go to some log system!!
	}
	fmt.Println("KLJUC: ", key)
	child.Finish()
}

func (s *StorageEtcd) PutTasks(ctx context.Context, req *bPb.PutReq) (*bPb.Resp, error) {
	span, _ := sg.FromContext(ctx, "db.PutTasks")
	defer span.Finish()
	fmt.Println(span)

	if len(req.Tasks) > 0 {
		fmt.Println("DESIo SE IF")
		for num, task := range req.Tasks {
			s.put(sg.NewTracedContext(ctx, span), req, num, task)
		}
	} else {
		fmt.Println("DESIO SE ELSE")
		s.put(sg.NewTracedContext(ctx, span), req, 0, nil)
	}

	tid := span.Serialize().Get("trace_id")[0]
	return &bPb.Resp{Msg: fmt.Sprintf("Task accepted with traceid: %s", tid)}, nil
}

func (s *StorageEtcd) TakeTasks(ctx context.Context, name, namespace string, tokens int64) (map[string]*cPb.Task, error) {
	span, _ := sg.FromContext(ctx, "db.TakeTasks")
	defer span.Finish()
	// fmt.Println(span)

	retTasks := map[string]*cPb.Task{}
	key := QueueKey(namespace, name)
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(tokens),
	}

	child1 := span.Child("etcd get")
	gresp, err := s.Kv.Get(ctx, key, opts...)
	if err != nil {
		child1.AddLog(&sg.KV{"etcd get error", err.Error()})
		return nil, err
	}
	child1.Finish()

	if int64(len(gresp.Kvs)) <= tokens {
		for _, item := range gresp.Kvs {
			newTask := &cPb.Task{}
			err = proto.Unmarshal(item.Value, newTask)
			if err != nil {
				span.AddLog(&sg.KV{"unmarshalling error", err.Error()})
				return nil, err
			}
			retTasks[string(item.Key)] = newTask

			child2 := span.Child("etcd delete")
			_, err = s.Kv.Delete(ctx, string(item.Key))
			if err != nil {
				child2.AddLog(&sg.KV{"etcd delete error", err.Error()})
				return nil, err
			}
			child2.Finish()
		}
	}

	return retTasks, nil
}

func (s *StorageEtcd) AddQueue(ctx context.Context, opt *model.TaskOption) error {
	span, _ := sg.FromContext(ctx, "db.AddQueue")
	defer span.Finish()
	fmt.Println(span)

	key := NewQueueKey(opt.Namespace, opt.Name)
	err, value := toString(opt)
	if err != nil {
		return err
	}

	child := span.Child("etcd put")
	_, err = s.Kv.Put(ctx, key, value)
	if err != nil {
		child.AddLog(&sg.KV{"etcd put error", err.Error()})
		return err
	}
	child.Finish()

	return nil
}

func (s *StorageEtcd) RemoveQueue(ctx context.Context, name, namespace string) error {
	span, _ := sg.FromContext(ctx, "db.RemoveQueue")
	defer span.Finish()
	fmt.Println(span)

	child := span.Child("etcd delete")
	key := RemoveQueueKey(namespace, name)
	_, err := s.Kv.Delete(ctx, key, clientv3.WithPrefix())
	if err != nil {
		child.AddLog(&sg.KV{"etcd delete error", err.Error()})
		return err
	}
	child.Finish()

	return nil
}

func (s *StorageEtcd) Close() {
	s.Client.Close()
}
