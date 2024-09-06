package etcd

import (
	"fmt"
	"github.com/c12s/blackhole/model"
	"gopkg.in/yaml.v2"
	"strings"
)

const (
	queues   = "queues"
	tasks    = "tasks"
	qdefault = "default"
	detail   = "detail"
)

//TODO: TEST IS NAMESPACE EMPTY STRING AND IF IS, THAN USE DEFAULT AS A NAMESPACE!!!

// queues/queue_name/tasks -> will be stored in db
// queue_name -> namespace:queue_name | for namespace exclusive queue use namespace:namespace and forceNSQueueName rule
// ex: queues/mynamespace:myqueue1/tasks | queues/default:myqueue1/tasks
func QueueKey(namespace, queueName string) string {
	mid := fmt.Sprintf("%s:%s", namespace, queueName)
	s := []string{queues, mid, tasks}
	return strings.Join(s, "/")
}

// queues/queue_name/tasks/task_group/task_name -> will be stored in db
// queue_name -> namespace:queue_name | for namespace exclusive queue use namespace:namespace and forceNSQueueName rule
// task_group -> user provided name of the task
// task_name -> task_name_id_in_group_timestamp
// ex: queues/mynamespace:myqueue1/tasks/mytask/mytask_1_1234567890 | queues/default:myqueue1/tasks/mytask/mytask_1_1234567890
func TaskKey(namespace, queueName, taskName string, timestamp int64, id int) string {
	prefix := TaskGroupKey(namespace, queueName, taskName)
	fTaskName := fmt.Sprintf("%s_%d_%d", taskName, id, timestamp)
	s := []string{prefix, fTaskName}
	return strings.Join(s, "/")
}

// queues/queue_name/tasks/task_group
// queue_name -> namespace:queue_name | for namespace exclusive queue use namespace:namespace and forceNSQueueName rule
// task_group -> user provided name of the task that holds other task in that group
func TaskGroupKey(namespace, queueName, task_group_name string) string {
	prefix := QueueKey(namespace, queueName)
	s := []string{prefix, task_group_name}
	return strings.Join(s, "/")

}

// On this key all details about queue is stored: maxworkers, retry, ...
// If new config come on this key, it changes this key data, and queue is updated
// queues/queue_name/detail -> will be stored in db
// queue_name -> namespace:queue_name | for namespace exclusive queue use namespace:namespace and forceNSQueueName rule
// ex: queues/mynamespace:myqueue1/detail | queues/default:myqueue1/detail
func NewQueueKey(namespace, queueName string) string {
	mid := fmt.Sprintf("%s:%s", namespace, queueName)
	s := []string{queues, mid, detail}
	return strings.Join(s, "/")
}

// queues/queue_name
// When deleting queue there are two things to remove
// queue_name -> namespace:queue_name | for namespace exclusive queue use namespace:namespace and forceNSQueueName rule
// ex: queues/mynamespace:myqueue1 | queues/default:myqueue1
// /detail and /tasks part all of them need to be deleted
func RemoveQueueKey(namespace, queueName string) string {
	mid := fmt.Sprintf("%s:%s", namespace, queueName)
	s := []string{queues, mid}
	return strings.Join(s, "/")
}

func toString(q *model.TaskOption) (error, string) {
	y, err := yaml.Marshal(*q)
	if err != nil {
		return err, ""
	}
	return nil, string(y)
}
