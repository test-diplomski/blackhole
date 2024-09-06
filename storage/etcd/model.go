package etcd

import (
	"github.com/coreos/etcd/clientv3"
	"time"
)

type StorageEtcd struct {
	Kv     clientv3.KV
	Client *clientv3.Client
}

func New(addr []string, timeout time.Duration) (*StorageEtcd, error) {
	cli, err := clientv3.New(clientv3.Config{
		DialTimeout: timeout,
		Endpoints:   addr,
	})
	if err != nil {
		return nil, err
	}

	return &StorageEtcd{
		Kv:     clientv3.NewKV(cli),
		Client: cli,
	}, nil
}
