package etcd

import (
	"Go_Logcollection/pkgs/logger"
	"context"
	"encoding/json"
	"time"

	"github.com/coreos/etcd/clientv3"
)

var (
	client *clientv3.Client
)

// LogEntry 需要收集的日志的配置信息
type LogEntry struct {
	Path  string `json:"path"`  // 日志存放的路径
	Topic string `json:"topic"` // 日志要发往Kafka中的哪个Topic
}

// Init 初始化函数
func Init(addrs string, timeout time.Duration) (err error) {
	client, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{addrs},
		DialTimeout: timeout})
	if err != nil {
		return
	}
	return
}

// GetConf 获取配置信息
func GetConf(key string) (logEntryConf []*LogEntry, err error) {
	// get
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := client.Get(ctx, key)
	cancel()
	if err != nil {
		return
	}
	for _, ev := range resp.Kvs {
		err = json.Unmarshal(ev.Value, &logEntryConf)
		if err != nil {
			return
		}
	}
	return
}

// WatchConf 用于监视key的变化
func WatchConf(key string, newConfCh chan<- []*LogEntry) {
	ch := client.Watch(context.Background(), key)
	// 从通道尝试取值(监视的信息)
	for wresp := range ch {
		for _, evt := range wresp.Events {
			logger.Log.Infof("Type:%v key:%v value:%v\n", evt.Type, string(evt.Kv.Key), string(evt.Kv.Value))
			// 通知taillog.tskMgr
			// 1. 先判断操作的类型
			var newConf []*LogEntry
			if evt.Type != clientv3.EventTypeDelete {
				// 如果是删除操作，手动传递一个空的配置项
				err := json.Unmarshal(evt.Kv.Value, &newConf)
				if err != nil {
					logger.Log.Warnf("unmarshal failed, err:%v\n", err)
					continue
				}
			}
			logger.Log.Infof(" get new conf:%v\n", newConf)
			newConfCh <- newConf
		}
	}
}
