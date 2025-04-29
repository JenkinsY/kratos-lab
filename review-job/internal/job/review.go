// 类似main函数中http和grpc的实现，只要实现transport.Server的Start和Stop接口即可
package job

import (
	"context"
	"encoding/json"
	"errors"
	"review-job/internal/conf"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/segmentio/kafka-go"
)

// 评价数据流处理任务

// JobWorker 自定义执行job的结构体，实现 transport.Server

type JobWorker struct {
	kafkareader *kafka.Reader // kafka reader
	esClient    *ESClient     // elasticsearch client
	log         *log.Helper
}

type ESClient struct {
	*elasticsearch.TypedClient
	index string
}

func NewJobWorker(kafkaReader *kafka.Reader, esClient *ESClient, logger log.Logger) *JobWorker {
	return &JobWorker{
		kafkareader: kafkaReader,
		esClient:    esClient,
		log:         log.NewHelper(logger),
	}
}

func NewkafkaReader(cfg *conf.Kafka) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.Brokers,
		Topic:   cfg.Topic,
		GroupID: cfg.GroupId, // 指定消费者组ID
	})
}

func NewESClient(cfg *conf.ElasticSearch) (*ESClient, error) {
	// ES配置
	c := elasticsearch.Config{
		Addresses: cfg.Addr,
	}

	// 创建客户端连接
	client, err := elasticsearch.NewTypedClient(c)
	if err != nil {
		return nil, err
	}
	return &ESClient{
		TypedClient: client,
		index:       cfg.Index,
	}, nil
}

// Msg 定义Kafka中接收到的数据
type Msg struct {
	Type     string                   `json:"type"`     // 消息类型
	Database string                   `json:"database"` // 数据库名称
	Table    string                   `json:"table"`    // 表名称
	IsDdl    bool                     `json:"isDdl"`    // 是否DDL语句
	Data     []map[string]interface{} `json:"data"`     // 数据
}

// Start kratos程序启动之后会调用的方法
// ctx 是kratos框架启动时传入的ctx，是带有退出取消的
func (jw JobWorker) Start(ctx context.Context) error {
	jw.log.Debug("JobWorker Start ...")
	// 1. 从Kafka中读取MySQL的数据变更消息
	// 接收消息
	for {
		m, err := jw.kafkareader.ReadMessage(ctx)
		if errors.Is(err, context.Canceled) {
			jw.log.Debug("context canceled, exiting...")
			return nil
		}
		if err != nil {
			jw.log.Errorf("readMessage from kafka failed, err:", err)
			break
		}
		jw.log.Debugf("message at topic/partition/offset %v%v%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
		// 2. 将完整的评价数据写入ES
		msg := new(Msg)
		if err := json.Unmarshal(m.Value, msg); err != nil {
			jw.log.Errorf("unmarshal message failed, err:", err)
			continue
		}

		// 补充！
		// 实际的业务场景可能需要在这里增加一个步骤：对数据做业务处理
		// 例如：把两张表的数据合成一个文档写入ES

		if msg.Type == "INSERT" {
			// 往ES中新增文档
			for idx := range msg.Data {
				jw.indexDocument(msg.Data[idx])
			}
		} else {
			// 往ES中更新文档
			for idx := range msg.Data {
				jw.updateDocument(msg.Data[idx])
			}
		}
	}
	return nil
}

func (jw JobWorker) Stop(context.Context) error {
	jw.log.Debug("JobWorker Stop ...")
	// 程序退出前关闭Reader
	return jw.kafkareader.Close()
}

// indexDocument 索引文档
func (jw JobWorker) indexDocument(d map[string]interface{}) {
	reviewID := d["review_id"].(string)
	// 添加文档
	resp, err := jw.esClient.Index(jw.esClient.index).
		Id(reviewID).
		Document(d).
		Do(context.Background())
	if err != nil {
		jw.log.Errorf("indexing document failed, err:%v\n", err)
		return
	}
	jw.log.Debugf("result:%#v\n", resp.Result)
}

// updateDocument 更新文档
func (jw JobWorker) updateDocument(d map[string]interface{}) {
	reviewID := d["review_id"].(string)
	resp, err := jw.esClient.Update(jw.esClient.index, reviewID).
		Doc(d). // 使用结构体变量更新
		Do(context.Background())
	if err != nil {
		jw.log.Errorf("update document failed, err:%v\n", err)
		return
	}
	jw.log.Debugf("result:%v\n", resp.Result)
}
