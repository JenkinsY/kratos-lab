package data

import (
	"context"
	v1 "review-o/api/review/v1"
	"review-o/internal/conf"

	consul "github.com/go-kratos/kratos/contrib/registry/consul/v2"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware/recovery"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-kratos/kratos/v2/transport/grpc"
	"github.com/google/wire"
	"github.com/hashicorp/consul/api"
)

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(NewData, NewOperationRepo, NewDiscovery, NewReviewServiceClient)

// Data .
type Data struct {
	// TODO wrapped database client
	rc  v1.ReviewClient
	log *log.Helper
}

// NewData .
func NewData(c *conf.Data, rc v1.ReviewClient, logger log.Logger) (*Data, func(), error) {
	cleanup := func() {
		log.NewHelper(logger).Info("closing the data resources")
	}
	return &Data{rc: rc, log: log.NewHelper(logger)}, cleanup, nil
}

// NewDiscovery 服务发现对象的构造函数
func NewDiscovery(conf *conf.Registry) registry.Discovery {
	// new consul client
	c := api.DefaultConfig()
	c.Address = conf.Consul.Addr // 使用配置文件中注册中心的配置
	c.Scheme = conf.Consul.Scheme

	client, err := api.NewClient(c)
	if err != nil {
		panic(err)
	}
	// new dis with consul client
	dis := consul.New(client)
	return dis
}

// NewReviewServiceClient 创建一个连接 review-service 的GRPC Client端
func NewReviewServiceClient(dis registry.Discovery) v1.ReviewClient {
	// import "github.com/go-kratos/kratos/v2/transport/grpc"
	conn, err := grpc.DialInsecure(
		context.Background(),
		// grpc.WithEndpoint("127.0.0.1:9092"),
		grpc.WithEndpoint("discovery:///review-service"),
		grpc.WithDiscovery(dis),
		grpc.WithMiddleware(
			recovery.Recovery(),
		),
	)
	if err != nil {
		panic(err)
	}
	return v1.NewReviewClient(conn)
}
