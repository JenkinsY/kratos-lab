package server

import (
	"review-service/internal/conf"

	consul "github.com/go-kratos/kratos/contrib/registry/consul/v2"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/google/wire"
	"github.com/hashicorp/consul/api"
)

// ProviderSet is server providers.
var ProviderSet = wire.NewSet(NewGRPCServer, NewHTTPServer, NewRegistrar)

func NewRegistrar(conf *conf.Registry) registry.Registrar {
	// new consul client
	c := api.DefaultConfig()
	c.Address = conf.Consul.Addr // 使用配置文件中的值
	c.Scheme = conf.Consul.Scheme

	client, err := api.NewClient(c)
	if err != nil {
		panic(err)
	}
	// new reg with consul client
	reg := consul.New(client, consul.WithHealthCheck(true))
	return reg
}

//1111
