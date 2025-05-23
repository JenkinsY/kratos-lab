package server

import (
	v1 "bubble/api/bubble/v1"
	"bubble/internal/conf"
	"bubble/internal/service"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware/recovery"
	"github.com/go-kratos/kratos/v2/transport/http"
)

// NewHTTPServer new an HTTP server.
func NewHTTPServer(c *conf.Server, todo *service.TodoService, logger log.Logger) *http.Server {
	var opts = []http.ServerOption{
		http.Middleware(
			recovery.Recovery(),
		),
	}
	if c.Http.Network != "" {
		opts = append(opts, http.Network(c.Http.Network))
	}
	if c.Http.Addr != "" {
		opts = append(opts, http.Address(c.Http.Addr))
	}
	if c.Http.Timeout != nil {
		opts = append(opts, http.Timeout(c.Http.Timeout.AsDuration()))
	}
	// 替换默认的HTTP响应编码器
	opts = append(opts, http.ResponseEncoder(responseEncoder))
	// 替换默认的错误响应编码器
	opts = append(opts, http.ErrorEncoder(errorEncoder))

	srv := http.NewServer(opts...)
	v1.RegisterTodoHTTPServer(srv, todo)
	return srv
}
