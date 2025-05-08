package data

import (
	"errors"
	"review-service/internal/conf"
	"review-service/internal/data/query"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
	"gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(NewData, NewReviewRepo, NewDB, NewEsClient)

// Data .
type Data struct {
	// TODO wrapped database client
	// db *gorm.DB
	query *query.Query
	log   *log.Helper
	es    *elasticsearch.TypedClient
}

// NewData .
func NewData(db *gorm.DB, es *elasticsearch.TypedClient, logger log.Logger) (*Data, func(), error) {
	cleanup := func() {
		log.NewHelper(logger).Info("closing the data resources")
	}
	// 非常重要!为GEN生成的query代码设置数据库连接对象
	query.SetDefault(db)
	return &Data{query: query.Q, es: es, log: log.NewHelper(logger)}, cleanup, nil
}

func NewDB(cfg *conf.Data) (*gorm.DB, error) {
	switch strings.ToLower(cfg.Database.GetDriver()) {
	case "mysql":
		return gorm.Open(mysql.Open(cfg.Database.GetSource()))
	case "sqlite":
		return gorm.Open(sqlite.Open(cfg.Database.GetSource()))
	}
	return nil, errors.New("connect db fail unsupported db driver")
}

// NewEsClient
func NewEsClient(cfg *conf.ElasticSearch) (*elasticsearch.TypedClient, error) {
	// ES 配置
	c := elasticsearch.Config{
		Addresses: cfg.GetAddr(),
	}

	// 创建客户端连接
	return elasticsearch.NewTypedClient(c)
}
