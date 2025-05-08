package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"strconv"
	"strings"
	"time"

	"review-service/internal/biz"
	"review-service/internal/data/model"
	"review-service/internal/data/query"
	"review-service/pkg/snowflake"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type reviewRepo struct {
	data *Data
	log  *log.Helper
}

// NewReviewRepo .
func NewReviewRepo(data *Data, logger log.Logger) biz.ReviewRepo {
	return &reviewRepo{
		data: data,
		log:  log.NewHelper(logger),
	}
}

func (r *reviewRepo) SaveReview(ctx context.Context, review *model.ReviewInfo) (*model.ReviewInfo, error) {
	err := r.data.query.ReviewInfo.
		WithContext(ctx).
		Create(review)
	return review, err
}

func (r *reviewRepo) GetReviewByOrderID(ctx context.Context, orderID int64) ([]*model.ReviewInfo, error) {
	return r.data.query.ReviewInfo.
		WithContext(ctx).
		Where(r.data.query.ReviewInfo.OrderID.Eq(orderID)).
		Find()
}

func (r *reviewRepo) GetReview(ctx context.Context, reviewID int64) (*model.ReviewInfo, error) {
	return r.data.query.ReviewInfo.
		WithContext(ctx).
		Where(r.data.query.ReviewInfo.ReviewID.Eq(reviewID)).
		First()
}

// SaveReply 保存评价回复
func (r *reviewRepo) SaveReply(ctx context.Context, reply *model.ReviewReplyInfo) (*model.ReviewReplyInfo, error) {
	// 1. 数据校验
	// 1.1 数据合法性校验（已回复的评价不允许商家再次回复）
	// 先用评价ID查库,看下是否已回复
	review, err := r.data.query.ReviewInfo.
		WithContext(ctx).
		Where(r.data.query.ReviewInfo.ReviewID.Eq(reply.ReviewID)).
		First()
	if err != nil {
		return nil, err
	}
	if review.HasReply == 1 {
		return nil, errors.New("该评价已回复")
	}
	// 1.2 水平越权校验（A商家只能回复自己的不能回复B商家的）
	// 举例子：用户A删除订单，userID + orderID 当条件去查询订单然后删除
	if review.StoreID != reply.StoreID {
		return nil, errors.New("水平越权,该评价不属于该商家")
	}
	// 2. 更新数据库中的数据（评价回复表和评价表要同时更新，涉及到事务操作）
	// 事务操作
	r.data.query.Transaction(func(tx *query.Query) error {
		// 回复表插入一条数据
		if err := tx.ReviewReplyInfo.
			WithContext(ctx).
			Save(reply); err != nil {
			r.log.WithContext(ctx).Errorf("SaveReply create reply fail, err:%v", err)
			return err
		}
		// 评价表更新hasReply字段
		if _, err := tx.ReviewInfo.
			WithContext(ctx).
			Where(tx.ReviewInfo.ReviewID.Eq(reply.ReviewID)).
			Update(tx.ReviewInfo.HasReply, 1); err != nil {
			r.log.WithContext(ctx).Errorf("SaveReply update review fail, err:%v", err)
			return err
		}
		return nil
	})
	// 3. 返回
	return reply, err
}

// GetReviewReply 获取评价回复
func (r *reviewRepo) GetReviewReply(ctx context.Context, reviewID int64) (*model.ReviewReplyInfo, error) {
	return r.data.query.ReviewReplyInfo.
		WithContext(ctx).
		Where(r.data.query.ReviewReplyInfo.ReviewID.Eq(reviewID)).
		First()
}

// AuditReview 审核评价（运营对用户的评价进行审核）
func (r *reviewRepo) AuditReview(ctx context.Context, param *biz.AuditParam) error {
	_, err := r.data.query.ReviewInfo.
		WithContext(ctx).
		Where(r.data.query.ReviewInfo.ReviewID.Eq(param.ReviewID)).
		Updates(map[string]interface{}{
			"status":     param.Status,
			"op_user":    param.OpUser,
			"op_reason":  param.OpReason,
			"op_remarks": param.OpRemarks,
		})
	return err
}

// AppealReview 申诉评价（商家对用户评价进行申诉）
func (r *reviewRepo) AppealReview(ctx context.Context, param *biz.AppealParam) (*model.ReviewAppealInfo, error) {
	// 先查询有没有申诉
	ret, err := r.data.query.ReviewAppealInfo.
		WithContext(ctx).
		Where(
			query.ReviewAppealInfo.ReviewID.Eq(param.ReviewID),
			query.ReviewAppealInfo.StoreID.Eq(param.StoreID),
		).First()
	r.log.Debugf("AppealReview query, ret:%v err:%v", ret, err)
	// 其他查询错误
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}
	if err == nil && ret.Status > 10 {
		return nil, errors.New("该评价已有审核过的申诉记录")
	}
	// 查询不到审核过的申诉记录
	// 1. 有申诉记录但是处于待审核状态，需要更新
	// if ret != nil{
	// 	// update
	// }else{
	// 	// insert
	// }
	// 2. 没有申诉记录，需要创建
	appeal := &model.ReviewAppealInfo{
		ReviewID:  param.ReviewID,
		StoreID:   param.StoreID,
		Status:    10,
		Reason:    param.Reason,
		Content:   param.Content,
		PicInfo:   param.PicInfo,
		VideoInfo: param.VideoInfo,
	}
	if ret != nil {
		appeal.AppealID = ret.AppealID
	} else {
		appeal.AppealID = snowflake.GenID()
	}
	err = r.data.query.ReviewAppealInfo.
		WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "review_id"}, // ON DUPLICATE KEY
			},
			DoUpdates: clause.Assignments(map[string]interface{}{ // UPDATE
				"status":     appeal.Status,
				"content":    appeal.Content,
				"reason":     appeal.Reason,
				"pic_info":   appeal.PicInfo,
				"video_info": appeal.VideoInfo,
			}),
		}).
		Create(appeal) // INSERT
	r.log.Debugf("AppealReview, err:%v", err)
	return appeal, err
}

// AuditAppeal 审核申诉（运营对商家的申诉进行审核，审核通过会隐藏该评价）
func (r *reviewRepo) AuditAppeal(ctx context.Context, param *biz.AuditAppealParam) error {
	err := r.data.query.Transaction(func(tx *query.Query) error {
		// 更新申诉表
		_, err := tx.ReviewAppealInfo.
			WithContext(ctx).
			Where(tx.ReviewAppealInfo.AppealID.Eq(param.AppealID)).
			Updates(map[string]interface{}{
				"status":  param.Status,
				"op_user": param.OpUser,
			})
		if err != nil {
			return err
		}
		// 更新评价表
		if param.Status == 20 {
			// 隐藏评价
			_, err := tx.ReviewInfo.
				WithContext(ctx).
				Where(tx.ReviewInfo.ReviewID.Eq(param.ReviewID)).
				Update(tx.ReviewInfo.Status, 40)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// ListReviewByUserID 根据userID查询所有评价
func (r *reviewRepo) ListReviewByUserID(ctx context.Context, userID int64, offset, limit int) ([]*model.ReviewInfo, error) {
	reviews, err := r.data.query.ReviewInfo.
		WithContext(ctx).
		Where(r.data.query.ReviewInfo.UserID.Eq(userID)).
		Order(r.data.query.ReviewInfo.ID.Desc()).
		Offset(offset).
		Limit(limit).
		Find()
	if err != nil {
		return nil, err
	}
	return reviews, nil
}

// ListReviewByStoreID 根据storeID查询所有评价
func (r *reviewRepo) ListReviewByStoreID(ctx context.Context, storeID int64, offset, limit int) ([]*biz.MyReviewInfo, error) {
	// return r.getData1(ctx, storeID, offset, limit) // 第一版直接查ES
	return r.getData2(ctx, storeID, offset, limit) // 第二版增加缓存和singlefilght
}

func (r *reviewRepo) getData1(ctx context.Context, storeID int64, offset, limit int) ([]*biz.MyReviewInfo, error) {
	// 去ES里面查询评价
	resp, err := r.data.es.Search().
		Index("review").
		From(offset).
		Size(limit).
		Query(&types.Query{
			Bool: &types.BoolQuery{
				Filter: []types.Query{
					{
						Term: map[string]types.TermQuery{
							"store_id": {Value: storeID},
						},
					},
				},
			},
		}).
		Do(ctx)
	fmt.Printf("--> es search: %v %v\n", resp, err)
	if err != nil {
		return nil, err
	}
	fmt.Printf("es result total:%v\n", resp.Hits.Total.Value)
	// 反序列化数据
	// resp.Hits.Hits[0].Source_(json.RawMessage)  ==>  model.ReviewInfo
	list := make([]*biz.MyReviewInfo, 0, resp.Hits.Total.Value) // ?
	// list := make([]*model.ReviewInfo)                           // ?

	for _, hit := range resp.Hits.Hits {
		tmp := &biz.MyReviewInfo{}
		if err := json.Unmarshal(hit.Source_, tmp); err != nil {
			r.log.Errorf("json.UnmarshalJSON failed, err:%v", err)
			continue
		}
		list = append(list, tmp)
	}

	return list, nil
}

func (r *reviewRepo) getData2(ctx context.Context, storeID int64, offset, limit int) ([]*biz.MyReviewInfo, error) {
	// 取数据
	// 1. 先查询redis缓存
	// 2. 缓存没有则查ES
	// 3. 通过 singleflight 合并短时间内大量的并发请求

	key := fmt.Sprintf("review:%d:%d:%d", storeID, offset, limit)

	b, err := r.getDataBySingleflight(ctx, key)
	if err != nil {
		return nil, err
	}

	// hm == req.Hits
	hm := new(types.HitsMetadata)

	if err := json.Unmarshal(b, hm); err != nil {
		r.log.Errorf("json.UnmarshalJSON failed, err:%v", err)
		return nil, err
	}

	// 反序列化数据
	// resp.Hits.Hits[0].Source_(json.RawMessage)  ==>  model.ReviewInfo
	list := make([]*biz.MyReviewInfo, 0, hm.Total.Value) // ?
	// list := make([]*model.ReviewInfo)                           // ?

	for _, hit := range hm.Hits {
		tmp := &biz.MyReviewInfo{}
		if err := json.Unmarshal(hit.Source_, tmp); err != nil {
			r.log.Errorf("json.UnmarshalJSON failed, err:%v", err)
			continue
		}
		list = append(list, tmp)
	}

	return list, nil
}

// key review:storeID:offset:limit --> "[{},{},{}]"
// key review:123:1:10
// json.unmarshal([]byte)

var g singleflight.Group

// getDataBySingleflight 通过 singleflight 合并短时间内大量的并发请求
func (r *reviewRepo) getDataBySingleflight(ctx context.Context, key string) ([]byte, error) {
	v, err, shared := g.Do(key, func() (interface{}, error) {
		// 1. 先查询redis缓存
		data, err := r.getDataFromCache(ctx, key)
		r.log.Debugf("getDataFromCache, key:%s, data:%s, err:%v", key, data, err)
		if err == nil {
			return data, nil
		}
		// 2. 只有在redis缓存没有这个key时查ES
		if errors.Is(err, redis.Nil) {
			// 2.1 查询ES
			data, err = r.getDataFromES(ctx, key)
			if err == nil {
				// 2.2 设置缓存
				if err := r.setCache(ctx, key, data); err != nil {
					r.log.Errorf("setCache failed, err:%v", err)
				}
				return data, nil
			}
			return nil, err
		}
		// 查缓存失败了，直接返回错误，不向下传导压力
		return nil, err
	})
	r.log.Debugf("getDataBySingleflight, v:%s, err:%v, shared:%v", v, err, shared)
	if err != nil {
		return nil, err
	}
	return v.([]byte), nil
}

// 返回 []byte 是因为 json.Unmarshal() 传入的 hit.Source_ 也是 []byte 类型，
// 直接将原始的数据存入缓存，使得缓存和ES一致
// getDataFromCache 读缓存
func (r *reviewRepo) getDataFromCache(ctx context.Context, key string) ([]byte, error) {
	r.log.Debugf("getDataFromCache, key:%s", key)
	return r.data.rdb.Get(ctx, key).Bytes()
}

// setCache 设置缓存
func (r *reviewRepo) setCache(ctx context.Context, key string, data []byte) error {
	return r.data.rdb.Set(ctx, key, data, time.Second*10).Err()
}

// getDataFromES 从ES查询数据
func (r *reviewRepo) getDataFromES(ctx context.Context, key string) ([]byte, error) {
	values := strings.Split(key, ":")
	if len(values) != 4 {
		return nil, errors.New("key format error")
	}
	index, storeID, offsetStr, limitStr := values[0], values[1], values[2], values[3]

	offset, err := strconv.Atoi(offsetStr)
	if err != nil {
		return nil, err
	}
	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		return nil, err
	}

	resp, err := r.data.es.Search().
		Index(index).
		From(offset).
		Size(limit).
		Query(&types.Query{
			Bool: &types.BoolQuery{
				Filter: []types.Query{
					{
						Term: map[string]types.TermQuery{
							"store_id": {Value: storeID},
						},
					},
				},
			},
		}).
		Do(ctx)
	fmt.Printf("--> es search: %v %v\n", resp, err)
	if err != nil {
		return nil, err
	}

	return json.Marshal(resp.Hits)
}
