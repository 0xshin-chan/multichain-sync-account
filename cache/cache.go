package cache

import (
	"errors"

	lru "github.com/hashicorp/golang-lru"

	"github.com/0xshin-chan/multichain-sync-account/services/http/models"
)

// ListSize lru缓存的容量，最多缓存 120 万条记录
const ListSize = 1200000

// LruCache 封装了两个 LRU 缓存：
// 1. lruStakingRecords：用于存储 Staking 相关数据
// 2. lruBridgeRecords ：用于存储 Bridge 相关数据
type LruCache struct {
	lruStakingRecords *lru.Cache
	lruBridgeRecords  *lru.Cache
}

func NewLruCache() *LruCache {
	lruStakingRecords, err := lru.New(ListSize)
	if err != nil {
		panic(errors.New("Failed to init lruStakingRecord, err :" + err.Error()))
	}
	lruBridgeRecords, err := lru.New(ListSize)
	if err != nil {
		panic(errors.New("Failed to init lruBridgeRecord, err :" + err.Error()))
	}
	// 初始化完成后返回封装好的结构体
	return &LruCache{
		lruStakingRecords: lruStakingRecords,
		lruBridgeRecords:  lruBridgeRecords,
	}
}

// GetStakingRecords 从 lruStakingRecords 里面获取数据
func (lc *LruCache) GetStakingRecords(key string) (*models.StakingResponse, error) {
	// 根据 key 从缓存获取
	result, ok := lc.lruStakingRecords.Get(key)
	if !ok {
		return nil, errors.New("lru get Staking records fail")
	}

	return result.(*models.StakingResponse), nil
}
