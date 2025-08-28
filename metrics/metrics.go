package metrics

import (
	"math/big"

	"github.com/prometheus/client_golang/prometheus"
)

type SyncerMetricer interface {
	RecordChainBlockHeight(chainId string, height *big.Int)
}

type SyncerMetrics struct {
	// gauge: 测量，GaugeVec 是一种指标的类型，可以记录数值
	chainBlockHeight *prometheus.GaugeVec
}

func NewSyncerMetrics(registry *prometheus.Registry, subsystem string) *SyncerMetrics {
	// 创建一个 GaugeVec 指标，指标系统名称subsystem，  chain_id 用于区分不同链
	chainBlockHeight := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:      "chain_block_height",
		Help:      "Different chain block height",
		Subsystem: subsystem,
	}, []string{"chain_id"})

	// 注册指标
	registry.MustRegister(chainBlockHeight)

	return &SyncerMetrics{
		chainBlockHeight: chainBlockHeight,
	}
}

func (rm *SyncerMetrics) RecordChainBlockHeight(chainId string, height *big.Int) {
	rm.chainBlockHeight.WithLabelValues(chainId).Set(float64(height.Uint64()))
}
