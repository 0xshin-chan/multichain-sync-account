package syncclient

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/log"

	"github.com/0xshin-chan/multichain-sync-account/common/bigint"
)

var (
	ErrBatchBlockAheadOfProvider = errors.New("the BatchBlock's internal state is ahead of the provider")
	ErrBlockFallBack             = errors.New("the block fallback, fallback handle it now")
)

type BatchBlock struct {
	rpcClient              *ChainAccountRpcClient
	latestHeader           *BlockHeader // 当前链的最新区块头
	lastTraversedHeader    *BlockHeader // 上一次遍历到的区块头
	blockConfirmationDepth *big.Int     // 确认深度
}

func NewBatchBlock(rpcClient *ChainAccountRpcClient, fromHeader *BlockHeader, confDepth *big.Int) *BatchBlock {
	return &BatchBlock{
		rpcClient:              rpcClient,
		lastTraversedHeader:    fromHeader,
		blockConfirmationDepth: confDepth,
	}
}

func (b *BatchBlock) LatestHeader() *BlockHeader {
	return b.latestHeader
}

func (b *BatchBlock) LastTraversedHeader() *BlockHeader {
	return b.lastTraversedHeader
}

func (b *BatchBlock) NextHeaders(maxSize uint64) ([]BlockHeader, *BlockHeader, bool, error) {
	// 1. 先获取链上最新区块头（latestHeader）
	latestHeader, err := b.rpcClient.GetBlockHeader(nil)
	if err != nil {
		return nil, nil, false, fmt.Errorf("uable to query latest block: %w", err)
	} else if latestHeader == nil {
		return nil, nil, false, fmt.Errorf("latest header unreported")
	} else {
		b.latestHeader = latestHeader
	}
	// 2. 计算允许遍历的最远区块（排除未确认的区块），new(big.Int)返回一个大整数的零值，  .Sub方法返回的值在赋给这个零值
	endHeight := new(big.Int).Sub(latestHeader.Number, b.blockConfirmationDepth)
	// 是math里的方法，结果为正数返回 1 ，结果为负数返回 -1 ， 结果为0返回 0
	if endHeight.Sign() < 0 {
		// 如果还没有足够确认，直接返回
		return nil, nil, false, nil
	}
	if b.lastTraversedHeader != nil {
		// cmp = compare， a.cmp(b), 如果 a 大于 b 返回 1， a 小于 b 返回 -1， 相等返回 0
		// 这里判断之前扫链存的区块号与现在扫链得到的区块号是否相当
		cmp := b.lastTraversedHeader.Number.Cmp(endHeight)
		// 已经追上可用的最高区块，不需要继续
		if cmp == 0 {
			return nil, nil, false, err
			// 内部状态比链上进度还超前（不正常情况）
		} else if cmp > 0 {
			return nil, nil, false, ErrBatchBlockAheadOfProvider
		}
	}
	// 4. 计算下一个需要遍历的起始区块高度
	nextHeight := bigint.Zero
	if b.lastTraversedHeader != nil {
		nextHeight = new(big.Int).Add(b.lastTraversedHeader.Number, bigint.One)
	}
	// 5.取能够遍历的区块高度，根据扫链得到的 endHeight - 确认位，和传进来的 maxSize
	// 如果 maxSize > endHeight - 确认位， 那么也只能取目前最高的链高度，即 endHeight - 确认位
	// 如果 maxSize < endHeight - 确认位， 就取 nextHeight + maxSize - 1
	endHeight = bigint.Clamp(nextHeight, endHeight, maxSize)
	// 需要遍历区块的个数
	count := new(big.Int).Sub(endHeight, nextHeight).Uint64() + 1
	// 6. 逐个拉取区块头
	var headers []BlockHeader
	for i := uint64(0); i < count; i++ {
		height := new(big.Int).Add(nextHeight, new(big.Int).SetUint64(i))
		blockHeader, err := b.rpcClient.GetBlockHeader(height)
		if err != nil {
			log.Error("get block info fail", "err", err)
			return nil, nil, false, err
		}
		headers = append(headers, *blockHeader)
		// 校验链是否连续（区块哈希和父哈希必须匹配）
		if len(headers) == 1 && b.lastTraversedHeader != nil && headers[0].ParentHash != b.lastTraversedHeader.Hash {
			log.Warn("lastTraversedHeader and header zero: parentHash and hash", "parentHash", headers[0].ParentHash, "Hash", b.lastTraversedHeader.Hash)
			return nil, blockHeader, true, ErrBlockFallBack
		}
		if len(headers) > 1 && headers[i-1].Hash != headers[i].ParentHash {
			log.Warn("headers[i-1] nad headers[i] parentHash and hash", "parentHash", headers[i].ParentHash, "Hash", headers[i-1].Hash)
			return nil, blockHeader, true, ErrBlockFallBack
		}
	}
	// 7. 如果有拉取到新区块，则更新 lastTraversedHeader
	numHeaders := len(headers)
	if numHeaders == 0 {
		return nil, nil, false, nil
	}

	b.lastTraversedHeader = &headers[numHeaders-1]
	return headers, nil, false, nil
}
