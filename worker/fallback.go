package worker

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/log"

	"github.com/huahaiwudi/multichain-sync-account/common/bigint"
	"github.com/huahaiwudi/multichain-sync-account/common/retry"
	"github.com/huahaiwudi/multichain-sync-account/common/tasks"
	"github.com/huahaiwudi/multichain-sync-account/config"
	"github.com/huahaiwudi/multichain-sync-account/database"
	"github.com/huahaiwudi/multichain-sync-account/rpcclient/syncclient"
)

type FallBack struct {
	deposit        *Deposit
	database       *database.DB
	rpcClient      *syncclient.ChainAccountRpcClient
	resourceCtx    context.Context
	resourceCancel context.CancelFunc
	tasks          tasks.Group
	ticker         *time.Ticker
	confirmations  uint64
}

func NewFallBack(cfg *config.Config, db *database.DB, rpcClient *syncclient.ChainAccountRpcClient, deposit *Deposit, shutdown context.CancelCauseFunc) (*FallBack, error) {
	resCtx, resCancel := context.WithCancel(context.Background())

	return &FallBack{
		deposit:        deposit,
		database:       db,
		rpcClient:      rpcClient,
		resourceCtx:    resCtx,
		resourceCancel: resCancel,
		ticker:         time.NewTicker(time.Second * 3),
		tasks: tasks.Group{HandleCrit: func(err error) {
			shutdown(fmt.Errorf("cretical error in deposit: %w", err))
		}},
		confirmations: uint64(cfg.ChainNode.Confirmations),
	}, nil
}

func (f *FallBack) Start() error {
	log.Info("starting f...")
	f.tasks.Go(func() error {
		for {
			select {
			case <-f.ticker.C:
				log.Info("fallback task", "depositIsFallBack", f.deposit.isFallBack)
				if f.deposit.isFallBack {
					log.Info("notified of fallback", "fallbackBlockNumber", f.deposit.fallbackBlockHeader.Number.String())
					if err := f.onFallBack(f.deposit.fallbackBlockHeader); err != nil {
						log.Error("handle fallback block fail", "err", err)
					}

					dbLatestBlockHeader, err := f.database.Blocks.LatestBlocks()
					if err != nil {
						log.Error("Query latest block fail", "err", err)
						return err
					}
					f.deposit.blockBatch = syncclient.NewBatchBlock(f.rpcClient, dbLatestBlockHeader, big.NewInt(int64(f.confirmations)))
					f.deposit.isFallBack = false
					f.deposit.fallbackBlockHeader = nil
				}

			case <-f.resourceCtx.Done():
				log.Info("stop fallback in worker")
				return nil
			}
		}
	})
	return nil
}

func (f *FallBack) Close() error {
	var result error
	f.resourceCancel()
	if err := f.tasks.Wait(); err != nil {
		result = errors.Join(result, fmt.Errorf("failed to wait tasks: %w", err))
	}
	return result
}

func (f *FallBack) onFallBack(fallbackBlockHeader *syncclient.BlockHeader) error {
	// reorg_block：要插入到 reorg_blocks 表的区块（被回滚的旧区块）
	var reorgBlockHeader []database.ReorgBlocks
	// chainBlocks：要删除的本地区块
	var chainBlocks []database.Blocks

	lastBlockHeader := fallbackBlockHeader

	for {
		// 取上一个区块号
		lastBlockNumber := new(big.Int).Sub(lastBlockHeader.Number, bigint.One)

		log.Info("start get block header info", "lastBlockNumber", lastBlockNumber)

		// 从链上查询该区块
		chainBlockHeader, err := f.rpcClient.GetBlockHeader(lastBlockNumber)
		if err != nil {
			log.Warn("query block from chain err", "err", err)
		}
		// 从数据库查询该区块
		dbBlockHeader, err := f.database.Blocks.QueryBlocksByNumber(lastBlockHeader.Number)
		if err != nil {
			log.Warn("query block from database err", "err", err)
		}
		log.Info("query blocks success", "dbBlockHeader", dbBlockHeader)

		// 保存到本地缓存，后面要更新数据库
		chainBlocks = append(chainBlocks, database.Blocks{
			Hash:       dbBlockHeader.Hash,
			ParentHash: dbBlockHeader.ParentHash,
			Number:     dbBlockHeader.Number,
			Timestamp:  dbBlockHeader.Timestamp,
		})
		reorgBlockHeader = append(reorgBlockHeader, database.ReorgBlocks{
			Hash:       dbBlockHeader.Hash,
			ParentHash: dbBlockHeader.ParentHash,
			Number:     dbBlockHeader.Number,
			Timestamp:  dbBlockHeader.Timestamp,
		})
		log.Info("lastBlockHeader chainBlockHeader",
			"lastBlockParentHash", lastBlockHeader.ParentHash,
			"lastBlockNumber", lastBlockHeader.Number,
			"chanBlockHash", chainBlockHeader.Hash,
			"chainBlockHeaderNumber", chainBlockHeader.Number)

		// 如果父哈希一致，说明找到了分叉点
		if lastBlockHeader.ParentHash == dbBlockHeader.Hash {
			break
		}
		// 否则继续往上追溯
		lastBlockHeader = chainBlockHeader
	}
	// 查询所有业务
	businessList, err := f.database.Business.QueryBusinessList()
	if err != nil {
		log.Error("Query business list fail", "err", err)
		return err
	}
	// 需要回滚的余额调整
	var fallbackBalances []*database.TokenBalance
	// 遍历业务，找到受影响的交易
	for _, businessItem := range businessList {
		log.Info("handle business", "BusinessUid", businessItem.BusinessUid)
		// 查找回退区块之间的交易
		transactionsList, err := f.database.Transactions.QueryFallBackTransactions(
			businessItem.BusinessUid,
			lastBlockHeader.Number,
			fallbackBlockHeader.Number,
		)
		if err != nil {
			return err
		}
		// 构造回退余额变更
		for _, transaction := range transactionsList {
			fbb := &database.TokenBalance{
				FromAddress:  transaction.FromAddress,
				ToAddress:    transaction.ToAddress,
				TokenAddress: transaction.TokenAddress,
				Balance:      transaction.Amount,
				TxType:       transaction.TxType,
			}
			fallbackBalances = append(fallbackBalances, fbb)
		}
	}

	// 使用重试策略，确保数据库更新成功
	retryStrategy := &retry.ExponentialStrategy{Min: 1000, Max: 20_000, MaxJitter: 250}
	if _, err := retry.Do[interface{}](f.resourceCtx, 10, retryStrategy, func() (interface{}, error) {
		// 开启事务
		if err := f.database.Transaction(func(tx *database.DB) error {
			// 保存 Reorg 区块
			if len(reorgBlockHeader) > 0 {
				log.Info("Store reorg block success", "totalTx", len(reorgBlockHeader))
				if err := tx.ReorgBlocks.StoreReorgBlocks(reorgBlockHeader); err != nil {
					return err
				}
			}

			// 删除旧区块
			if len(chainBlocks) > 0 {
				log.Info("delete block success", "totalTx", len(reorgBlockHeader))
				if err := tx.Blocks.DeleteBlocksByNumber(chainBlocks); err != nil {
					return err
				}
			}

			// 如果回退区块号大于分叉点，需要处理交易/余额回滚
			if fallbackBlockHeader.Number.Cmp(lastBlockHeader.Number) > 0 {
				for _, businessItem := range businessList {
					if err := tx.Deposits.HandleFallBackDeposits(businessItem.BusinessUid, lastBlockHeader.Number, fallbackBlockHeader.Number); err != nil {
						return err
					}
					if err := tx.Internals.HandleFallBackInternals(businessItem.BusinessUid, lastBlockHeader.Number, fallbackBlockHeader.Number); err != nil {
						return err
					}
					if err := tx.Withdraws.HandleFallBackWithdraw(businessItem.BusinessUid, lastBlockHeader.Number, fallbackBlockHeader.Number); err != nil {
						return err
					}
					if err := tx.Transactions.HandleFallBackTransactions(businessItem.BusinessUid, lastBlockHeader.Number, fallbackBlockHeader.Number); err != nil {
						return err
					}
					if err := tx.Balances.UpdateFallBackBalance(businessItem.BusinessUid, fallbackBalances); err != nil {
						return err
					}
				}
			}
			return nil
		}); err != nil {
			log.Error("unable to persist batch", "err", err)
			return nil, err
		}
		return nil, nil
	}); err != nil {
		return err
	}
	return nil
}
