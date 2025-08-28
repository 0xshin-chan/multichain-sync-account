package http

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/log"

	"github.com/huahaiwudi/multichain-sync-account/common/retry"
	"github.com/huahaiwudi/multichain-sync-account/common/tasks"
	"github.com/huahaiwudi/multichain-sync-account/database"
)

type DepositNotify struct {
	db             *database.DB
	businessIds    []string
	notifyClient   map[string]*NotifyClient // 每个 businessIds 对应的 NotifyClient
	resourceCtx    context.Context
	resourceCancel context.CancelFunc
	tasks          tasks.Group
	ticker         *time.Ticker

	shutdown context.CancelCauseFunc
	stopped  atomic.Bool
}

func NewDepositNotify(db *database.DB, notifyClient map[string]*NotifyClient, businessIds []string, shutdown context.CancelCauseFunc) (*DepositNotify, error) {
	resCtx, resCancel := context.WithCancel(context.Background())
	return &DepositNotify{
		db:             db,
		notifyClient:   notifyClient,
		businessIds:    businessIds,
		resourceCtx:    resCtx,
		resourceCancel: resCancel,
		tasks: tasks.Group{HandleCrit: func(err error) {
			shutdown(fmt.Errorf("critical error in internals: %w", err))
		}},
		ticker: time.NewTicker(time.Second * 5),
	}, nil
}

func (dn *DepositNotify) Start() error {
	dn.tasks.Go(func() error {
		for {
			select {
			case <-dn.ticker.C:
				var txn []Transaction
				for _, businessId := range dn.businessIds {
					log.Info("txn and businessId", "txn", txn, "businessId", businessId)

					// 从数据库查询需要通知的充值记录
					needNotifyDeposits, err := dn.db.Deposits.QueryNotifyDeposits(businessId)
					if err != nil {
						log.Error("Query notify deposits fail", "err", err)
						return err
					}

					// 构建通知请求参数
					notifyRequestTxn, err := dn.BuildNotifyDeposits(needNotifyDeposits)
					if err != nil {
						log.Error("notify fail", "err", err)
						return err
					}

					// 调用商户通知接口
					notify, err := dn.notifyClient[businessId].BusinessNotify(notifyRequestTxn)
					if err != nil {
						log.Error("notify business platform fail", "err")
						return err
					}

					// 修改充值记录的状态
					err = dn.AfterNotify(businessId, needNotifyDeposits, notify)
					if err != nil {
						log.Error("handle notifier transactions fail", "err", err)
						return err
					}
				}
			case <-dn.resourceCtx.Done():
				log.Info("stop internals in worker")
				return nil
			}
		}
	})
	return nil
}

func (dn *DepositNotify) Close() error {
	var result error
	dn.resourceCancel()
	dn.ticker.Stop()
	log.Info("stop withdraw......")
	if err := dn.tasks.Wait(); err != nil {
		result = errors.Join(result, fmt.Errorf("failed to await withdraw %w", err))
		return result
	}
	log.Info("stop withdraw success")
	return nil
}

func (dn *DepositNotify) BuildNotifyDeposits(deposits []*database.Deposits) (*NotifyRequest, error) {
	var notifyTransactions []*Transaction
	for _, deposit := range deposits {
		var txStatus database.TxStatus
		if deposit.Status == database.TxStatusUnSafe || deposit.Status == database.TxStatusUnSafeNotifyFail {
			txStatus = database.TxStatusUnSafe
		}
		if deposit.Status == database.TxStatusSafe || deposit.Status == database.TxStatusSafeNotifyFail {
			txStatus = database.TxStatusSafe
		}
		if deposit.Status == database.TxStatusFinalized || deposit.Status == database.TxStatusFinalizedNotifyFail {
			txStatus = database.TxStatusFinalized
		}
		if deposit.Status == database.TxStatusFallback || deposit.Status == database.TxStatusFallbackNotifyFail {
			txStatus = database.TxStatusFallback
		}

		txItem := &Transaction{
			BlockHash:    deposit.BlockHash.String(),
			BlockNumber:  deposit.BlockNumber.Uint64(),
			Hash:         deposit.TxHash.String(),
			FromAddress:  deposit.FromAddress.String(),
			ToAddress:    deposit.ToAddress.String(),
			Value:        deposit.Amount.String(),
			Fee:          deposit.MaxFeePerGas,
			TxType:       deposit.TxType,
			TxStatus:     txStatus,
			Confirms:     deposit.Confirms,
			TokenAddress: deposit.TokenAddress.String(),
			TokenId:      deposit.TokenId,
			TokenMeta:    deposit.TokenMeta,
		}
		notifyTransactions = append(notifyTransactions, txItem)
	}
	notifyReq := &NotifyRequest{
		Txn: notifyTransactions,
	}
	return notifyReq, nil
}

func (dn *DepositNotify) AfterNotify(businessId string, deposits []*database.Deposits, notify bool) error {
	for _, deposit := range deposits {
		if (deposit.Status == database.TxStatusUnSafe || deposit.Status == database.TxStatusUnSafeNotifyFail) && notify {
			deposit.Status = database.TxStatusUnSafeNotify
		} else if (deposit.Status == database.TxStatusUnSafe || deposit.Status == database.TxStatusUnSafeNotifyFail) && !notify {
			deposit.Status = database.TxStatusUnSafeNotifyFail
		} else if (deposit.Status == database.TxStatusSafe || deposit.Status == database.TxStatusSafeNotifyFail) && notify {
			deposit.Status = database.TxStatusSafeNotify
		} else if (deposit.Status == database.TxStatusSafe || deposit.Status == database.TxStatusSafeNotifyFail) && !notify {
			deposit.Status = database.TxStatusSafeNotifyFail
		} else if (deposit.Status == database.TxStatusFinalized || deposit.Status == database.TxStatusFinalizedNotifyFail) && notify {
			deposit.Status = database.TxStatusFinalizedNotify
		} else if (deposit.Status == database.TxStatusFinalized || deposit.Status == database.TxStatusFinalizedNotifyFail) && !notify {
			deposit.Status = database.TxStatusFinalizedNotifyFail
		} else if (deposit.Status == database.TxStatusFallback || deposit.Status == database.TxStatusFallbackNotifyFail) && notify {
			deposit.Status = database.TxStatusFallbackNotify
		} else if (deposit.Status == database.TxStatusFallback || deposit.Status == database.TxStatusFallbackNotifyFail) && !notify {
			deposit.Status = database.TxStatusFallbackNotifyFail
		} else {
			log.Error("no this status need handle")
			continue
		}
	}
	retryStrategy := &retry.ExponentialStrategy{Min: 1000, Max: 20_000, MaxJitter: 250}
	if _, err := retry.Do[interface{}](dn.resourceCtx, 10, retryStrategy, func() (interface{}, error) {
		if err := dn.db.Transaction(func(tx *database.DB) error {
			if len(deposits) > 0 {
				if err := tx.Deposits.UpdateDepositsStatusByTxHash(businessId, deposits); err != nil {
					return err
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
