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

type WithdrawNotify struct {
	db             *database.DB
	businessIds    []string
	notifyClient   map[string]*NotifyClient
	resourceCtx    context.Context
	resourceCancel context.CancelFunc
	tasks          tasks.Group
	ticker         *time.Ticker
	shutdown       context.CancelCauseFunc
	stopped        atomic.Bool
}

func NewWithdrawNotify(db *database.DB, notifyClient map[string]*NotifyClient, businessIds []string, shutdown context.CancelCauseFunc) (*WithdrawNotify, error) {
	resCtx, resCancel := context.WithCancel(context.Background())
	return &WithdrawNotify{
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

func (wn *WithdrawNotify) Start() error {
	log.Info("start internals......")
	wn.tasks.Go(func() error {
		for {
			select {
			case <-wn.ticker.C:
				for _, businessId := range wn.businessIds {
					log.Info("txn and businessId", "businessId", businessId)
					needNotifyWithdraws, err := wn.db.Withdraws.QueryNotifyWithdraws(businessId)
					if err != nil {
						log.Error("Query notify deposits fail", "err", err)
						return err
					}

					notifyRequest, err := wn.BuildNotifyWithdraw(needNotifyWithdraws)

					notify, err := wn.notifyClient[businessId].BusinessNotify(notifyRequest)
					if err != nil {
						log.Error("notify business platform fail", "err")
						return err
					}

					err = wn.AfterNotify(businessId, needNotifyWithdraws, notify)
					if err != nil {
						log.Error("handle notifier transactions fail", "err", err)
						return err
					}

				}

			case <-wn.resourceCtx.Done():
				log.Info("stop internals in worker")
				return nil
			}
		}
	})
	return nil
}

func (wn *WithdrawNotify) Close() error {
	var result error
	wn.resourceCancel()
	wn.ticker.Stop()
	log.Info("stop withdraw......")
	if err := wn.tasks.Wait(); err != nil {
		result = errors.Join(result, fmt.Errorf("failed to await withdraw %w", err))
		return result
	}
	log.Info("stop withdraw success")
	return nil
}

func (wn *WithdrawNotify) BuildNotifyWithdraw(withdraws []*database.Withdraws) (*NotifyRequest, error) {
	var notifyTransactions []*Transaction
	for _, withdraw := range withdraws {
		var txStatus database.TxStatus
		if withdraw.Status == database.TxStatusSent || withdraw.Status == database.TxStatusSentNotifyFail {
			txStatus = database.TxStatusSent
		}
		if withdraw.Status == database.TxStatusWithdrawed || withdraw.Status == database.TxStatusWithdrawedNotifyFail {
			txStatus = database.TxStatusWithdrawed
		}
		if withdraw.Status == database.TxStatusFallback || withdraw.Status == database.TxStatusFallbackNotifyFail {
			txStatus = database.TxStatusFallback
		}
		txItem := &Transaction{
			BlockHash:    withdraw.BlockHash.String(),
			BlockNumber:  withdraw.BlockNumber.Uint64(),
			Hash:         withdraw.TxHash.String(),
			FromAddress:  withdraw.FromAddress.String(),
			ToAddress:    withdraw.ToAddress.String(),
			Value:        withdraw.Amount.String(),
			Fee:          withdraw.MaxFeePerGas,
			TxType:       withdraw.TxType,
			TxStatus:     txStatus,
			Confirms:     0,
			TokenAddress: withdraw.TokenAddress.String(),
			TokenId:      withdraw.TokenId,
			TokenMeta:    withdraw.TokenMeta,
		}
		notifyTransactions = append(notifyTransactions, txItem)
	}
	notifyReq := &NotifyRequest{
		Txn: notifyTransactions,
	}
	return notifyReq, nil
}

func (wn *WithdrawNotify) AfterNotify(businessId string, withdraws []*database.Withdraws, notify bool) error {
	for _, withdraw := range withdraws {
		if (withdraw.Status == database.TxStatusSent || withdraw.Status == database.TxStatusSentNotifyFail) && notify {
			withdraw.Status = database.TxStatusSentNotify
		} else if (withdraw.Status == database.TxStatusSent || withdraw.Status == database.TxStatusSentNotifyFail) && !notify {
			withdraw.Status = database.TxStatusSentNotifyFail
		} else if (withdraw.Status == database.TxStatusWithdrawed || withdraw.Status == database.TxStatusWithdrawedNotifyFail) && notify {
			withdraw.Status = database.TxStatusWithdrawedNotify
		} else if (withdraw.Status == database.TxStatusWithdrawed || withdraw.Status == database.TxStatusWithdrawedNotifyFail) && !notify {
			withdraw.Status = database.TxStatusWithdrawedNotifyFail
		} else {
			log.Warn("don't support tx status handle")
			continue
		}
	}
	retryStrategy := &retry.ExponentialStrategy{Min: 1000, Max: 20_000, MaxJitter: 250}
	if _, err := retry.Do[interface{}](wn.resourceCtx, 10, retryStrategy, func() (interface{}, error) {
		if err := wn.db.Transaction(func(tx *database.DB) error {
			if len(withdraws) > 0 {
				if err := tx.Withdraws.UpdateWithdrawStatusByTxHash(businessId, withdraws); err != nil {
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
