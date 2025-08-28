package http

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"sync/atomic"

	"github.com/huahaiwudi/multichain-sync-account/common/retry"
	"github.com/huahaiwudi/multichain-sync-account/common/tasks"
	"github.com/huahaiwudi/multichain-sync-account/database"
)

type InternalCallBack struct {
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

func NewInternalCallBack(db *database.DB, notifyClient map[string]*NotifyClient, businessIds []string, shutdown context.CancelCauseFunc) (*InternalCallBack, error) {
	resCtx, resCancel := context.WithCancel(context.Background())
	return &InternalCallBack{
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

func (ic *InternalCallBack) Start() error {
	ic.tasks.Go(func() error {
		for {
			select {
			case <-ic.ticker.C:
				for _, businessId := range ic.businessIds {
					log.Info("businessId", "businessId", businessId)
					needCallBackInternals, err := ic.db.Internals.QueryInternalsListByStatus(businessId, database.TxStatusWaitSign)
					if err != nil {
						log.Error("query callback internals list error", "businessId", businessId, "err", err)
						continue
					}

					var internalTxn []*CollectionTx
					for _, internal := range needCallBackInternals {
						ictx := &CollectionTx{
							BusinessId:            businessId,
							TransactionId:         internal.GUID.String(),
							UnsignedTxMessageHash: internal.TxUnsignHex,
						}
						internalTxn = append(internalTxn, ictx)
					}
					callbackInternalTxn := &InternalCollectionCallBack{
						CollectionTxn: internalTxn,
					}
					callback, err := ic.notifyClient[businessId].BusinessInternalNotify(callbackInternalTxn)
					if err != nil || !callback {
						log.Error("notify internal callback error", "businessId", businessId, "err", err)
						continue
					}

					retryStrategy := &retry.ExponentialStrategy{Min: 1000, Max: 20_000, MaxJitter: 250}
					if _, err := retry.Do[interface{}](ic.resourceCtx, 10, retryStrategy, func() (interface{}, error) {
						if err := ic.db.Transaction(func(tx *database.DB) error {
							if len(needCallBackInternals) > 0 {
								if err := tx.Internals.UpdateInternalStatusByTxHash(businessId, database.TxStatusInternalCallBack, needCallBackInternals); err != nil {
									log.Error("update internal status by tx hash fail", "err", err)
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
				}
			case <-ic.resourceCtx.Done():
				log.Info("stop internals in work")
				return nil
			}
		}
	})
	return nil
}

func (dn *InternalCallBack) Close() error {
	var result error
	dn.resourceCancel()
	dn.ticker.Stop()
	log.Info("stop internal callback......")
	if err := dn.tasks.Wait(); err != nil {
		result = errors.Join(result, fmt.Errorf("failed to await internal callback %w", err))
		return result
	}
	log.Info("stop internal callback success")
	return nil
}
