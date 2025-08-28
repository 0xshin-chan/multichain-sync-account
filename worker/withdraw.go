package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"

	"github.com/huahaiwudi/multichain-sync-account/common/retry"
	"github.com/huahaiwudi/multichain-sync-account/common/tasks"
	"github.com/huahaiwudi/multichain-sync-account/config"
	"github.com/huahaiwudi/multichain-sync-account/database"
	"github.com/huahaiwudi/multichain-sync-account/rpcclient/syncclient"
)

type Withdraw struct {
	rpcClient      *syncclient.ChainAccountRpcClient
	db             *database.DB
	ticker         *time.Ticker
	resourceCtx    context.Context
	resourceCancel context.CancelFunc
	tasks          tasks.Group
}

func NewWithdraw(cfg *config.Config, db *database.DB, rpcClient *syncclient.ChainAccountRpcClient, shutdown context.CancelCauseFunc) (*Withdraw, error) {
	resCtx, resCancel := context.WithCancel(context.Background())

	return &Withdraw{
		rpcClient:      rpcClient,
		db:             db,
		ticker:         time.NewTicker(5 * time.Second),
		resourceCtx:    resCtx,
		resourceCancel: resCancel,
		tasks: tasks.Group{HandleCrit: func(err error) {
			shutdown(fmt.Errorf("cretical error in deposit: %w", err))
		}},
	}, nil
}

func (w *Withdraw) Start() error {
	log.Info("start withdraw......")
	w.tasks.Go(func() error {
		for {
			select {
			case <-w.ticker.C:
				businessList, err := w.db.Business.QueryBusinessList()
				if err != nil {
					log.Error("query business list fail", "err", err)
					continue
				}

				// 遍历查询业务表中未提现的交易
				for _, businessId := range businessList {
					unSendTransactionList, err := w.db.Withdraws.UnSendWithdrawsList(businessId.BusinessUid)
					if err != nil {
						log.Error("Query un send withdraws list fail", "err", err)
						continue
					}
					if len(unSendTransactionList) == 0 {
						log.Error("Withdraw Start", "businessId", businessId, "unSendTransactionList", "is null")
						continue
					}

					var balanceList []*database.Balances

					for _, unSendTransaction := range unSendTransactionList {
						// 发送交易上链
						txHash, err := w.rpcClient.SendTx(unSendTransaction.TxSignHex)
						if err != nil {
							log.Error("send transaction fail", "err", err)
							continue
						} else {
							// 生成一条 余额的更新记录
							balanceItem := &database.Balances{
								TokenAddress: unSendTransaction.TokenAddress,
								Address:      unSendTransaction.FromAddress,
								LockBalance:  unSendTransaction.Amount,
							}
							balanceList = append(balanceList, balanceItem)

							// 修改交易状态
							unSendTransaction.TxHash = common.HexToHash(txHash)
							unSendTransaction.Status = database.TxStatusSent
						}
					}

					retryStrategy := &retry.ExponentialStrategy{Min: 1000, Max: 20_000, MaxJitter: 250}
					if _, err := retry.Do[interface{}](w.resourceCtx, 10, retryStrategy, func() (interface{}, error) {
						if err := w.db.Transaction(func(tx *database.DB) error {
							if len(balanceList) > 0 {
								log.Info("Update address balance", "totalTx", len(balanceList))
								// 更新余额
								if err := tx.Balances.UpdateBalanceListByTwoAddress(businessId.BusinessUid, balanceList); err != nil {
									log.Error("Update address balance fail", "err", err)
									return err
								}
							}
							if len(unSendTransactionList) > 0 {
								err = w.db.Withdraws.UpdateWithdrawListById(businessId.BusinessUid, unSendTransactionList)
								if err != nil {
									log.Error("update withdraw status fail", "err", err)
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

			case <-w.resourceCtx.Done():
				log.Info("stop withdraw in worker")
				return nil
			}
		}
	})
	return nil
}

func (w *Withdraw) Close() error {
	var result error
	w.resourceCancel()
	if err := w.tasks.Wait(); err != nil {
		result = errors.Join(result, fmt.Errorf("failed to wait tasks: %w", err))
	}
	return result
}
