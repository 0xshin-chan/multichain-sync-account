package worker

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/google/uuid"

	"github.com/0xshin-chan/multichain-sync-account/common/json2"
	"github.com/0xshin-chan/multichain-sync-account/common/retry"
	"github.com/0xshin-chan/multichain-sync-account/common/tasks"
	"github.com/0xshin-chan/multichain-sync-account/config"
	"github.com/0xshin-chan/multichain-sync-account/database"
	"github.com/0xshin-chan/multichain-sync-account/rpcclient/syncclient"
	"github.com/0xshin-chan/multichain-sync-account/rpcclient/syncclient/account"
	types2 "github.com/0xshin-chan/multichain-sync-account/services/rpc"
)

type Internal struct {
	cfg            *config.Config
	rpcClient      *syncclient.ChainAccountRpcClient
	db             *database.DB
	ticker         *time.Ticker
	resourceCtx    context.Context
	resourceCancel context.CancelFunc
	tasks          tasks.Group
}

func NewInternal(cfg *config.Config, db *database.DB, rpcClient *syncclient.ChainAccountRpcClient, shutdown context.CancelCauseFunc) (*Internal, error) {
	resCtx, resCancel := context.WithCancel(context.Background())

	return &Internal{
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

func (i *Internal) Close() error {
	var result error
	i.resourceCancel()
	if err := i.tasks.Wait(); err != nil {
		result = errors.Join(result, fmt.Errorf("failed to wait tasks: %w", err))
	}
	return result
}

func (i *Internal) Start() error {
	log.Info("start internals")
	i.tasks.Go(func() error {
		log.Info("clarify internal transaction")
		err := i.ProcessBuildInternalTransaction()
		if err != nil {
			log.Error("ProcessBuildInternalTransaction fail")
			return err
		}
		return nil
	})

	i.tasks.Go(func() error {
		log.Info("start process unsent internal transaction")
		err := i.ProcessUnSendInternal()
		if err != nil {
			log.Error("ProcessUnSendInternal fail")
			return err
		}
		return nil
	})
	return nil
}

func (i *Internal) ProcessBuildInternalTransaction() error {
	for {
		select {
		case <-i.ticker.C:
			businessList, err := i.db.Business.QueryBusinessList()
			if err != nil {
				log.Error("QueryBusinessList fail")
				continue
			}
			for _, businessId := range businessList {
				var internalTxn []database.Internals
				var collectionDepositList []*database.Deposits

				// 查询没有归集的 token， 然后进行构建
				tokenInfoList, err := i.db.Tokens.TokensInfoByBusiness(businessId.BusinessUid)
				if err != nil {
					log.Error("QueryTokensInfoByBusiness fail")
					return err
				}
				for _, tokenInfo := range tokenInfoList {
					needCollectionDepositList, err := i.db.Deposits.QueryNeedCollectionDeposits(
						businessId.BusinessUid,
						tokenInfo.TokenAddress.String(),
						tokenInfo.CollectAmount,
					)
					if err != nil {
						log.Error("QueryNeedCollectionDeposits fail", "err", err)
						return err
					}

					for _, needCollectionDeposit := range needCollectionDepositList {
						accountReq := &account.AccountRequest{
							Chain:           i.cfg.ChainNode.ChainName,
							Network:         i.cfg.ChainNode.NetWork,
							Address:         needCollectionDeposit.FromAddress.String(),
							ContractAddress: "0x00",
						}
						accountInfo, err := i.rpcClient.AccountRpClient.GetAccount(context.Background(), accountReq)
						if err != nil {
							log.Error("GetAccountInfo fail", "err", err)
							return err
						}

						accountFeeReq := &account.FeeRequest{
							Chain:   i.cfg.ChainNode.ChainName,
							Network: i.cfg.ChainNode.NetWork,
							RawTx:   "",
							Address: needCollectionDeposit.ToAddress.String(),
						}
						feeResponse, err := i.rpcClient.AccountRpClient.GetFee(context.Background(), accountFeeReq)
						if err != nil {
							log.Error("GetFeeInfo fail", "err", err)
							return err
						}
						gasLimit, _ := i.getGasAndContractInfo(needCollectionDeposit.TokenAddress.String())
						nonce, _ := strconv.Atoi(accountInfo.Sequence)

						dynamicFeeTxReq := types2.Eip1559DynamicFeeTx{
							ChainId:              strconv.FormatUint(i.cfg.ChainNode.ChainId, 10),
							Nonce:                uint64(nonce),
							FromAddress:          needCollectionDeposit.FromAddress.String(),
							ToAddress:            needCollectionDeposit.ToAddress.String(),
							GasLimit:             gasLimit,
							MaxFeePerGas:         feeResponse.Eip1559Wallet.MaxFeePerGas,
							MaxPriorityFeePerGas: feeResponse.Eip1559Wallet.MaxPriorityFee,
							Amount:               needCollectionDeposit.Amount.String(),
							ContractAddress:      needCollectionDeposit.ToAddress.String(),
						}

						// 把 dynamicFeeReq 封装为 json 转为 base64 方便 rpc 传输
						data := json2.ToJSON(dynamicFeeTxReq)
						unSignTx := &account.UnSignTransactionRequest{
							Chain:    i.cfg.ChainNode.ChainName,
							Network:  i.cfg.ChainNode.NetWork,
							Base64Tx: base64.StdEncoding.EncodeToString(data),
						}
						// 构建 32 位 交易hash
						retTxMessageHash, err := i.rpcClient.AccountRpClient.BuildUnSignTransaction(context.Background(), unSignTx)
						if err != nil {
							log.Error("BuildUnSignTransaction fail", "err", err)
							return err
						}
						internalItem := database.Internals{
							GUID:                 uuid.New(),
							Timestamp:            uint64(time.Now().Unix()),
							Status:               database.TxStatusWaitSign,
							BlockHash:            common.Hash{},
							BlockNumber:          big.NewInt(1),
							TxHash:               common.Hash{},
							TxType:               database.TxTypeCollection,
							FromAddress:          needCollectionDeposit.FromAddress,
							ToAddress:            needCollectionDeposit.ToAddress,
							Amount:               needCollectionDeposit.Amount,
							GasLimit:             gasLimit,
							MaxFeePerGas:         feeResponse.Eip1559Wallet.MaxFeePerGas,
							MaxPriorityFeePerGas: feeResponse.Eip1559Wallet.MaxPriorityFee,
							TokenType:            types2.DetermineTokenType(needCollectionDeposit.TokenAddress.String()),
							TokenAddress:         needCollectionDeposit.TokenAddress,
							TokenId:              "0x00",
							TokenMeta:            "0x00",
							TxUnsignHex:          retTxMessageHash.UnSignTx,
							TxSignHex:            "",
						}
						needCollectionDeposit.IsCollection = true
						collectionDepositList = append(collectionDepositList, needCollectionDeposit)
						internalTxn = append(internalTxn, internalItem)
					}
				}

				retryStrategy := &retry.ExponentialStrategy{Min: 1000, Max: 20_000, MaxJitter: 250}
				if _, err := retry.Do[interface{}](i.resourceCtx, 10, retryStrategy, func() (interface{}, error) {
					if err := i.db.Transaction(func(tx *database.DB) error {
						if len(internalTxn) > 0 {
							log.Info("need collection deposit into internal", "totalTx", len(internalTxn))
							if err := tx.Internals.StoreInternals(businessId.BusinessUid, internalTxn); err != nil {
								log.Error("store internals transaction fail", "err", err)
								return err
							}
						}

						if len(collectionDepositList) > 0 {
							log.Info("deposit collection list", "totalTx", len(collectionDepositList))
							if err := tx.Deposits.UpdateDepositsStatusByTxHash(businessId.BusinessUid, collectionDepositList); err != nil {
								log.Error("update deposits status by tx fail", "err", err)
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
		case <-i.resourceCtx.Done():
			log.Info("stop process build internal in work")
			return nil
		}
	}
}

func (i *Internal) ProcessUnSendInternal() error {
	for {
		select {
		case <-i.ticker.C:
			log.Info("collection and hot to cold")
			businessList, err := i.db.Business.QueryBusinessList()
			if err != nil {
				log.Error("query business fail", "err", err)
				return err
			}

			for _, businessId := range businessList {
				unSendTransactionList, err := i.db.Internals.QueryInternalsListByStatus(businessId.BusinessUid, database.TxStatusUnSent)
				if err != nil {
					log.Error("query internals fail", "err", err)
					continue
				}
				if len(unSendTransactionList) == 0 {
					log.Error("unSendTransaction is null")
					continue
				}

				var balanceList []*database.Balances
				for _, unSendTransactionTx := range unSendTransactionList {
					txHash, err := i.rpcClient.SendTx(unSendTransactionTx.TxSignHex)
					if err != nil {
						log.Error("send tx fail", "err", err)
						continue
					} else {
						balanceItem := &database.Balances{
							TokenAddress: unSendTransactionTx.TokenAddress,
							Address:      unSendTransactionTx.FromAddress,
							LockBalance:  unSendTransactionTx.Amount,
						}
						balanceList = append(balanceList, balanceItem)

						unSendTransactionTx.TxHash = common.HexToHash(txHash)
						unSendTransactionTx.Status = database.TxStatusSent
					}
				}

				retryStrategy := &retry.ExponentialStrategy{Min: 1000, Max: 20_000, MaxJitter: 250}
				if _, err := retry.Do[interface{}](i.resourceCtx, 10, retryStrategy, func() (interface{}, error) {
					if err := i.db.Transaction(func(tx *database.DB) error {
						if len(balanceList) > 0 {
							log.Info("update address balance", "totalTx", len(balanceList))
							if err := tx.Balances.UpdateBalanceListByTwoAddress(businessId.BusinessUid, balanceList); err != nil {
								log.Error("update address balance fail", "err", err)
								return err
							}
						}

						if len(unSendTransactionList) > 0 {
							if err := i.db.Internals.UpdateInternalListById(businessId.BusinessUid, unSendTransactionList); err != nil {
								log.Error("update internals fail", "err", err)
								return err
							}
						}
						return nil
					}); err != nil {
						log.Error("transactions fail", "err", err)
						return nil, err
					}
					return nil, nil
				}); err != nil {
					log.Error("unable to persist batch", "err", err)
					return err
				}
			}
		case <-i.resourceCtx.Done():
			log.Info("stop process un send internal")
			return nil
		}
	}
}

func (i *Internal) getGasAndContractInfo(contractAddress string) (uint64, string) {
	if contractAddress == "0x00" {
		return types2.EthGasLimit, "0x00"
	}
	return types2.EthGasLimit, contractAddress
}
