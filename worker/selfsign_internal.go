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
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/google/uuid"
	"github.com/status-im/keycard-go/hexutils"

	"github.com/0xshin-chan/multichain-sync-account/common/json2"
	"github.com/0xshin-chan/multichain-sync-account/common/retry"
	"github.com/0xshin-chan/multichain-sync-account/common/tasks"
	"github.com/0xshin-chan/multichain-sync-account/config"
	"github.com/0xshin-chan/multichain-sync-account/database"
	"github.com/0xshin-chan/multichain-sync-account/rpcclient/signclient"
	"github.com/0xshin-chan/multichain-sync-account/rpcclient/syncclient"
	"github.com/0xshin-chan/multichain-sync-account/rpcclient/syncclient/account"
	types2 "github.com/0xshin-chan/multichain-sync-account/services/rpc"
)

type SelfSignInternal struct {
	cfg            *config.Config
	syncRpcClient  *syncclient.ChainAccountRpcClient
	signRpcClient  *signclient.SignMachineRpcClient
	db             *database.DB
	resourceCtx    context.Context
	resourceCancel context.CancelFunc
	tasks          tasks.Group
	ticker         *time.Ticker
}

func NewSelfSignInternal(cfg *config.Config, db *database.DB, syncRpcClient *syncclient.ChainAccountRpcClient, signRpcClient *signclient.SignMachineRpcClient, shutdown context.CancelCauseFunc) (*SelfSignInternal, error) {
	resCtx, resCancel := context.WithCancel(context.Background())
	return &SelfSignInternal{
		cfg:            cfg,
		syncRpcClient:  syncRpcClient,
		signRpcClient:  signRpcClient,
		db:             db,
		resourceCtx:    resCtx,
		resourceCancel: resCancel,
		tasks: tasks.Group{HandleCrit: func(err error) {
			shutdown(fmt.Errorf("critical error in self sign internal: %w", err))
		}},
		ticker: time.NewTicker(cfg.ChainNode.WorkerInterval),
	}, nil
}

func (s *SelfSignInternal) Close() error {
	var result error
	s.resourceCancel()
	s.ticker.Stop()
	log.Info("stop internal...")
	if err := s.tasks.Wait(); err != nil {
		result = errors.Join(result, fmt.Errorf("failed to await internal %w", err))
		return result
	}
	log.Info("stop self sign internal successfully")
	return nil
}

func (s *SelfSignInternal) Start() error {
	log.Info("start self sign internal...")
	s.tasks.Go(func() error {
		log.Info("handle process collection...")
		return s.processCollection()
	})

	s.tasks.Go(func() error {
		log.Info("handle process to cold wallet...")
		return s.processHotToCold()
	})
	return nil
}

func (s *SelfSignInternal) processCollection() error {
	for {
		select {
		case <-s.ticker.C:
			businessList, err := s.db.Business.QueryBusinessList()
			if err != nil {
				log.Error("failed to query business list", "err", err)
				continue
			}
			for _, business := range businessList {
				var internalTxn []database.Internals
				var collectionDepositList []*database.Deposits
				log.Info("handle collection business ", "business", business)
				tokenInfoList, err := s.db.Tokens.TokensInfoByBusiness(business.BusinessUid)
				if err != nil {
					log.Error("failed to query token info by business", "business", business.BusinessUid, "err", err)
					continue
				}
				for _, tokenInfo := range tokenInfoList {
					needCollectionDepositList, err := s.db.Deposits.QueryNeedCollectionDeposits(
						business.BusinessUid,
						tokenInfo.TokenAddress.String(),
						tokenInfo.CollectAmount,
					)
					if err != nil {
						log.Error("failed to query collection deposits", "business", business.BusinessUid, "err", err)
						continue
					}
					for _, needCollectionDeposit := range needCollectionDepositList {
						fromWallet, err := s.db.Addresses.QueryAddressesByToAddress(business.BusinessUid, &needCollectionDeposit.FromAddress)
						if err != nil {
							log.Error("query user wallet fail", "err", err)
							return err
						}

						// 构建 dynamicFee
						accountReq := &account.AccountRequest{
							Chain:           s.cfg.ChainNode.ChainName,
							Network:         s.cfg.ChainNode.NetWork,
							Address:         needCollectionDeposit.FromAddress.String(),
							ContractAddress: "0x00",
						}
						accountInfo, err := s.syncRpcClient.AccountRpClient.GetAccount(context.Background(), accountReq)
						if err != nil {
							return err
						}
						accountFeeReq := &account.FeeRequest{
							Chain:   s.cfg.ChainNode.ChainName,
							Network: s.cfg.ChainNode.NetWork,
							RawTx:   "",
							Address: needCollectionDeposit.FromAddress.String(),
						}
						feeResponse, err := s.syncRpcClient.AccountRpClient.GetFee(context.Background(), accountFeeReq)
						if err != nil {
							return err
						}
						gasLimit, _ := s.getGasAndContractInfo(needCollectionDeposit.TokenAddress.String())
						nonce, _ := strconv.Atoi(accountInfo.Sequence)
						dynamicFeeTxReq := types2.Eip1559DynamicFeeTx{
							ChainId:              strconv.FormatUint(s.cfg.ChainNode.ChainId, 10),
							Nonce:                uint64(nonce),
							FromAddress:          needCollectionDeposit.FromAddress.String(),
							ToAddress:            needCollectionDeposit.ToAddress.String(),
							GasLimit:             gasLimit,
							MaxFeePerGas:         feeResponse.Eip1559Wallet.MaxFeePerGas,
							MaxPriorityFeePerGas: feeResponse.Eip1559Wallet.MaxPriorityFee,
							Amount:               needCollectionDeposit.Amount.String(),
							ContractAddress:      needCollectionDeposit.TokenAddress.String(),
						}

						data := json2.ToJSON(dynamicFeeTxReq)

						txBase64Body := base64.StdEncoding.EncodeToString(data)

						RiskKeyHash := crypto.Keccak256(append(data, []byte(RiskKey)...))
						RistKeyHashStr := hexutils.BytesToHex(RiskKeyHash)

						WalletKeyHash := crypto.Keccak256(append(data, []byte(WalletKey)...))
						WalletKeyHashStr := hexutils.BytesToHex(WalletKeyHash)

						// 构建可发送上链的rawTx：（签名 + 交易体）经过 rlp 编码 得到 rawTx
						signedTx, err := s.signRpcClient.BuildAndSignTransaction(fromWallet.PublicKey, WalletKeyHashStr, RistKeyHashStr, txBase64Body)
						if err != nil {
							log.Error("build and sign transaction fail", "err", err)
							return err
						}
						sendTxBefore := &account.SendTxRequest{
							Chain:   s.cfg.ChainNode.ChainName,
							Network: s.cfg.ChainNode.NetWork,
							RawTx:   signedTx.SignedTx,
						}

						// todo：发交易上链, 如果有多笔交易需要归集，自己维护 nonce， 如果是同一个地址，可以签名之后跌倒 queue 里面发送出去， 另一个任务处理receipt
						// todo：系统里面热钱包地址很多，如果这段时间有 n 个交易，需要讲这些交易均分给热钱包，然后再签名交易。但更优的方法是合约
						txReturn, err := s.syncRpcClient.AccountRpClient.SendTx(context.Background(), sendTxBefore)
						if err != nil {
							return err
						}
						log.Info("send tx success", "txReturn", txReturn.TxHash)

						// 构建表 internal
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
							TxUnsignHex:          signedTx.TxMessageHash,
							TxSignHex:            signedTx.SignedTx,
						}
						needCollectionDeposit.Status = database.TxStatusSent
						needCollectionDeposit.IsCollection = true
						collectionDepositList = append(collectionDepositList, needCollectionDeposit)
						internalTxn = append(internalTxn, internalItem)
					}
				}

				retryStrategy := &retry.ExponentialStrategy{Min: 1000, Max: 20_000, MaxJitter: 250}
				if _, err := retry.Do[interface{}](s.resourceCtx, 10, retryStrategy, func() (interface{}, error) {
					if err := s.db.Transaction(func(tx *database.DB) error {
						if len(internalTxn) > 0 {
							log.Info("need collection deposit into internal", "totalTx", len(internalTxn))
							if err := tx.Internals.StoreInternals(business.BusinessUid, internalTxn); err != nil {
								log.Error("store internal transaction fail", "err", err)
								return err
							}
						}
						if len(collectionDepositList) > 0 {
							log.Info("deposit collection list", "totalTx", len(collectionDepositList))
							if err := tx.Deposits.UpdateDepositsStatusByTxHash(business.BusinessUid, collectionDepositList); err != nil {
								log.Error("store internal transaction fail", "err", err)
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
		case <-s.resourceCtx.Done():
			log.Info("self sign internal process stopped")
			return nil
		}
	}
}

func (ssi *SelfSignInternal) processHotToCold() error {
	for {
		select {
		case <-ssi.ticker.C:
			businessList, err := ssi.db.Business.QueryBusinessList()
			if err != nil {
				log.Error("query business list fail", "err", err)
				continue
			}
			for _, businessId := range businessList {

				var internalTxn []database.Internals
				var balances []*database.TokenBalance

				tokenInfoList, err := ssi.db.Tokens.TokensInfoByBusiness(businessId.BusinessUid)
				if err != nil {
					log.Error("query token info fail", "err", err)
					return err
				}
				coldWallet, err := ssi.db.Addresses.QueryColdWalletInfo(businessId.BusinessUid)
				if err != nil {
					log.Error("Query Cold Wallet Info Fail", "err", err)
					return err
				}
				for _, tokenInfo := range tokenInfoList {
					needTransferToColdList, err := ssi.db.Balances.NeedToColdWallet(businessId.BusinessUid, tokenInfo.TokenAddress, tokenInfo.ColdAmount)
					if err != nil {
						log.Error("query need cold wallet", "err", err)
						return err
					}
					for _, needTransfer := range needTransferToColdList {
						walletPuk, err := ssi.db.Addresses.QueryAddressesByToAddress(businessId.BusinessUid, &needTransfer.Address)
						if err != nil {
							log.Error("QueryAddressesByToAddress fail", "err", err)
							return err
						}

						accountReq := &account.AccountRequest{
							Chain:           ssi.cfg.ChainNode.ChainName,
							Network:         ssi.cfg.ChainNode.NetWork,
							Address:         walletPuk.Address.String(),
							ContractAddress: "0x00",
						}
						accountInfo, err := ssi.syncRpcClient.AccountRpClient.GetAccount(context.Background(), accountReq)
						if err != nil {
							log.Error("SelfInternal GetAccount Fail", "err", err)
							return err
						}

						accountFeeReq := &account.FeeRequest{
							Chain:   ssi.cfg.ChainNode.ChainName,
							Network: ssi.cfg.ChainNode.NetWork,
							RawTx:   "",
							Address: walletPuk.Address.String(),
						}
						feeResponse, err := ssi.syncRpcClient.AccountRpClient.GetFee(context.Background(), accountFeeReq)
						if err != nil {
							log.Error("get fee fail", "err", err)
							return err
						}
						gasLimit, _ := ssi.getGasAndContractInfo(needTransfer.TokenAddress.String())
						nonce, _ := strconv.Atoi(accountInfo.Sequence)

						toColdAmount := new(big.Int).Sub(needTransfer.Balance, tokenInfo.ColdAmount)

						dynamicFeeTx := Eip1559DynamicFeeTx{
							ChainId:              strconv.FormatUint(ssi.cfg.ChainNode.ChainId, 10),
							Nonce:                uint64(nonce),
							FromAddress:          walletPuk.Address.String(),
							ToAddress:            coldWallet.Address.String(),
							GasLimit:             gasLimit,
							MaxFeePerGas:         feeResponse.Eip1559Wallet.MaxFeePerGas,
							MaxPriorityFeePerGas: feeResponse.Eip1559Wallet.MaxPriorityFee,
							Amount:               toColdAmount.String(),
							ContractAddress:      needTransfer.TokenAddress.String(),
						}

						data := json2.ToJSON(dynamicFeeTx)

						txBase64Body := base64.StdEncoding.EncodeToString(data)

						RiskKeyHash := crypto.Keccak256(append(data, []byte(RiskKey)...))
						RiskKeyHashStr := hexutils.BytesToHex(RiskKeyHash)

						WalletKeyHash := crypto.Keccak256(append(data, []byte(WalletKey)...))
						WalletKeyHashStr := hexutils.BytesToHex(WalletKeyHash)
						signedTx, err := ssi.signRpcClient.BuildAndSignTransaction(walletPuk.PublicKey, WalletKeyHashStr, RiskKeyHashStr, txBase64Body)
						if err != nil {
							log.Error("build and sign transaction fail", "err", err)
							return err
						}
						sendTxBefore := &account.SendTxRequest{
							Chain:   ssi.cfg.ChainNode.ChainName,
							Network: ssi.cfg.ChainNode.NetWork,
							RawTx:   signedTx.SignedTx,
						}
						txReturn, err := ssi.syncRpcClient.AccountRpClient.SendTx(context.Background(), sendTxBefore)
						if err != nil {
							return err
						}
						log.Info("send tx success", "txReturn", txReturn.TxHash)

						internalItem := database.Internals{
							GUID:                 uuid.New(),
							Timestamp:            uint64(time.Now().Unix()),
							Status:               database.TxStatusWaitSign,
							BlockHash:            common.Hash{},
							BlockNumber:          big.NewInt(1),
							TxHash:               common.Hash{},
							TxType:               database.TxTypeCollection,
							FromAddress:          walletPuk.Address,
							ToAddress:            coldWallet.Address,
							Amount:               toColdAmount,
							GasLimit:             gasLimit,
							MaxFeePerGas:         feeResponse.Eip1559Wallet.MaxFeePerGas,
							MaxPriorityFeePerGas: feeResponse.Eip1559Wallet.MaxPriorityFee,
							TokenType:            types2.DetermineTokenType(needTransfer.TokenAddress.String()),
							TokenAddress:         needTransfer.TokenAddress,
							TokenId:              "0x00",
							TokenMeta:            "0x00",
							TxUnsignHex:          signedTx.TxMessageHash,
							TxSignHex:            signedTx.SignedTx,
						}
						internalTxn = append(internalTxn, internalItem)
						balances = append(
							balances,
							&database.TokenBalance{
								FromAddress:  walletPuk.Address,
								ToAddress:    coldWallet.Address,
								TokenAddress: needTransfer.TokenAddress,
								Balance:      toColdAmount,
								TxType:       database.TxTypeCollection,
							},
						)
					}
				}
				retryStrategy := &retry.ExponentialStrategy{Min: 1000, Max: 20_000, MaxJitter: 250}
				if _, err := retry.Do[interface{}](ssi.resourceCtx, 10, retryStrategy, func() (interface{}, error) {
					if err := ssi.db.Transaction(func(tx *database.DB) error {
						if len(balances) > 0 {
							log.Info("Handle balances success", "totalTx", len(balances))
							if err := tx.Balances.UpdateOrCreate(businessId.BusinessUid, balances); err != nil {
								return err
							}
						}

						if len(internalTxn) > 0 {
							log.Info("Handle self internal success", "totalTx", len(internalTxn))
							if err := tx.Internals.StoreInternals(businessId.BusinessUid, internalTxn); err != nil {
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

		case <-ssi.resourceCtx.Done():
			log.Info("stop self sign internal in worker")
			return nil
		}
	}
}

func (ssi *SelfSignInternal) getGasAndContractInfo(contractAddress string) (uint64, string) {
	if contractAddress == "0x00" {
		return types2.EthGasLimit, "0x00"
	}
	return types2.TokenGasLimit, contractAddress
}
