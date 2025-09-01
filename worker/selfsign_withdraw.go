package worker

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
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

const (
	AccessToken string = "slim"
	WalletKey   string = "WALLET_KEY"
	RiskKey     string = "RISK_KEY"
)

type SelfSignWithdraw struct {
	cfg            *config.Config
	syncRpcClient  *syncclient.ChainAccountRpcClient
	signRpcClient  *signclient.SignMachineRpcClient
	db             *database.DB
	resourceCtx    context.Context
	resourceCancel context.CancelFunc
	tasks          tasks.Group
	ticker         *time.Ticker
}

func NewSelfSignWithdraw(cfg *config.Config, db *database.DB, syncClient *syncclient.ChainAccountRpcClient, signClient *signclient.SignMachineRpcClient, shutdown context.CancelCauseFunc) (*SelfSignWithdraw, error) {
	resCtx, resCancel := context.WithCancel(context.Background())
	return &SelfSignWithdraw{
		cfg:            cfg,
		syncRpcClient:  syncClient,
		signRpcClient:  signClient,
		db:             db,
		resourceCtx:    resCtx,
		resourceCancel: resCancel,
		tasks: tasks.Group{HandleCrit: func(err error) {
			shutdown(fmt.Errorf("cretical error in self sign withdraw: %w", err))
		}},
		ticker: time.NewTicker(cfg.ChainNode.WorkerInterval),
	}, nil
	return nil, nil
}

func (s *SelfSignWithdraw) Close() error {
	var result error
	s.resourceCancel()
	s.ticker.Stop()
	log.Info("stop withdraw")
	if err := s.tasks.Wait(); err != nil {
		result = errors.Join(result, fmt.Errorf("failed to await withdraw: %w", err))
		return result
	}
	log.Info("stop self sign withdraw successfully")
	return nil
}

func (s *SelfSignWithdraw) Start() error {
	log.Info("start withdraw...")
	s.tasks.Go(func() error {
		for {
			select {
			case <-s.ticker.C:
				businessList, err := s.db.Business.QueryBusinessList()
				if err != nil {
					log.Error("query business list error", "err", err)
					continue
				}
				for _, business := range businessList {
					log.Info("handle business ", "business", business)
					needWithdrawList, err := s.db.Withdraws.QueryNeedWithdrawsList(business.BusinessUid)
					if err != nil {
						log.Error("query need withdraws list error", "err", err)
						return err
					}
					// 查询热钱包地址
					hotWallet, err := s.db.Addresses.QueryHotWalletInfo(business.BusinessUid)
					if err != nil {
						log.Error("query hotwallet info error", "err", err)
						return err
					}
					var sentWithdrawList []*database.Withdraws
					for _, needWithdraw := range needWithdrawList {
						accountReq := &account.AccountRequest{
							Chain:           s.cfg.ChainNode.ChainName,
							Network:         s.cfg.ChainNode.NetWork,
							Address:         needWithdraw.FromAddress.String(),
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
							Address: needWithdraw.FromAddress.String(),
						}
						feeResponse, err := s.syncRpcClient.AccountRpClient.GetFee(context.Background(), accountFeeReq)
						if err != nil {
							return err
						}
						gasLimit, _ := s.getGasAndContractInfo(needWithdraw.TokenAddress.String())
						nonce, _ := strconv.Atoi(accountInfo.Sequence)

						dynamicFeeTx := Eip1559DynamicFeeTx{
							ChainId:              strconv.FormatUint(s.cfg.ChainNode.ChainId, 10),
							Nonce:                uint64(nonce),
							FromAddress:          needWithdraw.FromAddress.String(),
							ToAddress:            needWithdraw.ToAddress.String(),
							GasLimit:             gasLimit,
							MaxFeePerGas:         feeResponse.Eip1559Wallet.MaxFeePerGas,
							MaxPriorityFeePerGas: feeResponse.Eip1559Wallet.MaxPriorityFee,
							Amount:               needWithdraw.Amount.String(),
							ContractAddress:      needWithdraw.TokenAddress.String(),
						}

						data := json2.ToJSON(dynamicFeeTx)
						txBase64Body := base64.StdEncoding.EncodeToString(data)

						RiskKeyHash := crypto.Keccak256(append(data, []byte(RiskKey)...))
						RiskKeyHashStr := hexutils.BytesToHex(RiskKeyHash)

						WalletKeyHash := crypto.Keccak256(append(data, []byte(WalletKey)...))
						WalletKeyHashStr := hexutils.BytesToHex(WalletKeyHash)

						// 构建并签名
						signedTx, err := s.signRpcClient.BuildAndSignTransaction(hotWallet.PublicKey, WalletKeyHashStr, RiskKeyHashStr, txBase64Body)
						if err != nil {
							log.Error("build and sign transaction failed", "err", err)
							return err
						}
						// 上链
						sendTxBefore := &account.SendTxRequest{
							Chain:   s.cfg.ChainNode.ChainName,
							Network: s.cfg.ChainNode.NetWork,
							RawTx:   signedTx.SignedTx,
						}
						txReturn, err := s.syncRpcClient.AccountRpClient.SendTx(context.Background(), sendTxBefore)
						if err != nil {
							log.Error("send transaction failed", "err", err)
							return err
						}
						log.Info("send transaction successfully", "txReturn", txReturn)
						sentWithdrawList = append(sentWithdrawList, needWithdraw)
					}

					retryStrategy := &retry.ExponentialStrategy{Min: 1000, Max: 20_000, MaxJitter: 250}
					if _, err := retry.Do[interface{}](s.resourceCtx, 10, retryStrategy, func() (interface{}, error) {
						if err := s.db.Transaction(func(tx *database.DB) error {
							if len(sentWithdrawList) > 0 {
								log.Info("send withdraw list", "totalTx", len(sentWithdrawList))
								if err := tx.Withdraws.UpdateWithdrawStatusById(business.BusinessUid, database.TxStatusSent, sentWithdrawList); err != nil {
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
			case <-s.resourceCtx.Done():
				log.Info("stop self sign withdraw")
				return nil
			}
		}
	})
	return nil
}

func (sw *SelfSignWithdraw) getGasAndContractInfo(contractAddress string) (uint64, string) {
	if contractAddress == "0x00" {
		return types2.EthGasLimit, "0x00"
	}
	return types2.TokenGasLimit, contractAddress
}
