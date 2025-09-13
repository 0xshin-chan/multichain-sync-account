package worker

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/google/uuid"

	"github.com/0xshin-chan/multichain-sync-account/common/retry"
	"github.com/0xshin-chan/multichain-sync-account/common/tasks"
	"github.com/0xshin-chan/multichain-sync-account/config"
	"github.com/0xshin-chan/multichain-sync-account/database"
	"github.com/0xshin-chan/multichain-sync-account/rpcclient/syncclient"
	"github.com/0xshin-chan/multichain-sync-account/rpcclient/syncclient/account"
)

type Deposit struct {
	BaseSynchronizer
	preConfirms    uint8
	confirms       uint8
	latestHeader   syncclient.BlockHeader
	resourceCtx    context.Context
	resourceCancel context.CancelFunc
	tasks          tasks.Group
}

// NewDeposit Deposit Deposit 是一个链上同步器，专门处理充值、提现、内部转账。
// 核心逻辑：
// 从链上拉取区块 → 转换成交易批次 → 遍历业务 → 分类处理交易 → 写入数据库。
// 用了 重试机制 和 事务，确保数据一致性。
// 把不同交易类型分别存储到对应的表（Deposits / Withdraws / Internals / Transactions / Balances）
func NewDeposit(cfg *config.Config, db *database.DB, rpcClient *syncclient.ChainAccountRpcClient, shutdown context.CancelCauseFunc) (*Deposit, error) {
	// 优先取数据库最新的区块
	//如果数据库没有，就用配置里设置的 StartingHeight
	//如果也没有，就从链上最新区块开始
	dbLatestBlockHeader, err := db.Blocks.LatestBlocks()
	if err != nil {
		log.Error("get latest block fail", "err", err)
		return nil, err
	}

	var fromHeader *syncclient.BlockHeader
	if dbLatestBlockHeader != nil {
		fromHeader = dbLatestBlockHeader
	} else if cfg.ChainNode.StartingHeight > 0 {
		chainBlockHeader, err := rpcClient.GetBlockHeader(big.NewInt(int64(cfg.ChainNode.StartingHeight)))
		if err != nil {
			log.Error("get block header by number fail", "err", err)
			return nil, err
		}
		fromHeader = chainBlockHeader
	} else {
		chainLatestBlockHeader, err := rpcClient.GetBlockHeader(nil)
		if err != nil {
			log.Error("get latest block header fail", "err", err)
		}
		fromHeader = chainLatestBlockHeader
	}

	// 业务交易通道：BaseSynchronizer 会往这里推送批次交易
	businessTxChannel := make(chan map[string]*TransactionChannel)

	// 初始化同步器
	baseSyncer := BaseSynchronizer{
		loopInterval:     cfg.ChainNode.SynchronizerInterval,
		headerBufferSize: cfg.ChainNode.BlocksStep,
		businessChannels: businessTxChannel,
		rpcClient:        rpcClient,
		// 用于在 synchronizer 中获取扫链的区块
		blockBatch:          syncclient.NewBatchBlock(rpcClient, fromHeader, big.NewInt(int64(cfg.ChainNode.Confirmations))),
		database:            db,
		isFallBack:          false,
		preConfirms:         uint8(cfg.ChainNode.PreConfirmations),
		confirms:            uint8(cfg.ChainNode.Confirmations),
		fallbackBlockHeader: nil,
	}

	resCtx, resCancel := context.WithCancel(context.Background())

	return &Deposit{
		BaseSynchronizer: baseSyncer,
		resourceCtx:      resCtx,
		resourceCancel:   resCancel,
		tasks: tasks.Group{HandleCrit: func(err error) {
			// 如果任务中出现严重错误，调用 shutdown
			shutdown(fmt.Errorf("cretical error in deposit: %w", err))
		}},
	}, nil
}

// Start 开一个 goroutine 消费 businessChannels（即区块里解析出的交易集合），并调用 handleBatch 处理
func (d *Deposit) Start() error {
	log.Info("start deposit...")
	if err := d.BaseSynchronizer.Start(); err != nil {
		return fmt.Errorf("failed to start internal Synchronizer: %w", err)
	}
	// 开启 goroutine 消费 businessChannels
	d.tasks.Go(func() error {
		for batch := range d.businessChannels {
			log.Info("deposit business channel", "batch length", len(batch))
			if err := d.handleBatch(batch); err != nil {
				log.Error("gailed to handle batch, stopping syncer", "err", err)
				return fmt.Errorf("failed to handle batch , stopping syncer: %w", err)
			}
		}
		return nil
	})
	return nil
}

// Close 取消上下文，停止所有 goroutine
func (d *Deposit) Close() error {
	var result error

	if err := d.BaseSynchronizer.Close(); err != nil {
		result = errors.Join(result, fmt.Errorf("failed to close interna base synchronizer: %w", err))
	}
	d.resourceCancel()
	// 等待任务完成
	if err := d.tasks.Wait(); err != nil {
		result = errors.Join(result, fmt.Errorf("failed to wait tasks: %w", err))
	}
	return result
}

func (deposit *Deposit) handleBatch(batch map[string]*TransactionChannel) error {
	businessList, err := deposit.database.Business.QueryBusinessList()
	if err != nil {
		log.Error("query business list fail", "err", err)
		return err
	}
	if businessList == nil || len(businessList) <= 0 {
		err := fmt.Errorf("QueryBusinessList businessList is nil")
		return err
	}

	for _, business := range businessList {
		_, exist := batch[business.BusinessUid]
		if !exist {
			continue
		}

		var (
			transactionFlowList []*database.Transactions
			depositList         []*database.Deposits
			withdrawList        []*database.Withdraws
			internals           []*database.Internals
			balances            []*database.TokenBalance
		)

		log.Info("handle business flow", "businessId", business.BusinessUid, "chainLatestBlock", batch[business.BusinessUid].BlockHeight, "txn", len(batch[business.BusinessUid].Transactions))

		for _, tx := range batch[business.BusinessUid].Transactions {
			log.Info("Request transaction from chain account", "txHash", tx.Hash, "fromAddress", tx.FromAddress)

			txItem, err := deposit.rpcClient.GetTransactionByHash(tx.Hash)
			if err != nil {
				log.Info("get transaction by hash fail", "err", err)
				return err
			}
			if txItem == nil {
				err := fmt.Errorf("GetTransactionByHash txItem is nil: TxHash = %s", tx.Hash)
				return err
			}

			amountBigInt, _ := new(big.Int).SetString(txItem.Value, 10)
			log.Info("Transaction amount", "amountBigInt", amountBigInt, "FromAddress", tx.FromAddress, "TokenAddress", tx.TokenAddress, "TokenAddress", tx.ToAddress)
			balances = append(
				balances,
				&database.TokenBalance{
					FromAddress:  common.HexToAddress(tx.FromAddress),
					ToAddress:    common.HexToAddress(txItem.To),
					TokenAddress: common.HexToAddress(txItem.ContractAddress),
					Balance:      amountBigInt,
					TxType:       tx.TxType,
				},
			)

			log.Info("get transaction success", "txHash", txItem.Hash)
			transactionFlow, err := deposit.BuildTransaction(tx, txItem)
			if err != nil {
				log.Info("handle  transaction fail", "err", err)
				return err
			}
			transactionFlowList = append(transactionFlowList, transactionFlow)

			// 根据交易的交易类型修改在业务系统中的交易状态，保存
			switch tx.TxType {
			case database.TxTypeDeposit:
				depositItem, _ := deposit.HandleDeposit(tx, txItem)
				depositList = append(depositList, depositItem)
				break
			case database.TxTypeWithdraw:
				withdrawItem, _ := deposit.HandleWithdraw(tx, txItem)
				withdrawList = append(withdrawList, withdrawItem)
				break
			case database.TxTypeCollection, database.TxTypeHot2Cold, database.TxTypeCold2Hot:
				internalItem, _ := deposit.HandleInternalTx(tx, txItem)
				internals = append(internals, internalItem)
				break
			default:
				break
			}

		}

		retryStrategy := &retry.ExponentialStrategy{Min: 1000, Max: 20_000, MaxJitter: 250}
		if _, err := retry.Do[interface{}](deposit.resourceCtx, 10, retryStrategy, func() (interface{}, error) {
			// 数据库事务，外面包了一层，把 tx 封装进一个新的 DB 实例
			if err := deposit.database.Transaction(func(tx *database.DB) error {
				if len(depositList) > 0 {
					log.Info("Store deposit transaction success", "totalTx", len(depositList))
					if err := tx.Deposits.StoreDeposits(business.BusinessUid, depositList); err != nil {
						return err
					}
				}

				// 更新余额
				if len(balances) > 0 {
					log.Info("Handle balances success", "totalTx", len(balances))
					if err := tx.Balances.UpdateOrCreate(business.BusinessUid, balances); err != nil {
						return err
					}
				}

				// 更新提现状态
				if len(withdrawList) > 0 {
					if err := tx.Withdraws.UpdateWithdrawStatusByTxHash(business.BusinessUid, withdrawList); err != nil {
						return err
					}
				}

				// 更新内部交易状态
				if len(internals) > 0 {
					if err := tx.Internals.UpdateInternalStatusByTxHash(business.BusinessUid, database.TxStatusSuccess, internals); err != nil {
						return err
					}
				}

				// 保存交易流水
				if len(transactionFlowList) > 0 {
					if err := tx.Transactions.StoreTransactions(business.BusinessUid, transactionFlowList, uint64(len(transactionFlowList))); err != nil {
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
	return nil
}

func (d *Deposit) BuildTransaction(tx *Transaction, txMsg *account.TxMessage) (*database.Transactions, error) {
	txFee, _ := new(big.Int).SetString(txMsg.Fee, 10)
	txAmount, _ := new(big.Int).SetString(txMsg.Value, 10)
	transactionTx := &database.Transactions{
		GUID:         uuid.New(),
		BlockHash:    common.Hash{},
		BlockNumber:  tx.BlockNumber,
		Hash:         common.HexToHash(tx.Hash),
		FromAddress:  common.HexToAddress(tx.FromAddress),
		ToAddress:    common.HexToAddress(tx.ToAddress),
		TokenAddress: common.HexToAddress(tx.TokenAddress),
		TokenId:      "0x00",
		TokenMeta:    "0x00",
		Fee:          txFee,
		Status:       database.TxStatusSuccess,
		Amount:       txAmount,
		TxType:       tx.TxType,
		Timestamp:    uint64(time.Now().Unix()),
	}
	return transactionTx, nil
}

// HandleDeposit 构造一个充值交易对象
func (deposit *Deposit) HandleDeposit(tx *Transaction, txMsg *account.TxMessage) (*database.Deposits, error) {
	txAmount, _ := new(big.Int).SetString(txMsg.Value, 10)
	depositTx := &database.Deposits{
		GUID:         uuid.New(),
		BlockHash:    common.Hash{},
		BlockNumber:  tx.BlockNumber,
		TxHash:       common.HexToHash(tx.Hash),
		FromAddress:  common.HexToAddress(tx.FromAddress),
		ToAddress:    common.HexToAddress(tx.ToAddress),
		TokenAddress: common.HexToAddress(tx.TokenAddress),
		TokenId:      "0x00",
		TokenMeta:    "0x00",
		MaxFeePerGas: txMsg.Fee,
		Amount:       txAmount,
		Status:       database.TxStatusSafe, // 链上扫到了，待确认
		Timestamp:    uint64(time.Now().Unix()),
	}
	return depositTx, nil
}

func (deposit *Deposit) HandleWithdraw(tx *Transaction, txMsg *account.TxMessage) (*database.Withdraws, error) {
	txAmount, _ := new(big.Int).SetString(txMsg.Value, 10)
	withdrawTx := &database.Withdraws{
		GUID:         uuid.New(),
		BlockHash:    common.Hash{},
		BlockNumber:  tx.BlockNumber,
		TxHash:       common.HexToHash(tx.Hash),
		FromAddress:  common.HexToAddress(tx.FromAddress),
		ToAddress:    common.HexToAddress(tx.ToAddress),
		TokenAddress: common.HexToAddress(tx.TokenAddress),
		TokenId:      "0x00",
		TokenMeta:    "0x00",
		MaxFeePerGas: txMsg.Fee,
		Amount:       txAmount,
		Status:       database.TxStatusWithdrawed,
		Timestamp:    uint64(time.Now().Unix()),
	}
	return withdrawTx, nil
}

func (deposit *Deposit) HandleInternalTx(tx *Transaction, txMsg *account.TxMessage) (*database.Internals, error) {
	txAmount, _ := new(big.Int).SetString(txMsg.Value, 10)
	internalTx := &database.Internals{
		GUID:         uuid.New(),
		BlockHash:    common.Hash{},
		BlockNumber:  tx.BlockNumber,
		TxHash:       common.HexToHash(tx.Hash),
		FromAddress:  common.HexToAddress(tx.FromAddress),
		ToAddress:    common.HexToAddress(tx.ToAddress),
		TokenAddress: common.HexToAddress(tx.TokenAddress),
		TokenId:      "0x00",
		TokenMeta:    "0x00",
		MaxFeePerGas: txMsg.Fee,
		Amount:       txAmount,
		Status:       database.TxStatusSuccess,
		Timestamp:    uint64(time.Now().Unix()),
	}
	return internalTx, nil
}
