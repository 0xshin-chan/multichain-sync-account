package worker

import (
	"context"
	"errors"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"

	"github.com/huahaiwudi/multichain-sync-account/common/clock"
	"github.com/huahaiwudi/multichain-sync-account/database"
	"github.com/huahaiwudi/multichain-sync-account/rpcclient/syncclient"
)

// 区块链交易对象
type Transaction struct {
	BusinessId     string
	BlockNumber    *big.Int
	FromAddress    string
	ToAddress      string
	Hash           string
	TokenAddress   string
	ContractWallet string
	TxType         database.TransactionType
}

type Config struct {
	LoopIntervalMsec uint
	HeaderBufferSize uint
	StartHeight      *big.Int
	Confirmations    uint64
}

// 区块同步器
type BaseSynchronizer struct {
	loopInterval        time.Duration
	headerBufferSize    uint64
	businessChannels    chan map[string]*TransactionChannel
	rpcClient           *syncclient.ChainAccountRpcClient
	blockBatch          *syncclient.BatchBlock
	database            *database.DB
	headers             []syncclient.BlockHeader
	worker              *clock.LoopFn
	fallbackBlockHeader *syncclient.BlockHeader
	isFallBack          bool
}

// 业务的交易通道
type TransactionChannel struct {
	BlockHeight  uint64
	ChannelId    string
	Transactions []*Transaction
}

func (syncer *BaseSynchronizer) Start() error {
	if syncer.worker != nil {
		return errors.New("worker is already started")
	}
	// 初始化循环任务
	syncer.worker = clock.NewLoopFn(clock.SystemClock, syncer.tick, func() error {
		return nil
	}, syncer.loopInterval)
	return nil
}

func (syncer *BaseSynchronizer) Close() error {
	if syncer.worker == nil {
		return nil
	}
	return syncer.worker.Close()
}

func (syncer *BaseSynchronizer) tick(_ context.Context) {
	// 如果headers原本就有值
	if len(syncer.headers) > 0 {
		log.Info("retrying previous batch")
	} else {
		if !syncer.isFallBack {
			// 调到拉取到的最新的区块头 headers
			newHeaders, fallBlockHeader, isReorg, err := syncer.blockBatch.NextHeaders(syncer.headerBufferSize)
			if err != nil {
				if isReorg && errors.Is(err, syncclient.ErrBlockFallBack) {
					if !syncer.isFallBack {
						log.Warn("found block fallback, start fallback tack")
						syncer.isFallBack = true
						syncer.fallbackBlockHeader = fallBlockHeader
					} else {
						log.Warn("the block fallback, fallback task handling it now")
					}
				} else {
					log.Error("error querying for headers", "err", err)
				}
			} else if len(newHeaders) == 0 {
				// 没有新块，可能已经追到链顶了
				log.Warn("no new headers. syncer at head?")
			} else {
				// 正常拉到新块，缓存起来
				syncer.headers = newHeaders
			}
		}
		// 处理这批区块
		err := syncer.processBatch(syncer.headers)
		if err == nil {
			// 成功处理完，清空 headers 缓存
			syncer.headers = nil
		} else {
			log.Info("handle fallback now.....")
		}
	}
}

// 处理区块数据，提取业务相关的交易
func (syncer *BaseSynchronizer) processBatch(headers []syncclient.BlockHeader) error {
	if len(headers) == 0 {
		return nil
	}

	// 业务交易通道
	businessTxChannel := make(map[string]*TransactionChannel)
	// 存储区块头到数据库的切片
	blockHeaders := make([]database.Blocks, len(headers))

	for i := range headers {
		log.Info("Sync block data", "height", headers[i].Number)
		blockHeaders[i] = database.Blocks{
			Hash:       headers[i].Hash,
			ParentHash: headers[i].ParentHash,
			Number:     headers[i].Number,
			Timestamp:  headers[i].Timestamp,
		}
		// 获取该区块的交易列表
		txList, err := syncer.rpcClient.GetBlockInfo(headers[i].Number)
		if err != nil {
			log.Error("get block info fail", "err", err)
			return err
		}

		// 从数据库查询所有业务
		businessList, err := syncer.database.Business.QueryBusinessList()
		if err != nil {
			log.Error("get business list fail", "err", err)
			return err
		}

		// 遍历每个业务，筛选属于该业务的交易
		for _, businessId := range businessList {
			var businessTransactions []*Transaction
			for _, tx := range txList {
				toAddress := common.HexToAddress(tx.To)
				fromAddress := common.HexToAddress(tx.From)
				// 检查地址是否属于该业务，并返回地址类型
				existToAddress, toAddressType := syncer.database.Addresses.AddressExist(businessId.BusinessUid, &toAddress)
				existFromAddress, FromAddressType := syncer.database.Addresses.AddressExist(businessId.BusinessUid, &fromAddress)
				// 如果两个地址都不属于该业务，跳过
				if !existToAddress && !existFromAddress {
					continue
				}

				tokenInfo, err := syncer.database.Tokens.TokensInfoByAddress(businessId.BusinessUid, tx.TokenAddress)
				if err != nil {
					log.Error("get token info by token address fail", "err", err)
				}

				if tokenInfo == nil {
					continue
				}

				log.Info("Found transaction", "txHash", tx.Hash, "from", fromAddress, "to", toAddress)

				// 命中交易，构建 Transaction
				txItem := &Transaction{
					BusinessId:     businessId.BusinessUid,
					BlockNumber:    headers[i].Number,
					FromAddress:    tx.From,
					ToAddress:      tx.To,
					Hash:           tx.Hash,
					TokenAddress:   tx.TokenAddress,
					ContractWallet: tx.ContractWallet,
					TxType:         database.TxTypeUnKnow,
				}

				// ====== 分类交易类型 ======
				if !existFromAddress && (existToAddress && toAddressType == database.AddressTypeUser) { // 充值
					log.Info("Found deposit transaction", "txHash", tx.Hash, "from", fromAddress, "to", toAddress)
					txItem.TxType = database.TxTypeDeposit
				}

				if (existFromAddress && FromAddressType == database.AddressTypeHot) && !existToAddress { // 提现
					log.Info("Found withdraw transaction", "txHash", tx.Hash, "from", fromAddress, "to", toAddress)
					txItem.TxType = database.TxTypeWithdraw
				}

				if (existFromAddress && FromAddressType == database.AddressTypeUser) && (existToAddress && toAddressType == database.AddressTypeHot) { // 归集
					log.Info("Found collection transaction", "txHash", tx.Hash, "from", fromAddress, "to", toAddress)
					txItem.TxType = database.TxTypeCollection
				}

				if (existFromAddress && FromAddressType == database.AddressTypeHot) && (existToAddress && toAddressType == database.AddressTypeCold) { // 热转冷
					log.Info("Found hot2cold transaction", "txHash", tx.Hash, "from", fromAddress, "to", toAddress)
					txItem.TxType = database.TxTypeHot2Cold
				}

				if (existFromAddress && FromAddressType == database.AddressTypeCold) && (existToAddress && toAddressType == database.AddressTypeHot) { // 冷转热
					log.Info("Found cold2hot transaction", "txHash", tx.Hash, "from", fromAddress, "to", toAddress)
					txItem.TxType = database.TxTypeCold2Hot
				}
				// 加入到该业务的交易列表
				businessTransactions = append(businessTransactions, txItem)
			}
			// 如果该业务有命中交易，加入到 businessTxChannel
			if len(businessTransactions) > 0 {
				if businessTxChannel[businessId.BusinessUid] == nil {
					// 第一次命中，创建新的 TransactionChannel
					businessTxChannel[businessId.BusinessUid] = &TransactionChannel{
						BlockHeight:  headers[i].Number.Uint64(),
						Transactions: businessTransactions,
					}
				} else {
					// 已经有了，追加交易
					businessTxChannel[businessId.BusinessUid].BlockHeight = headers[i].Number.Uint64()
					// ...是 go 的切片展开语法，必须写
					businessTxChannel[businessId.BusinessUid].Transactions = append(businessTxChannel[businessId.BusinessUid].Transactions, businessTransactions...)
				}
			}
		}
	}
	// 存储区块头到数据库
	if len(blockHeaders) > 0 {
		log.Info("Store block headers success", "totalBlockHeader", len(blockHeaders))
		if err := syncer.database.Blocks.StoreBlockss(blockHeaders); err != nil {
			return err
		}
	}

	// 把这批业务相关交易通过 channel 传出去（供后续 worker 消费）
	log.Info("business tx channel", "businessTxChannel", businessTxChannel, "map length", len(businessTxChannel))
	if len(businessTxChannel) > 0 {
		syncer.businessChannels <- businessTxChannel
	}

	return nil
}
