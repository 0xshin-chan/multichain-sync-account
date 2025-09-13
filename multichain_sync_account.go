package multichain_syncs6_account

import (
	"context"
	"github.com/0xshin-chan/multichain-sync-account/rpcclient/syncclient"
	"github.com/0xshin-chan/multichain-sync-account/rpcclient/syncclient/account"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sync/atomic"

	"github.com/0xshin-chan/multichain-sync-account/config"
	"github.com/0xshin-chan/multichain-sync-account/database"
	"github.com/0xshin-chan/multichain-sync-account/worker"
	"github.com/ethereum/go-ethereum/log"
)

// MultiChainSync 用于统一管理多链同步相关的子模块：存款、提现、内部转账、回滚。
// 负责整体的启动和停止。
type MultiChainSync struct {
	Synchronizer *worker.BaseSynchronizer
	Deposit      *worker.Deposit
	Withdraw     *worker.Withdraw
	Internal     *worker.Internal
	FallBack     *worker.FallBack

	shutdown context.CancelCauseFunc
	stopped  atomic.Bool
}

func NewMultiChainSync(ctx context.Context, cfg *config.Config, shutdown context.CancelCauseFunc) (*MultiChainSync, error) {
	// 初始化数据库连接
	db, err := database.NewDB(ctx, cfg.MasterDB)
	if err != nil {
		log.Error("init database fail", err)
		return nil, err
	}

	log.Info("New deposit", "ChainAccountRpc", cfg.ChainAccountRpc)

	// 连接链账户 RPC（gRPC），WithTransportCredentials：指定加密协议，NewCredentials： 明文传输
	conn, err := grpc.NewClient(cfg.ChainAccountRpc, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error("Connect to da retriever fail", "err", err)
		return nil, err
	}
	// 创建链账户客户端
	client := account.NewWalletAccountServiceClient(conn)

	accountClient, err := syncclient.NewChainAccountRpcClient(context.Background(), client, "Ethereum")
	if err != nil {
		log.Error("new wallet account http fail", "err", err)
		return nil, err
	}

	deposit, _ := worker.NewDeposit(cfg, db, accountClient, shutdown)
	withdraw, _ := worker.NewWithdraw(cfg, db, accountClient, shutdown)
	internal, _ := worker.NewInternal(cfg, db, accountClient, shutdown)
	fallback, _ := worker.NewFallBack(cfg, db, accountClient, deposit, shutdown)

	out := &MultiChainSync{
		Deposit:  deposit,
		Withdraw: withdraw,
		Internal: internal,
		FallBack: fallback,
		shutdown: shutdown,
	}
	return out, nil
}

func (mcs *MultiChainSync) Start(ctx context.Context) error {
	err := mcs.Deposit.Start()
	if err != nil {
		return err
	}
	err = mcs.Withdraw.Start()
	if err != nil {
		return err
	}
	err = mcs.Internal.Start()
	if err != nil {
		return err
	}
	//err = mcs.FallBack.Start()
	//if err != nil {
	//	return err
	//}
	return nil
}

func (mcs *MultiChainSync) Stop(ctx context.Context) error {
	err := mcs.Deposit.Close()
	if err != nil {
		return err
	}
	err = mcs.Withdraw.Close()
	if err != nil {
		return err
	}
	err = mcs.Internal.Close()
	if err != nil {
		return err
	}
	err = mcs.FallBack.Close()
	if err != nil {
		return err
	}
	return nil
}

func (mcs *MultiChainSync) Stopped() bool {
	return mcs.stopped.Load()
}
