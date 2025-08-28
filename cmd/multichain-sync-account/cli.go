package main

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/urfave/cli/v2"

	multichain_sync_account "github.com/huahaiwudi/multichain-sync-account"
	"github.com/huahaiwudi/multichain-sync-account/common/cliapp"
	"github.com/huahaiwudi/multichain-sync-account/common/opio"
	"github.com/huahaiwudi/multichain-sync-account/config"
	"github.com/huahaiwudi/multichain-sync-account/database"
	flags2 "github.com/huahaiwudi/multichain-sync-account/flags"
	http2 "github.com/huahaiwudi/multichain-sync-account/notifier/http"
	"github.com/huahaiwudi/multichain-sync-account/rpcclient/syncclient"
	"github.com/huahaiwudi/multichain-sync-account/rpcclient/syncclient/account"
	"github.com/huahaiwudi/multichain-sync-account/services/http"
	"github.com/huahaiwudi/multichain-sync-account/services/rpc"
)

const (
	POLLING_INTERVAL     = 1 * time.Second
	MAX_RPC_MESSAGE_SIZE = 1024 * 1024 * 300
)

func runMultichainSync(ctx *cli.Context, shutdown context.CancelCauseFunc) (cliapp.Lifecycle, error) {
	log.Info("exec wallet sync")
	cfg, err := config.LoadConfig(ctx)
	fmt.Println()
	if err != nil {
		log.Error("failed to load config", "error", err)
		return nil, err
	}
	return multichain_sync_account.NewMultiChainSync(ctx.Context, &cfg, shutdown)
}

func runRpc(ctx *cli.Context, shutdown context.CancelCauseFunc) (cliapp.Lifecycle, error) {
	fmt.Println("running grpc server...")
	cfg, err := config.LoadConfig(ctx)
	if err != nil {
		log.Error("failed to load config", "err", err)
		return nil, err
	}
	grpcServerCfg := &rpc.BusinessMiddleConfig{
		GrpcHostname: cfg.RpcServer.Host,
		GrpcPort:     cfg.RpcServer.Port,
		ChainName:    cfg.ChainNode.ChainName,
		NetWork:      cfg.ChainNode.NetWork,
	}
	db, err := database.NewDB(ctx.Context, cfg.MasterDB)
	if err != nil {
		log.Error("failed to connect to database", "err", err)
		return nil, err
	}

	log.Info("chain account rpc", "rpc url:", cfg.ChainAccountRpc)
	conn, err := grpc.NewClient(cfg.ChainAccountRpc, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error("Connect to da retriever fail", "err", err)
		return nil, err
	}
	client := account.NewWalletAccountServiceClient(conn)
	accountClient, err := syncclient.NewChainAccountRpcClient(context.Background(), client, cfg.ChainNode.ChainName)
	if err != nil {
		log.Error("new wallet account client fail", "err", err)
		return nil, err
	}

	return rpc.NewBusinessMiddleWireServices(db, grpcServerCfg, accountClient)
}

func runMigrations(ctx *cli.Context) error {
	ctx.Context = opio.CancelOnInterrupt(ctx.Context)
	log.Info("running migrations...")
	cfg, err := config.LoadConfig(ctx)
	if err != nil {
		log.Error("failed to load config", "err", err)
		return err
	}
	db, err := database.NewDB(ctx.Context, cfg.MasterDB)
	if err != nil {
		log.Error("failed to connect to database", "err", err)
		return err
	}
	defer func(db *database.DB) {
		err := db.Close()
		if err != nil {
			log.Error("fail to close database", "err", err)
		}
	}(db)
	return db.ExecuteSQLMigration(cfg.Migrations)
}

func runNotify(ctx *cli.Context, shutdown context.CancelCauseFunc) (cliapp.Lifecycle, error) {
	fmt.Println("running notify task...")
	cfg, err := config.LoadConfig(ctx)
	if err != nil {
		log.Error("failed to load config", "err", err)
		return nil, err
	}
	db, err := database.NewDB(ctx.Context, cfg.MasterDB)
	if err != nil {
		log.Error("failed to connect to database", "err", err)
		return nil, err
	}
	return http2.NewNotifier(ctx.Context, cfg, db, shutdown)
}

func runApi(ctx *cli.Context, _ context.CancelCauseFunc) (cliapp.Lifecycle, error) {
	cfg, err := config.LoadConfig(ctx)
	if err != nil {
		log.Error("failed to load config", "err", err)
		return nil, err
	}
	return http.NewApi(ctx.Context, &cfg)
}

func NewCli(GitCommit string, GitData string) *cli.App {
	flags := flags2.Flags
	return &cli.App{
		Version:              "0.0.1",
		Description:          "An exchange wallet scanner services with rpc and rest api server",
		EnableBashCompletion: true,
		Commands: []*cli.Command{
			{
				Name:        "rpc",
				Flags:       flags,
				Description: "Run rpc services",
				Action:      cliapp.LifecycleCmd(runRpc),
			},
			{
				Name:        "sync",
				Flags:       flags,
				Description: "Run rpc scanner wallet chain node",
				Action:      cliapp.LifecycleCmd(runMultichainSync),
			},
			{
				Name:        "notify",
				Flags:       flags,
				Description: "Run rpc scanner wallet chain node",
				Action:      cliapp.LifecycleCmd(runNotify),
			},
			{
				Name:        "api",
				Flags:       flags,
				Description: "Run rpc scanner wallet chain node",
				Action:      cliapp.LifecycleCmd(runNotify),
			},
			{
				Name:        "migrate",
				Flags:       flags,
				Description: "Run database migrations",
				Action:      runMigrations,
			},
			{
				Name:        "version",
				Description: "Show project version",
				Action: func(ctx *cli.Context) error {
					cli.ShowVersion(ctx)
					return nil
				},
			},
		},
	}
}
