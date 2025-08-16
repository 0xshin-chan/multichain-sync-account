package worker

import (
	"context"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/log"
	"github.com/huahaiwudi/multichain-sync-account/common/tasks"
	"github.com/huahaiwudi/multichain-sync-account/config"
	"github.com/huahaiwudi/multichain-sync-account/database"
)

type Deposit struct {
	BaseSynchronizer
	resourceCtx    context.Context
	resourceCancel context.CancelFunc
	tasks          tasks.Group
}

func NewDeposit(cfg *config.Config, db *database.DB, shutdown context.CancelCauseFunc) (*Deposit, error) {
	resCtx, resCancel := context.WithCancel(context.Background())

	baseSyncer := BaseSynchronizer{
		loopInterval: cfg.ChainNode.SynchronizerInterval,
	}

	return &Deposit{
		BaseSynchronizer: baseSyncer,
		resourceCtx:      resCtx,
		resourceCancel:   resCancel,
		tasks: tasks.Group{HandleCrit: func(err error) {
			shutdown(fmt.Errorf("cretical error in deposit: %w", err))
		}},
	}, nil
}

func (d *Deposit) Start() error {
	log.Info("start deposit...")
	if err := d.BaseSynchronizer.Start(); err != nil {
		return fmt.Errorf("failed to start internal Synchronizer: %w", err)
	}
	d.tasks.Go(func() error {
		log.Info("handle deposit task start")
		return nil
	})
	return nil
}

func (d *Deposit) Close() error {
	var result error

	if err := d.BaseSynchronizer.Close(); err != nil {
		result = errors.Join(result, fmt.Errorf("failed to close interna base synchronizer: %w", err))
	}
	d.resourceCancel()
	if err := d.tasks.Wait(); err != nil {
		result = errors.Join(result, fmt.Errorf("failed to wait tasks: %w", err))
	}
	return result
}
