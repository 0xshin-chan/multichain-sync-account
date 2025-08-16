package worker

import (
	"context"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/log"
	"github.com/huahaiwudi/multichain-sync-account/common/tasks"
	"github.com/huahaiwudi/multichain-sync-account/config"
	"github.com/huahaiwudi/multichain-sync-account/database"
	"time"
)

type Internal struct {
	ticker         *time.Ticker
	resourceCtx    context.Context
	resourceCancel context.CancelFunc
	tasks          tasks.Group
}

func NewInternal(cfg *config.Config, db *database.DB, shutdown context.CancelCauseFunc) (*Internal, error) {
	resCtx, resCancel := context.WithCancel(context.Background())

	return &Internal{
		ticker:         time.NewTicker(5 * time.Second),
		resourceCtx:    resCtx,
		resourceCancel: resCancel,
		tasks: tasks.Group{HandleCrit: func(err error) {
			shutdown(fmt.Errorf("cretical error in deposit: %w", err))
		}},
	}, nil
}

func (i *Internal) Start() error {
	log.Info("start internal...")

	i.tasks.Go(func() error {
		for {
			select {
			case <-i.ticker.C:
				log.Info("normal handle fallback business")
			case <-i.resourceCtx.Done():
				log.Info("stop internal in worker")
				return nil
			}
		}
	})
	return nil
}

func (i *Internal) Close() error {
	var result error
	i.resourceCancel()
	if err := i.tasks.Wait(); err != nil {
		result = errors.Join(result, fmt.Errorf("failed to wait tasks: %w", err))
	}
	return result
}
