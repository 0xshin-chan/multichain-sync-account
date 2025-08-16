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

type FallBack struct {
	resourceCtx    context.Context
	resourceCancel context.CancelFunc
	tasks          tasks.Group
	ticker         *time.Ticker
}

func NewFallBack(cfg *config.Config, db *database.DB, shutdown context.CancelCauseFunc) (*FallBack, error) {
	resCtx, resCancel := context.WithCancel(context.Background())

	return &FallBack{
		resourceCtx:    resCtx,
		resourceCancel: resCancel,
		ticker:         time.NewTicker(time.Second * 3),
		tasks: tasks.Group{HandleCrit: func(err error) {
			shutdown(fmt.Errorf("cretical error in deposit: %w", err))
		}},
	}, nil
}

func (f *FallBack) Start() error {
	log.Info("start fallback...")

	f.tasks.Go(func() error {
		for {
			select {
			case <-f.ticker.C:
				log.Info("normal handle fallback business")
			case <-f.resourceCtx.Done():
				log.Info("stop fallback in worker")
				return nil
			}
		}
	})
	return nil
}

func (f *FallBack) Close() error {
	var result error
	f.resourceCancel()
	if err := f.tasks.Wait(); err != nil {
		result = errors.Join(result, fmt.Errorf("failed to wait tasks: %w", err))
	}
	return result
}
