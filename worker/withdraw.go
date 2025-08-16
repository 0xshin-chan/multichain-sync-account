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

type Withdraw struct {
	ticker         *time.Ticker
	resourceCtx    context.Context
	resourceCancel context.CancelFunc
	tasks          tasks.Group
}

func NewWithdraw(cfg *config.Config, db *database.DB, shutdown context.CancelCauseFunc) (*Withdraw, error) {
	resCtx, resCancel := context.WithCancel(context.Background())

	return &Withdraw{
		ticker:         time.NewTicker(5 * time.Second),
		resourceCtx:    resCtx,
		resourceCancel: resCancel,
		tasks: tasks.Group{HandleCrit: func(err error) {
			shutdown(fmt.Errorf("cretical error in deposit: %w", err))
		}},
	}, nil
}

func (w *Withdraw) Start() error {
	log.Info("start withdraw...")

	w.tasks.Go(func() error {
		for {
			select {
			case <-w.ticker.C:
				log.Info("normal handle withdraw business")
			case <-w.resourceCtx.Done():
				log.Info("stop withdraw in worker")
				return nil
			}
		}
	})
	return nil
}

func (w *Withdraw) Close() error {
	var result error
	w.resourceCancel()
	if err := w.tasks.Wait(); err != nil {
		result = errors.Join(result, fmt.Errorf("failed to wait tasks: %w", err))
	}
	return result
}
