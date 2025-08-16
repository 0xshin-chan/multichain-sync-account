package worker

import (
	"context"
	"errors"
	"github.com/ethereum/go-ethereum/log"
	"github.com/huahaiwudi/multichain-sync-account/common/clock"
	"time"
)

type BaseSynchronizer struct {
	loopInterval time.Duration
	worker       *clock.LoopFn
}

func (syncer *BaseSynchronizer) Start() error {
	if syncer.worker != nil {
		return errors.New("worker is already started")
	}
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
	log.Info("tick exec start")
	return
}
