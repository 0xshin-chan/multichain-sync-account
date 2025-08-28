package http

import (
	"context"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/log"

	"github.com/huahaiwudi/multichain-sync-account/config"
	"github.com/huahaiwudi/multichain-sync-account/database"
)

type Notifier struct {
	cfg              config.Config
	depositNotify    *DepositNotify
	withdrawNotify   *WithdrawNotify
	internalCallBack *InternalCallBack

	shutdown context.CancelCauseFunc
	stopped  atomic.Bool
}

func NewNotifier(ctx context.Context, cfg config.Config, db *database.DB, shutdown context.CancelCauseFunc) (*Notifier, error) {

	var businessIds []string
	// 1. 从数据库查询所有商户
	businessList, err := db.Business.QueryBusinessList()
	if err != nil {
		log.Error("query business list fail", "err", err)
		return nil, err
	}

	// 2.为每个 businessItem 创建一个 NotifyClient
	notifyClient := make(map[string]*NotifyClient)

	for _, businessItem := range businessList {
		businessIds = append(businessIds, businessItem.BusinessUid)
		// 根据商户的 NotifyUrl 创建 http client
		nClient, err := NewNotifierClient(businessItem.NotifyUrl)
		if err != nil {
			log.Error("new notifier client fail", "err", err)
			return nil, err
		}
		notifyClient[businessItem.BusinessUid] = nClient
	}

	// 3.创建充值、提现的 Notify
	depositNotify, _ := NewDepositNotify(db, notifyClient, businessIds, shutdown)
	withdrawNotify, _ := NewWithdrawNotify(db, notifyClient, businessIds, shutdown)
	internalCallBack, _ := NewInternalCallBack(db, notifyClient, businessIds, shutdown)

	// 4.封窗成 Notifier 对象返回
	out := &Notifier{
		cfg:              cfg,
		depositNotify:    depositNotify,
		withdrawNotify:   withdrawNotify,
		internalCallBack: internalCallBack,
		shutdown:         shutdown,
	}
	return out, nil
}

func (nf *Notifier) Start(ctx context.Context) error {
	err := nf.depositNotify.Start()
	if err != nil {
		return err
	}
	err = nf.withdrawNotify.Start()
	if err != nil {
		return err
	}
	if nf.cfg.IsSelfSign {
		err = nf.internalCallBack.Start()
		if err != nil {
			return err
		}
	}
	return nil
}

func (nf *Notifier) Stop(ctx context.Context) error {
	err := nf.depositNotify.Close()
	if err != nil {
		return err
	}
	err = nf.withdrawNotify.Close()
	if err != nil {
		return err
	}
	err = nf.internalCallBack.Close()
	if err != nil {
		return err
	}
	return nil
}

func (nf *Notifier) Stopped() bool {
	return nf.stopped.Load()
}
