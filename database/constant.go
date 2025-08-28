package database

import (
	"errors"
	"fmt"
	"strings"
)

type TxStatus string

// 交易状态
const (
	TxStatusUnSent               TxStatus = "unsend"              // 交易未发送
	TxStatusWaitSign             TxStatus = "wait_sign"           // 交易等待签名
	TxStatusSent                 TxStatus = "sent"                // 交易已发送
	TxStatusSentNotify           TxStatus = "sent_notify_success" // 交易以广播通知
	TxStatusSentNotifyFail       TxStatus = "sent_notify_fail"    // 交易以广播失败通知
	TxStatusWithdrawed           TxStatus = "withdrawed"          // 交易已发送
	TxStatusWithdrawedNotify     TxStatus = "withdrawed_notify_success"
	TxStatusWithdrawedNotifyFail TxStatus = "withdrawed_notify_fail"

	TxStatusUnSafe              TxStatus = "unsafe"                   // 链上扫到交易
	TxStatusSafe                TxStatus = "safe"                     // 交易过了安全确认位
	TxStatusFinalized           TxStatus = "finalized"                // 交易已完成，可以提现
	TxStatusUnSafeNotify        TxStatus = "unsafe_notify_success"    // 链上扫到交易已通知
	TxStatusSafeNotify          TxStatus = "safe_notify_success"      // 交易过了安全确认位已通知
	TxStatusFinalizedNotify     TxStatus = "finalized_notify_success" // 交易完成已通知
	TxStatusUnSafeNotifyFail    TxStatus = "unsafe_notify_fail"       // 链上扫到交易通知失败
	TxStatusSafeNotifyFail      TxStatus = "safe_notify_fail"         // 交易过了安全确认位通知失败
	TxStatusFinalizedNotifyFail TxStatus = "finalized_notify_fail"    // 交易完成通知失败

	TxStatusSuccess        TxStatus = "done_success"
	TxStatusFail           TxStatus = "done_fail"
	TxStatusFailNotify     TxStatus = "done_fail_notify_success"
	TxStatusFailNotifyFail TxStatus = "done_fail_notify_fail"

	TxStatusFallback           TxStatus = "fallback"                // 交易回滚状态
	TxStatusFallbackNotify     TxStatus = "fallback_notify_success" // 交易回滚通知成功
	TxStatusFallbackNotifyFail TxStatus = "fallback_notify_fail"    // 交易回滚通知失败
	TxStatusFallbackDone       TxStatus = "done_fallback"           // 交易回滚状态
	TxStatusInternalCallBack   TxStatus = "send_to_business_for_sign"
)

type TokenType string

const (
	TokenTypeETH     TokenType = "ETH"
	TokenTypeERC20   TokenType = "ERC20"
	TokenTypeERC721  TokenType = "ERC721"
	TokenTypeERC1155 TokenType = "ERC1155"
)

type AddressType string

const (
	AddressTypeUser AddressType = "user"
	AddressTypeHot  AddressType = "hot"
	AddressTypeCold AddressType = "cold"
)

func (at AddressType) String() string {
	return string(at)
}

func ParseAddressType(s string) (AddressType, error) {
	switch strings.ToLower(s) {
	case string(AddressTypeUser):
		return AddressTypeUser, nil
	case string(AddressTypeHot):
		return AddressTypeHot, nil
	case string(AddressTypeCold):
		return AddressTypeCold, nil
	default:
		return "", fmt.Errorf("invalid address type: %s", s)
	}
}

type TransactionType string

// 交易类型
const (
	TxTypeUnKnow     TransactionType = "unknow"
	TxTypeDeposit    TransactionType = "deposit"
	TxTypeWithdraw   TransactionType = "withdraw"
	TxTypeCollection TransactionType = "collection"
	TxTypeHot2Cold   TransactionType = "hot2cold"
	TxTypeCold2Hot   TransactionType = "cold2hot"
)

func ParseTransactionType(s string) (TransactionType, error) {
	switch s {
	case string(TxTypeDeposit):
		return TxTypeDeposit, nil
	case string(TxTypeWithdraw):
		return TxTypeWithdraw, nil
	case string(TxTypeCollection):
		return TxTypeCollection, nil
	case string(TxTypeHot2Cold):
		return TxTypeHot2Cold, nil
	case string(TxTypeCold2Hot):
		return TxTypeCold2Hot, nil
	default:
		return TxTypeUnKnow, errors.New("unknown transaction type")
	}
}
