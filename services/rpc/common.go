package rpc

import (
	"github.com/0xshin-chan/multichain-sync-account/database"
)

type FeeInfo struct {
	MaxPriorityFee string
	MaxFeePerGas   string
}

func DetermineTokenType(contractAddress string) database.TokenType {
	if contractAddress == "0x00" {
		return database.TokenTypeETH
	}
	return database.TokenTypeERC20
}
