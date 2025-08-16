package rpcclient

import (
	"errors"
	"math/big"
)

var (
	ErrBatchBlockAheadOfProvider = errors.New("the BatchBlock's internal state is ahead of the provider")
	ErrBlockFallBack             = errors.New("the block fallback, fallback handle it now")
)

type BatchBlock struct {
	latestHeader        *BlockHeader
	lastTraversedHeader *BlockHeader

	blockConfirmationDepth *big.Int
}
