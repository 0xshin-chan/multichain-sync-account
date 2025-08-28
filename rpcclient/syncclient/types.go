package syncclient

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

type BlockHeader struct {
	Hash       common.Hash
	ParentHash common.Hash
	Number     *big.Int
	Timestamp  uint64
}
