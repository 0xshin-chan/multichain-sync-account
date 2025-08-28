package http

import "github.com/huahaiwudi/multichain-sync-account/database"

// =========业务通知==========
type NotifyRequest struct {
	Txn []*Transaction `json:"txn"`
}

type Transaction struct {
	BlockHash    string                   `json:"block_hash"`
	BlockNumber  uint64                   `json:"block_number"`
	Hash         string                   `json:"hash"`
	FromAddress  string                   `json:"from_address"`
	ToAddress    string                   `json:"to_address"`
	Value        string                   `json:"value"`
	Fee          string                   `json:"fee"`
	TxType       database.TransactionType `json:"tx_type"`
	TxStatus     database.TxStatus        `json:"tx_status"`
	Confirms     uint8                    `json:"confirms"`
	TokenAddress string                   `json:"token_address"`
	TokenId      string                   `json:"token_id"`
	TokenMeta    string                   `json:"token_meta"`
}

type NotifyResponse struct {
	Success bool `json:"success"`
}

// ========归集回调=========
type CollectionTx struct {
	BusinessId            string `json:"business_id"`
	TransactionId         string `json:"transaction_id"`
	UnsignedTxMessageHash string `json:"unsigned_tx_message_hash"`
}

type InternalCollectionCallBack struct {
	CollectionTxn []*CollectionTx `json:"collection_txn"`
}

type CallBackResponse struct {
	Success bool `json:"success"`
}
