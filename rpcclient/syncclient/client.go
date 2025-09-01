package syncclient

import (
	"context"
	"math/big"
	"strconv"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"

	account2 "github.com/0xshin-chan/multichain-sync-account/rpcclient/syncclient/account"
)

type ChainAccountRpcClient struct {
	Ctx             context.Context
	ChainName       string
	AccountRpClient account2.WalletAccountServiceClient
}

func NewChainAccountRpcClient(ctx context.Context, rpc account2.WalletAccountServiceClient, chainName string) (*ChainAccountRpcClient, error) {
	log.Info("New account chain rpc client", "chainName", chainName)
	return &ChainAccountRpcClient{Ctx: ctx, AccountRpClient: rpc, ChainName: chainName}, nil
}

func (car *ChainAccountRpcClient) ExportAddressByPubKey(typeOrVersion, publicKey string) string {
	req := &account2.ConvertAddressRequest{
		Chain:     car.ChainName,
		Type:      typeOrVersion,
		PublicKey: publicKey,
	}
	address, err := car.AccountRpClient.ConvertAddress(car.Ctx, req)
	if err != nil {
		log.Error("covert address fail", "err", err)
		return ""
	}
	if address.Code == account2.ReturnCode_ERROR {
		log.Error("covert address fail", "err", err)
		return ""
	}
	return address.Address
}

func (car *ChainAccountRpcClient) GetBlockHeader(number *big.Int) (*BlockHeader, error) {
	var height int64
	if number == nil {
		height = 0
	} else {
		height = number.Int64()
	}
	req := &account2.BlockHeaderNumberRequest{
		Chain:   car.ChainName,
		Network: "mainnet",
		Height:  height,
	}
	blockHeader, err := car.AccountRpClient.GetBlockHeaderByNumber(car.Ctx, req)
	if err != nil {
		log.Error("get latest block GetBlockHeaderByNumber fail", "err", err)
		return nil, err
	}
	if blockHeader.Code == account2.ReturnCode_ERROR {
		log.Error("get latest block fail", "err", err)
		return nil, err
	}
	blockNumber, _ := new(big.Int).SetString(blockHeader.BlockHeader.Number, 10)
	header := &BlockHeader{
		Hash:       common.HexToHash(blockHeader.BlockHeader.Hash),
		ParentHash: common.HexToHash(blockHeader.BlockHeader.ParentHash),
		Number:     blockNumber,
		Timestamp:  blockHeader.BlockHeader.Time,
	}
	return header, nil
}

func (car *ChainAccountRpcClient) GetBlockInfo(blockNumber *big.Int) ([]*account2.BlockInfoTransactionList, error) {
	req := &account2.BlockNumberRequest{
		Chain:  car.ChainName,
		Height: blockNumber.Int64(),
		ViewTx: true,
	}
	blockInfo, err := car.AccountRpClient.GetBlockByNumber(car.Ctx, req)
	if err != nil {
		log.Error("get block GetBlockByNumber fail", "err", err)
		return nil, err
	}
	if blockInfo.Code == account2.ReturnCode_ERROR {
		log.Error("get block info fail", "err", err)
		return nil, err
	}
	return blockInfo.Transactions, nil
}

func (car *ChainAccountRpcClient) GetTransactionByHash(hash string) (*account2.TxMessage, error) {
	req := &account2.TxHashRequest{
		Chain:   car.ChainName,
		Network: "mainnet",
		Hash:    hash,
	}
	txInfo, err := car.AccountRpClient.GetTxByHash(car.Ctx, req)
	if err != nil {
		log.Error("get GetTxByHash fail", "err", err)
		return nil, err
	}
	if txInfo.Code == account2.ReturnCode_ERROR {
		log.Error("get block info fail", "err", err)
		return nil, err
	}
	return txInfo.Tx, nil
}

func (car *ChainAccountRpcClient) GetAccountAccountNumber(address string) (int, error) {
	req := &account2.AccountRequest{
		Chain:   car.ChainName,
		Network: "mainnet",
		Address: address,
	}
	accountInfo, err := car.AccountRpClient.GetAccount(car.Ctx, req)
	if accountInfo.Code == account2.ReturnCode_ERROR {
		log.Error("get block info fail", "err", err)
		return 0, err
	}
	return strconv.Atoi(accountInfo.AccountNumber)
}

func (car *ChainAccountRpcClient) GetAccount(address string) (int, int, int) {
	req := &account2.AccountRequest{
		Chain:           car.ChainName,
		Network:         "mainnet",
		Address:         address,
		ContractAddress: "0x00",
	}

	accountInfo, err := car.AccountRpClient.GetAccount(car.Ctx, req)
	if err != nil {
		log.Info("GetAccount fail", "err", err)
		return 0, 0, 0
	}

	if accountInfo.Code == account2.ReturnCode_ERROR {
		log.Info("get account info fail", "msg", accountInfo.Msg)
		return 0, 0, 0
	}

	accountNumber, err := strconv.Atoi(accountInfo.AccountNumber)
	if err != nil {
		log.Info("failed to convert account number", "err", err)
		return 0, 0, 0
	}

	sequence, err := strconv.Atoi(accountInfo.Sequence)
	if err != nil {
		log.Info("failed to convert sequence", "err", err)
		return 0, 0, 0
	}

	balance, err := strconv.Atoi(accountInfo.Balance)
	if err != nil {
		log.Info("failed to convert balance", "err", err)
		return 0, 0, 0
	}

	return accountNumber, sequence, balance
}

func (car *ChainAccountRpcClient) SendTx(rawTx string) (string, error) {
	log.Info("Send transaction", "rawTx", rawTx, "ChainName", car.ChainName)
	req := &account2.SendTxRequest{
		Chain:   car.ChainName,
		Network: "mainnet",
		RawTx:   rawTx,
	}
	txInfo, err := car.AccountRpClient.SendTx(car.Ctx, req)
	if txInfo == nil {
		log.Error("send tx info fail, txInfo is null")
		return "", err
	}
	if txInfo.Code == account2.ReturnCode_ERROR {
		log.Error("send tx info fail", "err", err)
		return "", err
	}
	return txInfo.TxHash, nil
}
