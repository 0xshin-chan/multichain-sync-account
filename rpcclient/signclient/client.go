package signclient

import (
	"context"

	"github.com/ethereum/go-ethereum/log"

	"github.com/huahaiwudi/multichain-sync-account/rpcclient/signclient/wallet"
)

type SignMachineRpcClient struct {
	Ctx          context.Context
	ChainName    string
	Network      string
	SignRpClient wallet.WalletServiceClient
}

func NewSignMachineRpcClient(ctx context.Context, rpc wallet.WalletServiceClient, chainName string) (*SignMachineRpcClient, error) {
	log.Info("New account chain rpc client", "chainName", chainName)
	return &SignMachineRpcClient{
		Ctx:          ctx,
		SignRpClient: rpc,
		ChainName:    chainName,
	}, nil
}

func (smr *SignMachineRpcClient) BuildAndSignTransaction(publicKey, walletKeyHash, riskKeyHash, txBase64Body string) (*SignedTxTransaction, error) {
	signRequest := &wallet.BuildAndSignTransactionRequest{
		ConsumerToken: "slim",
		ChainName:     smr.ChainName,
		Network:       smr.Network,
		PublicKey:     publicKey,
		WalletKeyHash: walletKeyHash,
		RiskKeyHash:   riskKeyHash,
		TxBase64Body:  txBase64Body,
	}

	signedTxn, err := smr.SignRpClient.BuildAndSignTransaction(smr.Ctx, signRequest)
	if err != nil {
		log.Error("build and sign transaction fail", "err", err)
		return &SignedTxTransaction{}, err
	}
	if signedTxn.Code == wallet.ReturnCode_ERROR {
		return &SignedTxTransaction{}, nil
	}

	return &SignedTxTransaction{
		TxMessageHash: signedTxn.TxMessageHash,
		TxHash:        signedTxn.TxHash,
		SignedTx:      signedTxn.SignedTx,
	}, nil
}
