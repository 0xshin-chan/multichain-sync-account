package rpc

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/google/uuid"

	"github.com/0xshin-chan/multichain-sync-account/common/json2"
	"github.com/0xshin-chan/multichain-sync-account/database"
	"github.com/0xshin-chan/multichain-sync-account/database/dynamic"
	"github.com/0xshin-chan/multichain-sync-account/protobuf/wallet"
	"github.com/0xshin-chan/multichain-sync-account/rpcclient/syncclient/account"
)

const (
	ConsumerToken = "slim"
)

var (
	EthGasLimit   uint64 = 60000
	TokenGasLimit uint64 = 120000
	Min1Gwei      uint64 = 1000000000
	//maxFeePerGas                = "135177480"
	//maxPriorityFeePerGas        = "535177480"
)

func (bws *BusinessMiddleWireServices) BusinessRegister(ctx context.Context, request *wallet.BusinessRegisterRequest) (*wallet.BusinessRegisterResponse, error) {
	resp := &wallet.BusinessRegisterResponse{
		Code: wallet.ReturnCode_ERROR,
	}
	if request.ConsumerToken != ConsumerToken {
		resp.Code = wallet.ReturnCode_ERROR
		resp.Msg = "invalid consumer token"
		return resp, nil
	}

	if request.RequestId == "" || request.NotifyUrl == "" {
		return &wallet.BusinessRegisterResponse{
			Code: wallet.ReturnCode_ERROR,
			Msg:  "invalid params",
		}, nil
	}
	business := &database.Business{
		GUID:        uuid.New(),
		BusinessUid: request.RequestId,
		NotifyUrl:   request.NotifyUrl,
		Timestamp:   uint64(time.Now().Unix()),
	}
	err := bws.db.Business.StoreBusiness(business)
	if err != nil {
		log.Error("store business fail", "err", err)
		return &wallet.BusinessRegisterResponse{
			Code: wallet.ReturnCode_ERROR,
			Msg:  "store db fail",
		}, nil
	}
	dynamic.CreateTableFromTemplate(request.RequestId, bws.db)
	return &wallet.BusinessRegisterResponse{
		Code: wallet.ReturnCode_SUCCESS,
		Msg:  "config business success",
	}, nil
}

// ExportAddressesByPublicKeys todo change to tx
// ExportAddressesByPublicKeys
func (bws *BusinessMiddleWireServices) ExportAddressesByPublicKeys(ctx context.Context, request *wallet.ExportAddressesRequest) (*wallet.ExportAddressesResponse, error) {
	resp := &wallet.ExportAddressesResponse{
		Code: wallet.ReturnCode_ERROR,
	}
	if request.ConsumerToken != ConsumerToken {
		resp.Code = wallet.ReturnCode_ERROR
		resp.Msg = "invalid consumer token"
		return resp, nil
	}

	var (
		retAddresses []*wallet.Address
		dbAddresses  []*database.Addresses
		balances     []*database.Balances
	)
	for _, value := range request.PublicKeys {
		address := bws.accountRpcClient.ExportAddressByPubKey("", value.PublicKey)
		item := &wallet.Address{
			Type:    value.Type,
			Address: address,
		}
		retAddresses = append(retAddresses, item)

		parseAddressType, err := database.ParseAddressType(value.Type)
		if err != nil {
			log.Error("parse address type fail", "err", err)
			return nil, err
		}

		dbAddress := &database.Addresses{
			GUID:        uuid.New(),
			Address:     common.HexToAddress(address),
			AddressType: parseAddressType,
			PublicKey:   value.PublicKey,
			Timestamp:   uint64(time.Now().Unix()),
		}
		dbAddresses = append(dbAddresses, dbAddress)
		balanceItem := &database.Balances{
			GUID:        uuid.New(),
			Address:     common.HexToAddress(address),
			AddressType: parseAddressType,
			Balance:     big.NewInt(0),
			Timestamp:   uint64(time.Now().Unix()),
		}
		balances = append(balances, balanceItem)
	}

	err := bws.db.Addresses.StoreAddresses(request.RequestId, dbAddresses)
	if err != nil {
		return &wallet.ExportAddressesResponse{
			Code:      wallet.ReturnCode_ERROR,
			Msg:       "store addresses to db fail",
			Addresses: nil,
		}, nil
	}
	err = bws.db.Balances.StoreBalances(request.RequestId, balances)
	if err != nil {
		return &wallet.ExportAddressesResponse{
			Code:      wallet.ReturnCode_ERROR,
			Msg:       "store balances to db fail",
			Addresses: nil,
		}, nil
	}

	return &wallet.ExportAddressesResponse{
		Code:      wallet.ReturnCode_SUCCESS,
		Msg:       "generate address success",
		Addresses: retAddresses,
	}, nil
}

func (bws *BusinessMiddleWireServices) BuildUnSignTransaction(ctx context.Context, request *wallet.UnSignTransactionRequest) (*wallet.UnSignTransactionResponse, error) {
	resp := &wallet.UnSignTransactionResponse{
		Code: wallet.ReturnCode_ERROR,
	}
	if request.ConsumerToken != ConsumerToken {
		resp.Code = wallet.ReturnCode_ERROR
		resp.Msg = "invalid consumer token"
		return resp, nil
	}

	var withdrawList []database.Withdraws
	var unSignedTxMsgList []*wallet.UnSignTransactionMessageHash

	for _, txItem := range request.UnSignTxn {
		amountBigInt, ok := new(big.Int).SetString(txItem.Value, 10)
		if !ok {
			return nil, fmt.Errorf("invalid amount value : %s", txItem.Value)
		}
		nonce, err := bws.getAccountNonce(ctx, bws.ChainName, bws.NetWork, txItem.From)
		if err != nil {
			log.Error("get account nonce fail", "err", err)
		}

		feeInfo, err := bws.getFeeInfo(ctx, bws.ChainName, bws.NetWork, txItem.From)
		if err != nil {
			log.Error("get fee info fail", "err", err)
			return nil, err
		}
		gasLimit, contractAddress := bws.getGasAndContractInfo(txItem.ContractAddress)

		transactionId := uuid.New()
		withdraw := database.Withdraws{
			GUID:                 transactionId,
			Timestamp:            uint64(time.Now().Unix()),
			Status:               database.TxStatusWaitSign,
			BlockHash:            common.Hash{},
			BlockNumber:          big.NewInt(0),
			TxHash:               common.Hash{},
			TxType:               database.TxTypeWithdraw,
			FromAddress:          common.HexToAddress(txItem.From),
			ToAddress:            common.HexToAddress(txItem.To),
			Memo:                 "",
			Amount:               amountBigInt,
			GasLimit:             gasLimit,
			MaxFeePerGas:         feeInfo.MaxFeePerGas,
			MaxPriorityFeePerGas: feeInfo.MaxPriorityFee,
			Nonce:                uint64(nonce),
			TokenType:            DetermineTokenType(txItem.ContractAddress),
			TokenAddress:         common.HexToAddress(txItem.ContractAddress),
			TokenId:              "0x00",
			TokenMeta:            "0x00",
			TxSignHex:            "",
			SelfSign:             true,
		}

		// 构建签名交易
		dynamicFeeReq := Eip1559DynamicFeeTx{
			ChainId:              bws.ChainName,
			Nonce:                uint64(nonce),
			FromAddress:          txItem.From,
			ToAddress:            txItem.To,
			GasLimit:             gasLimit,
			MaxFeePerGas:         feeInfo.MaxFeePerGas,
			MaxPriorityFeePerGas: feeInfo.MaxPriorityFee,
			Amount:               txItem.Value,
			ContractAddress:      contractAddress,
		}
		// 构建 32 位hash
		data := json2.ToJSON(dynamicFeeReq)
		base64Str := base64.StdEncoding.EncodeToString(data)
		unsignTx := &account.UnSignTransactionRequest{
			ConsumerToken: ConsumerToken,
			Chain:         bws.ChainName,
			Network:       bws.NetWork,
			Base64Tx:      base64Str,
		}
		returnTxMessageHash, err := bws.accountRpcClient.AccountRpClient.BuildUnSignTransaction(ctx, unsignTx)
		if err != nil {
			log.Error("build unsign transaction fail", "err", err)
			return nil, err
		}

		unSignTxnMsgHash := &wallet.UnSignTransactionMessageHash{
			TransactionId: transactionId.String(),
			UnSignTx:      returnTxMessageHash.String(),
		}

		unSignedTxMsgList = append(unSignedTxMsgList, unSignTxnMsgHash)
		withdrawList = append(withdrawList, withdraw)
	}

	err := bws.db.Withdraws.StoreWithdraws(request.RequestId, withdrawList)
	if err != nil {
		log.Error("store withdraws fail", "err", err)
		return nil, err
	}

	resp.Code = wallet.ReturnCode_SUCCESS
	resp.Msg = "submit withdraw and build un sign transaction success"
	resp.UnSignTxnMsgHash = unSignedTxMsgList
	return resp, nil
}

func (bws *BusinessMiddleWireServices) BuildSignedTransaction(ctx context.Context, request *wallet.SignedTransactionRequest) (*wallet.SignedTransactionResponse, error) {
	resp := &wallet.SignedTransactionResponse{
		Code: wallet.ReturnCode_ERROR,
	}
	if request.ConsumerToken != ConsumerToken {
		resp.Code = wallet.ReturnCode_ERROR
		resp.Msg = "invalid consumer token"
		return resp, nil
	}

	var signedTxList []*wallet.SignedTransactionWithSignature
	for _, txItem := range request.SignedTxn {
		var (
			fromAddress          string
			toAddress            string
			amount               string
			tokenAddress         string
			gasLimit             uint64
			nonce                uint64
			maxFeePerGas         string
			maxPriorityFeePerGas string
		)
		transactionType, err := database.ParseTransactionType(txItem.TxType)
		if err != nil {
			resp.Msg = "invalid request TxType"
			return resp, nil
		}

		if transactionType == database.TxTypeWithdraw {
			tx, err := bws.db.Withdraws.QueryWithdrawsById(request.RequestId, txItem.TransactionId)
			if err != nil {
				resp.Msg = "query withdraws fail"
				return resp, nil
			}
			if tx == nil {
				resp.Msg = "withdraw transaction not found"
			}

			fromAddress = tx.FromAddress.String()
			toAddress = tx.ToAddress.String()
			amount = tx.Amount.String()
			tokenAddress = tx.TokenAddress.String()
			gasLimit = tx.GasLimit
			nonce = tx.Nonce
			maxFeePerGas = tx.MaxFeePerGas
			maxPriorityFeePerGas = tx.MaxPriorityFeePerGas
		}
		if transactionType == database.TxTypeCollection || transactionType == database.TxTypeHot2Cold {
			tx, err := bws.db.Internals.QueryInternalsById(request.RequestId, txItem.TransactionId)
			if err != nil {
				resp.Msg = "query internal fail"
				return resp, nil
			}
			if tx == nil {
				resp.Msg = "internal transaction not found"
				return resp, nil
			}

			fromAddress = tx.FromAddress.String()
			toAddress = tx.ToAddress.String()
			amount = tx.Amount.String()
			tokenAddress = tx.TokenAddress.String()
			gasLimit = tx.GasLimit
			nonce = tx.Nonce
			maxFeePerGas = tx.MaxFeePerGas
			maxPriorityFeePerGas = tx.MaxPriorityFeePerGas
		}

		dynamicFeeTx := Eip1559DynamicFeeTx{
			ChainId:              txItem.ChainId,
			Nonce:                nonce,
			FromAddress:          fromAddress,
			ToAddress:            toAddress,
			GasLimit:             gasLimit,
			MaxFeePerGas:         maxFeePerGas,
			MaxPriorityFeePerGas: maxPriorityFeePerGas,
			Amount:               amount,
			ContractAddress:      tokenAddress,
		}

		data := json2.ToJSON(dynamicFeeTx)
		base64Str := base64.StdEncoding.EncodeToString(data)
		signedTxReq := &account.SignedTransactionRequest{
			ConsumerToken: ConsumerToken,
			Chain:         bws.ChainName,
			Network:       bws.NetWork,
			Base64Tx:      base64Str,
			Signature:     txItem.Signature,
		}

		rawTx, err := bws.accountRpcClient.AccountRpClient.BuildSignedTransaction(ctx, signedTxReq)
		if err != nil {
			resp.Msg = "build signed transaction fail"
			return resp, nil
		}
		if transactionType == database.TxTypeWithdraw {
			updateWithdrawErr := bws.db.Withdraws.UpdateWithdrawById(request.RequestId, txItem.TransactionId, rawTx.SignedTx, database.TxStatusUnSent)
			if updateWithdrawErr != nil {
				resp.Msg = "update withdraw fail"
				return resp, nil
			}
		}

		if transactionType == database.TxTypeCollection || transactionType == database.TxTypeHot2Cold {
			updateInternalErr := bws.db.Internals.UpdateInternalById(request.RequestId, txItem.TransactionId, rawTx.SignedTx, database.TxStatusUnSent)
			if updateInternalErr != nil {
				resp.Msg = "update internal fail"
				return resp, nil
			}
		}
		signedTx := &wallet.SignedTransactionWithSignature{
			SignedTx:      rawTx.SignedTx,
			TxHash:        common.Hash{}.String(),
			TransactionId: txItem.TransactionId,
		}
		signedTxList = append(signedTxList, signedTx)
	}

	resp.SignedTxnWithSignature = signedTxList
	resp.Msg = "build signed tx success"
	resp.Code = wallet.ReturnCode_SUCCESS
	return resp, nil
}

func (bws *BusinessMiddleWireServices) SetTokenAddress(ctx context.Context, request *wallet.SetTokenAddressRequest) (*wallet.SetTokenAddressResponse, error) {
	resp := &wallet.SetTokenAddressResponse{
		Code: wallet.ReturnCode_ERROR,
	}
	if request.ConsumerToken != ConsumerToken {
		resp.Code = wallet.ReturnCode_ERROR
		resp.Msg = "invalid consumer token"
		return resp, nil
	}

	var (
		tokenList []database.Tokens
	)
	for _, value := range request.TokenList {
		CollectAmountBigInt, _ := new(big.Int).SetString(value.CollectAmount, 10)
		ColdAmountBigInt, _ := new(big.Int).SetString(value.ColdAmount, 10)
		token := database.Tokens{
			GUID:          uuid.New(),
			TokenAddress:  common.HexToAddress(value.Address),
			Decimals:      uint8(value.Decimals),
			TokenName:     value.TokenName,
			CollectAmount: CollectAmountBigInt,
			ColdAmount:    ColdAmountBigInt,
			Timestamp:     uint64(time.Now().Unix()),
		}
		tokenList = append(tokenList, token)
	}
	err := bws.db.Tokens.StoreTokens(request.RequestId, tokenList)
	if err != nil {
		log.Error("set token address fail", "err", err)
		return nil, err
	}
	return &wallet.SetTokenAddressResponse{
		Code: 1,
		Msg:  "set token address success",
	}, nil
}

func (bws *BusinessMiddleWireServices) SubmitWithdraw(ctx context.Context, request *wallet.SubmitWithdrawRequest) (*wallet.SubmitWithdrawResponse, error) {
	resp := &wallet.SubmitWithdrawResponse{
		Code: wallet.ReturnCode_ERROR,
	}
	if request.ConsumerToken != ConsumerToken {
		resp.Code = wallet.ReturnCode_ERROR
		resp.Msg = "invalid consumer token"
		return resp, nil
	}

	var withdrawList []database.Withdraws
	for _, withdrawItem := range request.WithdrawList {
		bigIntValue, _ := new(big.Int).SetString(withdrawItem.Value, 10)
		withdraw := database.Withdraws{
			GUID:                 uuid.New(),
			Timestamp:            uint64(time.Now().Unix()),
			Status:               database.TxStatusWaitSign,
			BlockHash:            common.Hash{},
			BlockNumber:          big.NewInt(1),
			TxHash:               common.Hash{},
			TxType:               database.TxTypeWithdraw,
			FromAddress:          common.HexToAddress(withdrawItem.From),
			ToAddress:            common.HexToAddress(withdrawItem.To),
			Amount:               bigIntValue,
			GasLimit:             0,
			MaxFeePerGas:         "0",
			MaxPriorityFeePerGas: "0",
			TokenType:            DetermineTokenType(withdrawItem.ContractAddress),
			TokenAddress:         common.HexToAddress(withdrawItem.ContractAddress),
			TokenId:              withdrawItem.TokenId,
			TokenMeta:            withdrawItem.TokenMeta,
			TxSignHex:            "",
			SelfSign:             true,
		}
		withdrawList = append(withdrawList, withdraw)
	}
	err := bws.db.Withdraws.StoreWithdraws(request.RequestId, withdrawList)
	if err != nil {
		resp.Msg = "store Withdraws fail"
		return resp, err
	}
	return &wallet.SubmitWithdrawResponse{
		Code: wallet.ReturnCode_SUCCESS,
		Msg:  "submit withdraw success",
	}, nil
}

// get nonce
func (bws *BusinessMiddleWireServices) getAccountNonce(ctx context.Context, chainName string, network string, address string) (int, error) {
	accountReq := &account.AccountRequest{
		ConsumerToken:   ConsumerToken,
		Chain:           chainName,
		Network:         network,
		ContractAddress: "0x00",
	}

	accountInfo, err := bws.accountRpcClient.AccountRpClient.GetAccount(ctx, accountReq)
	if err != nil {
		return 0, fmt.Errorf("get account info failed: %w", err)
	}
	return strconv.Atoi(accountInfo.Sequence)
}

func (bws *BusinessMiddleWireServices) getFeeInfo(ctx context.Context, chainName string, network string, address string) (*FeeInfo, error) {
	accountFeeReq := &account.FeeRequest{
		ConsumerToken: ConsumerToken,
		Chain:         chainName,
		Network:       network,
		RawTx:         "",
		Address:       address,
	}
	feeResponse, err := bws.accountRpcClient.AccountRpClient.GetFee(ctx, accountFeeReq)
	if err != nil {
		return nil, fmt.Errorf("get fee info failed: %w", err)
	}

	return &FeeInfo{
		MaxFeePerGas:   feeResponse.Eip1559Wallet.MaxFeePerGas,
		MaxPriorityFee: feeResponse.Eip1559Wallet.MaxPriorityFee,
	}, nil
}

func (bws *BusinessMiddleWireServices) getGasAndContractInfo(contractAddress string) (uint64, string) {
	if contractAddress == "0x00" {
		return EthGasLimit, "0x00"
	}
	return TokenGasLimit, contractAddress
}
