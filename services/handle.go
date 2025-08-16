package services

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/google/uuid"

	"github.com/huahaiwudi/multichain-sync-account/database"
	"github.com/huahaiwudi/multichain-sync-account/database/dynamic"
	"github.com/huahaiwudi/multichain-sync-account/protobuf/wallet"
)

const (
	ChainName = "Ethereum"
	Network   = "mainnet"
)

var (
	EthGasLimit   uint64 = 60000
	TokenGasLimit uint64 = 120000
	Min1Gwei      uint64 = 1000000000
	//maxFeePerGas                = "135177480"
	//maxPriorityFeePerGas        = "535177480"
)

func (bws *BusinessMiddleWireServices) BusinessRegister(ctx context.Context, request *wallet.BusinessRegisterRequest) (*wallet.BusinessRegisterResponse, error) {
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
	return &wallet.ExportAddressesResponse{
		Code:      wallet.ReturnCode_SUCCESS,
		Msg:       "generate address success",
		Addresses: nil,
	}, nil
}

func (bws *BusinessMiddleWireServices) BuildUnSignTransaction(ctx context.Context, request *wallet.UnSignTransactionRequest) (*wallet.UnSignTransactionResponse, error) {
	response := &wallet.UnSignTransactionResponse{
		Code:     wallet.ReturnCode_ERROR,
		UnSignTx: "0x00",
	}
	response.Code = wallet.ReturnCode_SUCCESS
	response.Msg = "submit withdraw and build un sign tranaction success"
	response.TransactionId = "guid.String()"
	response.UnSignTx = "returnTx.UnSignTx"
	return response, nil
}

func (bws *BusinessMiddleWireServices) BuildSignedTransaction(ctx context.Context, request *wallet.SignedTransactionRequest) (*wallet.SignedTransactionResponse, error) {
	response := &wallet.SignedTransactionResponse{
		Code: wallet.ReturnCode_ERROR,
	}

	response.SignedTx = ""
	response.Msg = "build signed tx success"
	response.Code = wallet.ReturnCode_SUCCESS
	return response, nil
}

func (bws *BusinessMiddleWireServices) SetTokenAddress(ctx context.Context, request *wallet.SetTokenAddressRequest) (*wallet.SetTokenAddressResponse, error) {
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

// FeeInfo 结构体用于存储解析后的费用信息
type FeeInfo struct {
	GasPrice       *big.Int // 基础 gas 价格
	GasTipCap      *big.Int // 小费上限
	Multiplier     int64    // 倍数
	MultipliedTip  *big.Int // 小费 * 倍数
	MaxPriorityFee *big.Int // 小费 * 倍数 * 2 (最大上限)
}

// ParseFastFee 解析 FastFee 字符串并计算相关费用
func ParseFastFee(fastFee string) (*FeeInfo, error) {
	// 1. 按 "|" 分割字符串
	parts := strings.Split(fastFee, "|")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid fast fee format: %s", fastFee)
	}

	// 2. 解析 GasPrice (baseFee)
	gasPrice := new(big.Int)
	if _, ok := gasPrice.SetString(parts[0], 10); !ok {
		return nil, fmt.Errorf("invalid gas price: %s", parts[0])
	}

	// 3. 解析 GasTipCap
	gasTipCap := new(big.Int)
	if _, ok := gasTipCap.SetString(parts[1], 10); !ok {
		return nil, fmt.Errorf("invalid gas tip cap: %s", parts[1])
	}

	// 4. 解析倍数（去掉 "*" 前缀）
	multiplierStr := strings.TrimPrefix(parts[2], "*")
	multiplier, err := strconv.ParseInt(multiplierStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid multiplier: %s", parts[2])
	}

	// 5. 计算 MultipliedTip (小费 * 倍数)
	multipliedTip := new(big.Int).Mul(
		gasTipCap,
		big.NewInt(multiplier),
	)
	// 设置最小小费阈值 (1 Gwei)
	//minTipCap := big.NewInt(int64(Min1Gwei))
	//if multipliedTip.Cmp(minTipCap) < 0 {
	//	multipliedTip = minTipCap
	//}

	// 6. 计算 MaxPriorityFee (baseFee + 小费*倍数*2)
	maxPriorityFee := new(big.Int).Mul(
		multipliedTip,
		big.NewInt(2),
	)
	// 加上 baseFee
	maxPriorityFee.Add(maxPriorityFee, gasPrice)

	return &FeeInfo{
		GasPrice:       gasPrice,
		GasTipCap:      gasTipCap,
		Multiplier:     multiplier,
		MultipliedTip:  multipliedTip,
		MaxPriorityFee: maxPriorityFee,
	}, nil
}

func validateRequest(request *wallet.UnSignTransactionRequest) error {
	if request == nil {
		return errors.New("request cannot be nil")
	}
	if request.From == "" {
		return errors.New("from address cannot be empty")
	}
	if request.To == "" {
		return errors.New("to address cannot be empty")
	}
	if request.Value == "" {
		return errors.New("value cannot be empty")
	}
	return nil
}

func determineTokenType(contractAddress string) database.TokenType {
	if contractAddress == "0x00" {
		return database.TokenTypeETH
	}
	// 这里可以添加更多的 token 类型判断逻辑
	return database.TokenTypeERC20
}

func (bws *BusinessMiddleWireServices) getAccountNonce(ctx context.Context, address string) (int, error) {
	return strconv.Atoi("1")
}

func (bws *BusinessMiddleWireServices) getFeeInfo(ctx context.Context, address string) (*FeeInfo, error) {
	return nil, nil
}

func (bws *BusinessMiddleWireServices) getGasAndContractInfo(contractAddress string) (uint64, string) {
	if contractAddress == "0x00" {
		return EthGasLimit, "0x00"
	}
	return TokenGasLimit, contractAddress
}

func (bws *BusinessMiddleWireServices) storeWithdraw(request *wallet.UnSignTransactionRequest,
	transactionId uuid.UUID, amountBig *big.Int, gasLimit uint64, feeInfo *FeeInfo, transactionType database.TransactionType) error {

	withdraw := &database.Withdraws{
		GUID:                 transactionId,
		Timestamp:            uint64(time.Now().Unix()),
		Status:               database.TxStatusCreateUnsigned,
		BlockHash:            common.Hash{},
		BlockNumber:          big.NewInt(1),
		TxHash:               common.Hash{},
		TxType:               transactionType,
		FromAddress:          common.HexToAddress(request.From),
		ToAddress:            common.HexToAddress(request.To),
		Amount:               amountBig,
		GasLimit:             gasLimit,
		MaxFeePerGas:         feeInfo.MaxPriorityFee.String(),
		MaxPriorityFeePerGas: feeInfo.MultipliedTip.String(),
		TokenType:            determineTokenType(request.ContractAddress),
		TokenAddress:         common.HexToAddress(request.ContractAddress),
		TokenId:              request.TokenId,
		TokenMeta:            request.TokenMeta,
		TxSignHex:            "",
	}

	return bws.db.Withdraws.StoreWithdraw(request.RequestId, withdraw)
}

// 辅助方法：存储内部交易
func (bws *BusinessMiddleWireServices) storeInternal(request *wallet.UnSignTransactionRequest,
	transactionId uuid.UUID, amountBig *big.Int, gasLimit uint64, feeInfo *FeeInfo, transactionType database.TransactionType) error {

	internal := &database.Internals{
		GUID:                 transactionId,
		Timestamp:            uint64(time.Now().Unix()),
		Status:               database.TxStatusCreateUnsigned,
		BlockHash:            common.Hash{},
		BlockNumber:          big.NewInt(1),
		TxHash:               common.Hash{},
		TxType:               transactionType,
		FromAddress:          common.HexToAddress(request.From),
		ToAddress:            common.HexToAddress(request.To),
		Amount:               amountBig,
		GasLimit:             gasLimit,
		MaxFeePerGas:         feeInfo.MaxPriorityFee.String(),
		MaxPriorityFeePerGas: feeInfo.MultipliedTip.String(),
		TokenType:            determineTokenType(request.ContractAddress),
		TokenAddress:         common.HexToAddress(request.ContractAddress),
		TokenId:              request.TokenId,
		TokenMeta:            request.TokenMeta,
		TxSignHex:            "",
	}

	return bws.db.Internals.StoreInternal(request.RequestId, internal)
}

func (bws *BusinessMiddleWireServices) StoreDeposits(ctx context.Context,
	depositsRequest *wallet.UnSignTransactionRequest, transactionId uuid.UUID, amountBig *big.Int,
	gasLimit uint64, feeInfo *FeeInfo, transactionType database.TransactionType) error {

	dbDeposit := &database.Deposits{
		GUID:                 transactionId,
		Timestamp:            uint64(time.Now().Unix()),
		Status:               database.TxStatusCreateUnsigned,
		Confirms:             0,
		BlockHash:            common.Hash{},
		BlockNumber:          big.NewInt(1),
		TxHash:               common.Hash{},
		TxType:               transactionType,
		FromAddress:          common.HexToAddress(depositsRequest.From),
		ToAddress:            common.HexToAddress(depositsRequest.To),
		Amount:               amountBig,
		GasLimit:             gasLimit,
		MaxFeePerGas:         feeInfo.MaxPriorityFee.String(),
		MaxPriorityFeePerGas: feeInfo.MultipliedTip.String(),
		TokenType:            determineTokenType(depositsRequest.ContractAddress),
		TokenAddress:         common.HexToAddress(depositsRequest.ContractAddress),
		TokenId:              depositsRequest.TokenId,
		TokenMeta:            depositsRequest.TokenMeta,
		TxSignHex:            "",
	}

	return bws.db.Deposits.StoreDeposits(depositsRequest.RequestId, []*database.Deposits{dbDeposit})
}
