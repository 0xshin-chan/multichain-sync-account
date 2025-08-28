package models

type QuerySRParams struct {
	Address  string
	Page     int
	PageSize int
	Order    string
}

type QueryBRParams struct {
	Address  string
	Page     int
	PageSize int
	Order    string
}

type QueryPageParams struct {
	ChainId  string
	Page     int
	PageSize int
	Order    string
}

type QueryIdParams struct {
	ChainId string
	Id      uint64
}

type QueryIndexParams struct {
	ChainId string
	Index   uint64
}

type StakingResponse struct {
	Current int   `json:"Current"`
	Size    int   `json:"Size"`
	Total   int64 `json:"Total"`
}

type BridgeResponse struct {
	Current int   `json:"Current"`
	Size    int   `json:"Size"`
	Total   int64 `json:"Total"`
}

type ValidResult struct {
	Result Result `json:"result"`
}

type Result struct {
	IsValid bool `json:"isValid"`
}

type QueryGasFeeParams struct {
	ChainId uint64
	Symbol  string
}

type GasFeeResponse struct {
	ChainId   uint64 `json:"chain_id"`
	LowFee    string `json:"low_fee"`
	NormalFee string `json:"normal_fee"`
	HighFee   string `json:"high_fee"`
}
