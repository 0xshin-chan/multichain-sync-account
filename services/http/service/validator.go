package service

import (
	"errors"
	"github.com/ethereum/go-ethereum/common"
)

type Validator struct{}

// 验证地址合法性
func (v *Validator) ParseValidateAddress(addr string) (common.Address, error) {
	var parsedAddr common.Address
	// 判断是否为合法 16 进制地址
	if addr != "0x00" {
		if !common.IsHexAddress(addr) {
			return common.Address{}, errors.New("address must be represented as a valid hexadecimal string")
		}
		parsedAddr = common.HexToAddress(addr)
		// 不能是零地址
		if parsedAddr == common.HexToAddress("0x0") {
			return common.Address{}, errors.New("address cannot be the zero address")
		}
	}
	return parsedAddr, nil
}

// 验证页码
func (v *Validator) ValidatePage(page int) error {
	if page <= 0 {
		return errors.New("page must be more than 1")
	}
	return nil
}

// 验证每页大小
func (v *Validator) ValidatePageSize(pageSize int) error {
	if pageSize <= 0 {
		return errors.New("page size must be more than 1")
	}
	if pageSize > 1000 {
		return errors.New("page size must be less than 1000")
	}
	return nil
}

// 验证排序字段
func (v *Validator) ValidateOrder(order string) error {
	if order == "asc" || order == "ASC" || order == "DESC" || order == "desc" {
		return nil
	} else {
		return errors.New("order is must one of asc, ASC, DESC, desc value")
	}
}

// 验证 id 或索引
func (v *Validator) ValidateIdOrIndex(idOrIndex uint64) error {
	if idOrIndex <= 0 {
		return errors.New("page size must be more than 0")
	}
	return nil
}
