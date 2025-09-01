package service

import (
	"strconv"

	"github.com/ethereum/go-ethereum/log"
	"github.com/pkg/errors"

	"github.com/0xshin-chan/multichain-sync-account/services/http/models"
)

type Service interface {
	GetGasFeeByChainId(params *models.QueryGasFeeParams) (*models.GasFeeResponse, error)

	QuerySRParams(address, page, pageSize, order string) (*models.QuerySRParams, error)
	QueryBRParams(address string, page string, pageSize string, order string) (*models.QueryBRParams, error)
}

type HandlerSvc struct {
	v *Validator
}

func New(v *Validator) Service {
	return &HandlerSvc{
		v: v,
	}
}

func (h HandlerSvc) GetGasFeeByChainId(params *models.QueryGasFeeParams) (*models.GasFeeResponse, error) {
	return &models.GasFeeResponse{}, nil
}

func (h HandlerSvc) QuerySRParams(address, page, pageSize, order string) (*models.QuerySRParams, error) {
	var paraAddress string
	if address == "0x00" {
		paraAddress = "0x00"
	} else {
		addr, err := h.v.ParseValidateAddress(address)
		if err != nil {
			log.Error("invalid address param", "address", address, "err", err)
			return nil, err
		}
		paraAddress = addr.String()
	}
	pageVal, err := strconv.Atoi(page)
	if err != nil {
		return nil, errors.New("page must be an integer value")
	}
	err = h.v.ValidatePage(pageVal)
	if err != nil {
		log.Error("invalid page param", "page", page, "err", err)
		return nil, err
	}

	pageSizeVal, err := strconv.Atoi(pageSize)
	if err != nil {
		return nil, errors.New("pageSize must be an integer value")
	}
	err = h.v.ValidatePageSize(pageSizeVal)
	if err != nil {
		log.Error("invalid query param", "pageSize", pageSize, "err", err)
		return nil, err
	}

	err = h.v.ValidateOrder(order)
	if err != nil {
		log.Error("invalid query param", "order", order, "err", err)
		return nil, err
	}

	return &models.QuerySRParams{
		Address:  paraAddress,
		Page:     pageVal,
		PageSize: pageSizeVal,
		Order:    order,
	}, nil
}

func (h HandlerSvc) QueryBRParams(address string, page string, pageSize string, order string) (*models.QueryBRParams, error) {
	var paraAddress string
	if address == "0x00" {
		paraAddress = "0x00"
	} else {
		addr, err := h.v.ParseValidateAddress(address)
		if err != nil {
			log.Error("invalid address param", "address", address, "err", err)
			return nil, err
		}
		paraAddress = addr.String()
	}
	pageVal, err := strconv.Atoi(page)
	if err != nil {
		return nil, errors.New("page must be an integer value")
	}
	err = h.v.ValidatePage(pageVal)
	if err != nil {
		log.Error("invalid page param", "page", page, "err", err)
		return nil, err
	}

	pageSizeVal, err := strconv.Atoi(pageSize)
	if err != nil {
		return nil, errors.New("pageSize must be an integer value")
	}
	err = h.v.ValidatePageSize(pageSizeVal)
	if err != nil {
		log.Error("invalid query param", "pageSize", pageSize, "err", err)
		return nil, err
	}

	err = h.v.ValidateOrder(order)
	if err != nil {
		log.Error("invalid query param", "pageSize", pageSize, "err", err)
		return nil, err
	}
	return &models.QueryBRParams{
		Address:  paraAddress,
		Page:     pageVal,
		PageSize: pageSizeVal,
		Order:    order,
	}, nil
}
