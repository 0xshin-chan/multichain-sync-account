package routes

import (
	"net/http"
	"strconv"

	"github.com/ethereum/go-ethereum/log"

	"github.com/0xshin-chan/multichain-sync-account/services/http/models"
)

func (h Routes) GasFeeHandler(w http.ResponseWriter, r *http.Request) {
	chainId := r.URL.Query().Get("chain")
	rawtx := r.URL.Query().Get("rawtx")
	chainIdInt, err := strconv.Atoi(chainId)
	if err != nil {
		return
	}
	if chainIdInt <= 0 {
		http.Error(w, "invalid query params", http.StatusBadRequest)
		log.Error("error reading request params", "err", err.Error())
		return
	}
	log.Info("ChainId and Symbol", "rawtx", rawtx, "chainId", chainId)
	gasFeeResponse := models.GasFeeResponse{
		ChainId:   1,
		LowFee:    "210000",
		NormalFee: "2110000",
		HighFee:   "12210000",
	}
	err = jsonResponse(w, &gasFeeResponse, http.StatusOK)
	if err != nil {
		log.Error("Error writing response", "err", err.Error())
	}
}
