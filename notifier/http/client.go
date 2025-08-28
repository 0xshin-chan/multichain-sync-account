package http

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"

	"github.com/ethereum/go-ethereum/log"
	gresty "github.com/go-resty/resty/v2"
)

var errBlockChainHTTPError = errors.New("blockchain http error")

type NotifyClient struct {
	client *gresty.Client
}

func NewNotifierClient(baseUrl string) (*NotifyClient, error) {
	if baseUrl == "" {
		return nil, fmt.Errorf("blockchain URL cannot be empty")
	}
	// 新建 resty client
	client := gresty.New()
	// 设置基础 url， 后续请求会自动拼接
	client.SetBaseURL(baseUrl)
	// 钩子，每次请求完成后都会调用该回调函数
	client.OnAfterResponse(func(c *gresty.Client, r *gresty.Response) error {
		statusCode := r.StatusCode()
		if statusCode >= 400 {
			method := r.Request.Method
			url := r.Request.URL
			return fmt.Errorf("%d cannot %s %s: %w", statusCode, method, url, errBlockChainHTTPError)
		}
		return nil
	})
	return &NotifyClient{
		client: client,
	}, nil
}

// BusinessNotify 向外部服务发送通知
func (nc *NotifyClient) BusinessNotify(notifyData *NotifyRequest) (bool, error) {
	// 将请求数列化为 json
	body, err := json.Marshal(notifyData)
	if err != nil {
		log.Error("failed to marshal notify data", "err", err)
		return false, err
	}
	// 发起 http post 请求
	res, err := nc.client.R().
		SetHeader("Content-Type", "application/json"). // 设置请求头
		SetBody(body).                                 // 设置请求体
		SetResult(&NotifyResponse{}).                  // 设置响应结果绑定到 NotifyResponse
		Post("/dapplink/notify")                       // 调用接口 post

	if err != nil {
		log.Error("get transaction fee fail", "err", err)
		return false, err
	}
	// 将结果返回为 *NotifyResponse 类型
	// 使用类型断言，这个 res.Result() 是一个 interface 类型，可以用 .(type) 方式断言，Result() 接口里装的是 *NotifyResponse 类型
	spt, ok := res.Result().(*NotifyResponse)
	if !ok {
		return false, errors.New("get transaction fee fail, ok is false")
	}

	return spt.Success, nil
}

// BusinessInternalNotify 向外部服务发送通知
func (nc *NotifyClient) BusinessInternalNotify(internalCallBack *InternalCollectionCallBack) (bool, error) {
	// 将请求数列化为 json
	body, err := json.Marshal(internalCallBack)
	if err != nil {
		log.Error("failed to marshal notify data", "err", err)
		return false, err
	}
	// 发起 http post 请求
	res, err := nc.client.R().
		SetHeader("Content-Type", "application/json"). // 设置请求头
		SetBody(body).                                 // 设置请求体
		SetResult(&NotifyResponse{}).                  // 设置响应结果绑定到 NotifyResponse
		Post("/dapplink/callback")                     // 调用接口 post

	if err != nil {
		log.Error("callback txn fail", "err", err)
		return false, err
	}
	// 将结果返回为 *CallBackResponse 类型
	// 使用类型断言，这个 res.Result() 是一个 interface 类型，可以用 .(type) 方式断言，Result() 接口里装的是 *CallBackResponse 类型
	spt, ok := res.Result().(*CallBackResponse)
	if !ok {
		return false, errors.New("callback txn fail, ok is false")
	}

	return spt.Success, nil
}
