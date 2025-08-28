package http

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/huahaiwudi/multichain-sync-account/cache"
	"github.com/huahaiwudi/multichain-sync-account/common/httputil"
	"github.com/huahaiwudi/multichain-sync-account/config"
	"github.com/huahaiwudi/multichain-sync-account/metrics"
	"github.com/huahaiwudi/multichain-sync-account/services/http/routes"
	"github.com/huahaiwudi/multichain-sync-account/services/http/service"
)

const (
	HealthPath      = "/healthz"
	GetGasFeeV1Path = "/api/v1/get-fee"
)

type APIConfig struct {
	HTTPServer    config.ServerConfig
	MetricsServer config.ServerConfig
}

type API struct {
	apiConfig       APIConfig
	metricsRegistry *prometheus.Registry
	syncerMetrics   *metrics.SyncerMetrics
	router          *chi.Mux // HTTP 路由器
	apiServer       *httputil.HTTPServer
	metricsServer   *httputil.HTTPServer
	stopped         atomic.Bool
}

func NewApi(ctx context.Context, cfg *config.Config) (*API, error) {
	out := &API{}
	if err := out.initFromConfig(ctx, cfg); err != nil {
		return nil, errors.Join(err, out.Stop(ctx)) // 出错时确保关闭
	}
	return out, nil
}

func (a *API) initFromConfig(ctx context.Context, cfg *config.Config) error {
	a.initRouter(cfg) // 初始化路由
	apiConfig := APIConfig{
		HTTPServer:    cfg.HttpServer,
		MetricsServer: cfg.MetricsServer,
	}
	a.apiConfig = apiConfig

	metricsRegistry := metrics.NewRegistry() //prometheus注册表

	syncerMetrics := metrics.NewSyncerMetrics(metricsRegistry, "syncer") // 指标器

	a.metricsRegistry = metricsRegistry
	a.syncerMetrics = syncerMetrics

	return nil
}

// 初始化 HTTP 路由：
// 配置缓存
// 使用中间件（超时、panic恢复、心跳）
// 注册 /api/v1/get-fee 业务路由
func (a *API) initRouter(conf *config.Config) {
	v := new(service.Validator) // 创建一个 Validator，用于参数验证

	var lruCache = new(cache.LruCache) // 默认是空 cache
	if conf.ApiCacheEnable {
		lruCache = cache.NewLruCache() // 启用 LRU 缓存
	}

	svc := service.New(v) // 创建 service 层处理对象

	apiRouter := chi.NewRouter() // 创建 chi 路由器
	// 封装路由和 handler
	h := routes.NewRoutes(apiRouter, svc, conf.ApiCacheEnable, lruCache)

	apiRouter.Use(middleware.Timeout(time.Second * 12)) // 请求超时 12s
	apiRouter.Use(middleware.Recoverer)                 // panic 恢复

	apiRouter.Use(middleware.Heartbeat(HealthPath)) // 健康检查 /healthz

	// 注册路由
	// 当请求匹配 /api/v1/get-fee 并且是 GET 方法时，Chi 会调用 GasFeeHandler 这个函数处理请求
	// 注册路由的核心作用就是 建立 URL → 函数 的映射，实现 HTTP 请求的分发
	apiRouter.Get(fmt.Sprintf(GetGasFeeV1Path), h.GasFeeHandler)

	a.router = apiRouter
}

func (a *API) Start(ctx context.Context) error {
	go func() {
		err := func() error {
			if err := a.startMetricsServer(a.apiConfig.MetricsServer); err != nil {
				return fmt.Errorf("failed to start metrics server: %w", err)
			}
			return nil
		}()
		if err != nil {
			log.Error("start metrics fail", "err", err)
		}
	}()

	//*chi.Mux 本身实现了 http.Handler 接口，可以当作 http.Handler 使用
	if err := a.startServer(a.apiConfig.HTTPServer); err != nil {
		return fmt.Errorf("failed to start API server: %w", err)
	}

	return nil
}

func (a *API) Stop(ctx context.Context) error {
	var result error
	if a.apiServer != nil {
		if err := a.apiServer.Stop(ctx); err != nil {
			result = errors.Join(result, fmt.Errorf("failed to stop API server: %w", err))
		}
	}
	if a.metricsServer != nil {
		if err := a.metricsServer.Stop(ctx); err != nil {
			result = errors.Join(result, fmt.Errorf("failed to stop metrics server: %w", err))
		}
	}
	a.stopped.Store(true)
	log.Info("API service shutdown complete")
	return result
}

func (a *API) startServer(serverConfig config.ServerConfig) error {
	log.Debug("API server listening...", "port", serverConfig.Port)
	addr := net.JoinHostPort(serverConfig.Host, strconv.Itoa(serverConfig.Port))
	srv, err := httputil.StartHTTPServer(addr, a.router)
	if err != nil {
		return fmt.Errorf("failed to start API server: %w", err)
	}
	log.Info("API server started", "addr", srv.Addr().String())
	a.apiServer = srv
	return nil
}

func (as *API) startMetricsServer(cfg config.ServerConfig) error {
	srv, err := metrics.StartServer(as.metricsRegistry, cfg.Host, cfg.Port)
	if err != nil {
		return fmt.Errorf("metrics server failed to start: %w", err)
	}
	as.metricsServer = srv
	log.Info("metrics server started", "port", cfg.Port, "addr", srv.Addr())
	return nil
}

func (a *API) Stopped() bool {
	return a.stopped.Load()
}
