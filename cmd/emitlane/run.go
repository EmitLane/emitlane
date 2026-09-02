package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"golang.org/x/sync/errgroup"

	"github.com/emitlane/emitlane/broker/kafka"
	"github.com/emitlane/emitlane/config"
	adminapi "github.com/emitlane/emitlane/internal/admin"
	"github.com/emitlane/emitlane/relay"
	"github.com/emitlane/emitlane/storage/postgres"
	"github.com/emitlane/emitlane/telemetry"
)

func runCmd(args []string) error {
	if len(args) != 0 {
		return fmt.Errorf("usage: emitlane run")
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := newLogger(cfg.LogLevel)
	slog.SetDefault(log)
	otel.SetTextMapPropagator(telemetry.TextMapPropagator())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := newPool(ctx, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := requireCurrentSchema(ctx, pool); err != nil {
		return err
	}

	metrics, err := telemetry.NewMetrics(prometheus.DefaultRegisterer)
	if err != nil {
		return fmt.Errorf("metrics: %w", err)
	}

	pub, err := kafka.NewPublisher(kafka.Config{
		Brokers:          cfg.KafkaBrokers,
		ClientID:         cfg.KafkaClientID,
		PublishTimeout:   cfg.Relay.PublishTimeout,
		AutoCreateTopics: cfg.AutoCreateTopics,
	})
	if err != nil {
		return err
	}
	defer func() { _ = pub.Close() }()

	store, err := postgres.NewStore(pool)
	if err != nil {
		return err
	}
	rly, err := relay.New(cfg.Relay, store, pub,
		relay.WithLogger(log),
		relay.WithMetrics(metrics),
		relay.WithWakeupListener(postgres.NewListener(cfg.DatabaseURL, log)),
		relay.WithPresenceInfo("", version),
	)
	if err != nil {
		return err
	}

	httpSrv := newHTTPServer(cfg.HTTPAddr, pool, pub)
	var adminSrv *http.Server
	if cfg.Admin.Enabled {
		service, err := adminapi.NewService(store, cfg.Relay.PresenceStaleAfter, metrics)
		if err != nil {
			return err
		}
		adminSrv, err = adminapi.NewHTTPServer(adminapi.HTTPConfig{
			Addr: cfg.Admin.Addr, Token: cfg.Admin.Token,
			ExposePayload: cfg.Admin.ExposePayload, Logger: log,
		}, service)
		if err != nil {
			return err
		}
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		log.Info("http server listening", "addr", cfg.HTTPAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()
		shctx, cancel := context.WithTimeout(context.Background(), cfg.Relay.ShutdownTimeout)
		defer cancel()
		return httpSrv.Shutdown(shctx)
	})
	g.Go(func() error {
		return rly.Run(gctx)
	})
	if adminSrv != nil {
		g.Go(func() error {
			log.Info("admin server listening", "addr", cfg.Admin.Addr, "payload_exposure", cfg.Admin.ExposePayload)
			if err := adminSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				return err
			}
			return nil
		})
		g.Go(func() error {
			<-gctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Relay.ShutdownTimeout)
			defer cancel()
			return adminSrv.Shutdown(shutdownCtx)
		})
	}
	return g.Wait()
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

func newPool(ctx context.Context, cfg config.Config) (*pgxpool.Pool, error) {
	pcfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("database url: %w", err)
	}
	pcfg.MaxConns = cfg.DBMaxConns
	pcfg.MinConns = cfg.DBMinConns
	pcfg.MaxConnLifetime = cfg.DBMaxConnLifetime
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return pool, nil
}

func newHTTPServer(addr string, pool *pgxpool.Pool, pub *kafka.Publisher) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if err := pool.Ping(ctx); err != nil {
			http.Error(w, "postgres unavailable", http.StatusServiceUnavailable)
			return
		}
		if err := requireCurrentSchema(ctx, pool); err != nil {
			http.Error(w, "emitlane schema unavailable", http.StatusServiceUnavailable)
			return
		}
		if err := pub.Ping(ctx); err != nil {
			http.Error(w, "kafka unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.Handle("GET /metrics", promhttp.Handler())
	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

func requireCurrentSchema(ctx context.Context, pool *pgxpool.Pool) error {
	version, err := postgres.SchemaVersion(ctx, pool)
	if err != nil {
		return fmt.Errorf("read emitlane schema version: %w", err)
	}
	if version != postgres.CurrentSchemaVersion() {
		return fmt.Errorf("emitlane schema version %d is incompatible with binary version %d; run emitlane migrate up", version, postgres.CurrentSchemaVersion())
	}
	return nil
}
