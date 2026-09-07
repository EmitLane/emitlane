package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	kafkaadapter "github.com/emitlane/emitlane/broker/kafka"
	managed "github.com/emitlane/emitlane/consumer"
	"github.com/emitlane/emitlane/outbox"
	pgstore "github.com/emitlane/emitlane/storage/postgres"
)

type orderCreated struct {
	OrderID string `json:"order_id"`
	Amount  int    `json:"amount"`
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	dbURL := envOr("DATABASE_URL", "postgres://emitlane:emitlane@localhost:5432/emitlane?sslmode=disable")
	brokers := envOr("KAFKA_BROKERS", "localhost:19092")
	httpAddr := envOr("HTTP_ADDR", ":8081")
	topic := envOr("ORDERS_TOPIC", "orders.events")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Error("connect postgres", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	if err := setup(ctx, pool); err != nil {
		log.Error("setup schema", "error", err)
		os.Exit(1)
	}

	writer := outbox.NewWriter()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		var req struct {
			Amount int `json:"amount"`
		}
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&req); err != nil || req.Amount <= 0 {
			http.Error(w, "amount is required", http.StatusBadRequest)
			return
		}
		orderID := uuid.NewString()
		payload, err := outbox.JSON(orderCreated{OrderID: orderID, Amount: req.Amount})
		if err != nil {
			internalError(log, w, "encode order event", err)
			return
		}
		tx, err := pool.Begin(r.Context())
		if err != nil {
			internalError(log, w, "begin order transaction", err)
			return
		}
		defer rollbackTx(r.Context(), log, tx)
		if _, err := tx.Exec(r.Context(), `INSERT INTO public.orders (id, amount) VALUES ($1, $2)`, orderID, req.Amount); err != nil {
			internalError(log, w, "insert order", err)
			return
		}
		if _, err := writer.Enqueue(r.Context(), tx, outbox.Event{
			Destination: topic,
			Type:        "order.created",
			Payload:     payload,
			ContentType: "application/json",
			OrderingKey: "order:" + orderID,
			Sequence:    1,
		}); err != nil {
			internalError(log, w, "enqueue order event", err)
			return
		}
		if err := tx.Commit(r.Context()); err != nil {
			internalError(log, w, "commit order transaction", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": orderID, "amount": req.Amount})
	})
	mux.HandleFunc("POST /orders/{id}/paid", func(w http.ResponseWriter, r *http.Request) {
		orderID := r.PathValue("id")
		tx, err := pool.Begin(r.Context())
		if err != nil {
			internalError(log, w, "begin payment transaction", err)
			return
		}
		defer rollbackTx(r.Context(), log, tx)
		var amount int
		var version int64
		err = tx.QueryRow(r.Context(), `
UPDATE public.orders
SET status='paid', version=version+1
WHERE id=$1 AND status='created'
RETURNING amount, version`, orderID).Scan(&amount, &version)
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "order not found or already paid", http.StatusConflict)
			return
		}
		if err != nil {
			internalError(log, w, "mark order paid", err)
			return
		}
		payload, err := outbox.JSON(orderCreated{OrderID: orderID, Amount: amount})
		if err != nil {
			internalError(log, w, "encode paid event", err)
			return
		}
		if _, err := writer.Enqueue(r.Context(), tx, outbox.Event{
			Destination: topic,
			Type:        "order.paid",
			Payload:     payload,
			ContentType: "application/json",
			OrderingKey: "order:" + orderID,
			Sequence:    version,
		}); err != nil {
			internalError(log, w, "enqueue paid event", err)
			return
		}
		if err := tx.Commit(r.Context()); err != nil {
			internalError(log, w, "commit payment transaction", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"id": orderID, "status": "paid", "version": version})
	})
	mux.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		var amount int
		var status string
		var version int64
		err := pool.QueryRow(r.Context(), `SELECT amount, status, version FROM public.orders WHERE id = $1`, id).Scan(&amount, &status, &version)
		if errors.Is(err, pgx.ErrNoRows) {
			http.NotFound(w, r)
			return
		}
		if err != nil {
			internalError(log, w, "read order", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"id": id, "amount": amount, "status": status, "version": version})
	})
	mux.HandleFunc("GET /payments/{order_id}", func(w http.ResponseWriter, r *http.Request) {
		orderID := r.PathValue("order_id")
		var amount int
		err := pool.QueryRow(r.Context(), `SELECT amount FROM public.payments WHERE order_id = $1`, orderID).Scan(&amount)
		if errors.Is(err, pgx.ErrNoRows) {
			http.NotFound(w, r)
			return
		}
		if err != nil {
			internalError(log, w, "read payment", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"order_id": orderID, "amount": amount})
	})

	srv := &http.Server{
		Addr:              httpAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		log.Info("ecommerce example listening", "addr", httpAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http", "error", err)
		}
	}()
	go consumePayments(ctx, log, pool, brokers, topic)

	<-ctx.Done()
	shctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shctx)
}

func internalError(log *slog.Logger, w http.ResponseWriter, operation string, err error) {
	log.Error(operation, "error", err)
	http.Error(w, "internal server error", http.StatusInternalServerError)
}

func rollbackTx(ctx context.Context, log *slog.Logger, tx pgx.Tx) {
	rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := tx.Rollback(rollbackCtx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		log.Warn("rollback transaction", "error", err)
	}
}

func consumePayments(ctx context.Context, log *slog.Logger, pool *pgxpool.Pool, brokers, topic string) {
	const consumerName = "ecommerce-payments"
	store, err := pgstore.NewInboxStore(pool)
	if err != nil {
		log.Error("create managed Inbox store", "error", err)
		return
	}
	factory, err := kafkaadapter.NewConsumerFactory(kafkaadapter.ConsumerConfig{
		Brokers: splitCSV(brokers), Group: consumerName, Topics: []string{topic},
		SessionTimeout: 10 * time.Second, RebalanceTimeout: 30 * time.Second,
		FetchMaxWait: 250 * time.Millisecond,
	})
	if err != nil {
		log.Error("create Kafka consumer source", "error", err)
		return
	}
	config := managed.DefaultConfig()
	config.Consumer = consumerName
	config.InstanceID = "ecommerce-" + uuid.NewString()
	runtime, err := managed.New(config, pool, store, factory, handlePayment, managed.WithLogger(log))
	if err != nil {
		log.Error("create managed consumer", "error", err)
		return
	}
	if err := runtime.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("managed consumer stopped", "error", err)
	}
}

func handlePayment(ctx context.Context, tx pgx.Tx, message managed.Message) error {
	var body orderCreated
	if err := json.Unmarshal(message.Payload, &body); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `
INSERT INTO public.payments (order_id, amount)
VALUES ($1, $2)
ON CONFLICT (order_id) DO NOTHING`, body.OrderID, body.Amount)
	return err
}

func setup(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
CREATE TABLE IF NOT EXISTS public.orders (
    id TEXT PRIMARY KEY,
    amount INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'created',
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE public.orders ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'created';
ALTER TABLE public.orders ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 1;
CREATE TABLE IF NOT EXISTS public.payments (
    order_id TEXT PRIMARY KEY,
    amount INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);`)
	return err
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
