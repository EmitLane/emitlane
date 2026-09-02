package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

// Listener owns the dedicated PostgreSQL connection used for LISTEN/NOTIFY.
// It is deliberately separate from Store so the relay storage port contains
// only durable state operations.
type Listener struct {
	databaseURL string
	log         *slog.Logger
}

// NewListener constructs a reconnecting notification listener.
func NewListener(databaseURL string, log *slog.Logger) *Listener {
	if log == nil {
		log = slog.Default()
	}
	return &Listener{databaseURL: databaseURL, log: log}
}

// Run listens for event and runtime-control changes using a dedicated connection.
// Notification failure never stops durable delivery; the relay always polls.
func (l *Listener) Run(ctx context.Context, wake chan<- struct{}) {
	backoff := time.Second
	const maxBackoff = 30 * time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		connected, err := l.listenOnce(ctx, wake)
		if ctx.Err() != nil {
			return
		}
		if connected {
			backoff = time.Second
		}
		l.log.Warn("listen connection ended; polling continues", "error", err)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if !connected {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

func (l *Listener) listenOnce(ctx context.Context, wake chan<- struct{}) (bool, error) {
	conn, err := pgx.Connect(ctx, l.databaseURL)
	if err != nil {
		return false, fmt.Errorf("listen connect: %w", err)
	}
	defer closeConnection(ctx, conn)

	if _, err := conn.Exec(ctx, `LISTEN emitlane_events`); err != nil {
		return false, fmt.Errorf("listen: %w", err)
	}
	if _, err := conn.Exec(ctx, `LISTEN emitlane_control`); err != nil {
		return false, fmt.Errorf("listen control: %w", err)
	}
	l.log.Info("listening for emitlane notifications", "channels", []string{notifyChannel, "emitlane_control"})

	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return true, err
		}
		if n == nil {
			continue
		}
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

func closeConnection(ctx context.Context, conn *pgx.Conn) {
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = conn.Close(closeCtx)
}
