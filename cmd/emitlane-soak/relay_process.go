package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	kafkapub "github.com/emitlane/emitlane/broker/kafka"
	"github.com/emitlane/emitlane/relay"
	pgstore "github.com/emitlane/emitlane/storage/postgres"
)

func relayCommand(args []string) error {
	fs := flag.NewFlagSet("relay", flag.ContinueOnError)
	databaseURL := fs.String("database-url", "", "runner-owned PostgreSQL endpoint")
	brokersText := fs.String("brokers", "", "runner-owned Kafka endpoints")
	instance := fs.String("instance", "", "relay instance id")
	parentPID := fs.Int("parent-pid", 0, "owning soak process")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *databaseURL == "" || *brokersText == "" || *instance == "" {
		return errors.New("relay requires database-url, brokers, and instance")
	}
	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(signalCtx)
	defer cancel()
	if *parentPID > 1 {
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if !processAlive(*parentPID) {
						cancel()
						return
					}
				}
			}
		}()
	}
	pool, err := pgxpool.New(ctx, *databaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	store, err := pgstore.NewStore(pool)
	if err != nil {
		return err
	}
	publisher, err := kafkapub.NewPublisher(kafkapub.Config{Brokers: strings.Split(*brokersText, ","), ClientID: *instance, PublishTimeout: time.Second, AutoCreateTopics: false})
	if err != nil {
		return err
	}
	defer publisher.Close()
	cfg := relay.DefaultConfig()
	cfg.InstanceID = *instance
	cfg.BatchSize = 100
	cfg.Concurrency = 8
	cfg.PollInterval = 50 * time.Millisecond
	cfg.LeaseDuration = 4 * time.Second
	cfg.PublishTimeout = time.Second
	cfg.ShutdownTimeout = 3 * time.Second
	cfg.StatsInterval = 5 * time.Second
	cfg.ControlInterval = 100 * time.Millisecond
	cfg.HeartbeatInterval = 250 * time.Millisecond
	cfg.PresenceStaleAfter = time.Second
	cfg.OrderingRebalanceInterval = 200 * time.Millisecond
	cfg.OrderingLeaseDuration = 4 * time.Second
	cfg.OrderingSafetyMargin = 250 * time.Millisecond
	cfg.BaseDelay = 100 * time.Millisecond
	cfg.MaxDelay = 3 * time.Second
	cfg.Retention = 0
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	rly, err := relay.New(cfg, store, publisher, relay.WithLogger(logger), relay.WithPresenceInfo("local-soak", "v0.3-soak"))
	if err != nil {
		return err
	}
	return rly.Run(ctx)
}

type relayChild struct {
	cmd *exec.Cmd
}

type relayGroup struct {
	mu          sync.Mutex
	runID       string
	databaseURL string
	brokers     string
	next        int
	children    []*relayChild
}

func newRelayGroup(runID, databaseURL string, brokers []string) *relayGroup {
	return &relayGroup{runID: runID, databaseURL: databaseURL, brokers: strings.Join(brokers, ",")}
}

func (g *relayGroup) start() (*relayChild, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.next++
	instance := fmt.Sprintf("soak-%s-relay-%03d", g.runID, g.next)
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(executable, "relay", "--database-url", g.databaseURL, "--brokers", g.brokers, "--instance", instance, "--parent-pid", fmt.Sprint(os.Getpid()))
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	child := &relayChild{cmd: cmd}
	g.children = append(g.children, child)
	return child, nil
}

func (g *relayGroup) count() int { g.mu.Lock(); defer g.mu.Unlock(); return len(g.children) }

func (g *relayGroup) stopIndex(index int, graceful bool) error {
	g.mu.Lock()
	if len(g.children) == 0 {
		g.mu.Unlock()
		return errors.New("no relay children")
	}
	index %= len(g.children)
	child := g.children[index]
	g.children = append(g.children[:index], g.children[index+1:]...)
	g.mu.Unlock()
	if child.cmd.Process == nil {
		return nil
	}
	if graceful {
		_ = child.cmd.Process.Signal(syscall.SIGTERM)
	} else {
		_ = child.cmd.Process.Kill()
	}
	done := make(chan error, 1)
	go func() { done <- child.cmd.Wait() }()
	select {
	case <-done:
		return nil
	case <-time.After(8 * time.Second):
		_ = child.cmd.Process.Kill()
		<-done
		return errors.New("relay stop timed out")
	}
}

func (g *relayGroup) stopAll() {
	g.mu.Lock()
	children := append([]*relayChild(nil), g.children...)
	g.children = nil
	g.mu.Unlock()
	for _, child := range children {
		if child.cmd.Process != nil {
			_ = child.cmd.Process.Signal(syscall.SIGTERM)
		}
	}
	deadline := time.Now().Add(8 * time.Second)
	for _, child := range children {
		done := make(chan struct{})
		go func(c *relayChild) { _ = c.cmd.Wait(); close(done) }(child)
		remaining := time.Until(deadline)
		if remaining < 0 {
			remaining = 0
		}
		select {
		case <-done:
		case <-time.After(remaining):
			_ = child.cmd.Process.Kill()
			<-done
		}
	}
}
