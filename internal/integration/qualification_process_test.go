//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	kafkaadapter "github.com/emitlane/emitlane/broker/kafka"
	managed "github.com/emitlane/emitlane/consumer"
	"github.com/emitlane/emitlane/relay"
	pgstore "github.com/emitlane/emitlane/storage/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func killQualificationChild(t *testing.T, e *env, c qualificationConfig, topic, group, name, mode string, cycle int) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	signalPath := filepath.Join(t.TempDir(), "ready")
	logPath := filepath.Join(c.output, mode+"-"+time.Now().UTC().Format("20060102T150405.000000000")+".log")
	log, err := os.OpenFile(logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	cmd := exec.Command(executable, "-test.run=^TestQualificationProcessHelper$", "-test.timeout=40s")
	cmd.Env = append(os.Environ(), "EMITLANE_QUALIFICATION_HELPER="+mode, "EMITLANE_QUALIFICATION_DATABASE="+e.databaseURL,
		"EMITLANE_QUALIFICATION_BROKERS="+strings.Join(e.brokers, ","), "EMITLANE_QUALIFICATION_TOPIC="+topic,
		"EMITLANE_QUALIFICATION_GROUP="+group, "EMITLANE_QUALIFICATION_CONSUMER="+name, "EMITLANE_QUALIFICATION_SIGNAL="+signalPath)
	cmd.Stdout = log
	cmd.Stderr = log
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	waited := false
	defer func() {
		if !waited {
			_ = cmd.Process.Kill()
			<-done
		}
	}()
	deadline := time.Now().Add(25 * time.Second)
	for {
		if _, err := os.Stat(signalPath); err == nil {
			break
		}
		select {
		case err := <-done:
			waited = true
			t.Fatalf("%s helper exited before crash window: %v (see %s)", mode, err, logPath)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s helper missed crash window in cycle %d (see %s)", mode, cycle+1, logPath)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	err = <-done
	waited = true
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != -1 {
		t.Fatalf("%s helper was not killed: %v", mode, err)
	}
}

// Spawned using this test binary: the parent kills us only after the ready
// marker proves we are inside the requested transaction/ACK/claim window.
func TestQualificationProcessHelper(t *testing.T) {
	mode := os.Getenv("EMITLANE_QUALIFICATION_HELPER")
	if mode == "" {
		t.Skip("subprocess helper")
	}
	ctx := context.Background()
	poolConfig, err := pgxpool.ParseConfig(os.Getenv("EMITLANE_QUALIFICATION_DATABASE"))
	if err != nil {
		t.Fatal(err)
	}
	poolConfig.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ready := func() {
		if err := os.WriteFile(os.Getenv("EMITLANE_QUALIFICATION_SIGNAL"), []byte(mode), 0600); err != nil {
			t.Fatal(err)
		}
		select {}
	}
	brokers := strings.Split(os.Getenv("EMITLANE_QUALIFICATION_BROKERS"), ",")
	if mode == "consumer" {
		store, err := pgstore.NewInboxStore(pool)
		if err != nil {
			t.Fatal(err)
		}
		factory, err := kafkaadapter.NewConsumerFactory(kafkaadapter.ConsumerConfig{Brokers: brokers,
			Group: os.Getenv("EMITLANE_QUALIFICATION_GROUP"), Topics: []string{os.Getenv("EMITLANE_QUALIFICATION_TOPIC")},
			SessionTimeout: 6 * time.Second, RebalanceTimeout: 10 * time.Second, FetchMaxWait: 50 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		c := qualificationConsumerConfig(os.Getenv("EMITLANE_QUALIFICATION_CONSUMER"), "qualification-crash-child")
		c.Concurrency = 1
		r, err := managed.New(c, pool, store, factory, func(ctx context.Context, tx pgx.Tx, m managed.Message) error {
			var p qualificationPayload
			if err := json.Unmarshal(m.Payload, &p); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO public.business_payments(order_id,amount) VALUES($1,$2)`, m.EventID.String(), p.Sequence); err != nil {
				return err
			}
			ready()
			return nil
		}, managed.WithLogger(logger))
		if err != nil {
			t.Fatal(err)
		}
		if err := r.Run(ctx); err != nil {
			t.Fatal(err)
		}
		return
	}
	store, err := pgstore.NewStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	pub, err := kafkaadapter.NewPublisher(kafkaadapter.Config{Brokers: brokers, ClientID: "qualification-crash-child", PublishTimeout: 500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	hooks := relay.FailureHooks{}
	switch mode {
	case "relay_claim_crash":
		hooks.AfterClaimCommit = func(context.Context, relay.Event) error { ready(); return nil }
	case "relay_ack_crash":
		hooks.AfterPublishAck = func(context.Context, relay.Event) error { ready(); return nil }
	default:
		t.Fatalf("unknown qualification helper mode %q", mode)
	}
	c := qualificationRelayConfig("qualification-crash-child")
	c.Concurrency = 1
	r, err := relay.New(c, store, pub, relay.WithLogger(logger), relay.WithFailureHooks(hooks))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(ctx); err != nil {
		t.Fatal(err)
	}
}
