//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/emitlane/emitlane/broker"
	"github.com/emitlane/emitlane/relay"
	"github.com/google/uuid"
)

func TestQualificationAckWindowAfterProbeRetry(t *testing.T) {
	if os.Getenv("EMITLANE_QUALIFICATION_ACK_CHECK") != "1" {
		t.Skip("opt-in real ACK crash-window selection regression")
	}
	t.Setenv("EMITLANE_QUALIFICATION_FAIL_PROBE_ONCE", "1")
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := e.pool.Exec(ctx, `CREATE TABLE public.soak_expected (
 event_id UUID PRIMARY KEY, sequence BIGINT UNIQUE NOT NULL, stream TEXT NOT NULL,
 stream_sequence BIGINT NOT NULL, poison BOOLEAN NOT NULL, repaired BOOLEAN NOT NULL DEFAULT FALSE
)`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := e.pool.Exec(ctx, `DROP TABLE public.soak_expected`); err != nil {
			t.Error(err)
		}
	})
	output := os.Getenv("EMITLANE_QUALIFICATION_OUTPUT")
	if output == "" {
		output = t.TempDir()
	}
	output = filepath.Join(output, "ack-window-regression")
	if err := os.Mkdir(output, 0700); err != nil {
		t.Fatal(err)
	}
	topic := "qualification-ack-" + uuid.NewString()
	e.ensureTopicPartitions(t, topic, 1)
	pub := e.publisher(t)
	defer pub.Close()
	// Confirm actual broker readiness before the single deliberate probe error.
	if err := pub.Publish(ctx, broker.Message{ID: uuid.NewString(), Destination: topic, Payload: []byte("warmup")}); err != nil {
		t.Fatal(err)
	}
	first := qualificationProbe(t, e, topic, 20261005, time.Now().Add(-time.Minute))
	second := qualificationProbe(t, e, topic, 20261005, time.Now().Add(-time.Minute))
	selected := killQualificationChild(t, e, qualificationConfig{output: output, seed: 20261005}, topic, "unused", "unused", "relay_ack_crash", 0)
	if selected.String() != first && selected.String() != second {
		t.Fatalf("ACK window did not select the ready backlog after probe retry: %s", selected)
	}
	logs, err := filepath.Glob(filepath.Join(output, "relay_ack_crash-*.log"))
	if err != nil || len(logs) != 1 {
		t.Fatalf("helper logs: %v %v", logs, err)
	}
	data, err := os.ReadFile(logs[0])
	if err != nil {
		t.Fatal(err)
	}
	var probe uuid.UUID
	for _, line := range strings.Split(string(data), "\n") {
		var row struct {
			Message string    `json:"msg"`
			ProbeID uuid.UUID `json:"probe_id"`
		}
		if json.Unmarshal([]byte(line), &row) == nil && row.Message == "crash helper reached window" {
			probe = row.ProbeID
		}
	}
	if probe == uuid.Nil || probe == selected || !bytes.Contains(data, []byte("qualification injected first-probe publish failure")) {
		t.Fatalf("missing distinct retried-probe/actual-ACK proof: %s", data)
	}
	var retried bool
	if err := e.pool.QueryRow(ctx, `SELECT status='pending' AND attempts=1 AND last_error='qualification injected first-probe publish failure'
 FROM emitlane.outbox_events WHERE id=$1`, probe).Scan(&retried); err != nil || !retried {
		t.Fatalf("initial probe was not retained for retry: %t %v", retried, err)
	}
	runRelay(t, e.newRelay(t, qualificationRelayConfig("qualification-ack-survivor"), e.publisher(t), relay.FailureHooks{}))
	for _, id := range []string{first, second, probe.String()} {
		e.waitStatus(t, id, "delivered", 20*time.Second)
	}
	records := consumeRecords(t, e.brokers, topic, 5, 15*time.Second)
	duplicates := 0
	for _, record := range records {
		id := headerValue(record, broker.HeaderEventID)
		if id != selected.String() {
			continue
		}
		var payload, key []byte
		if err := e.pool.QueryRow(ctx, `SELECT payload,message_key FROM emitlane.outbox_events WHERE id=$1`, selected).Scan(&payload, &key); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(record.Value, payload) || !bytes.Equal(record.Key, key) {
			t.Fatal("actual ACKed record identity/payload differs")
		}
		duplicates++
	}
	if duplicates != 2 {
		t.Fatalf("ACK-before-DB-ack record did not recover with expected at-least-once duplicate: %d", duplicates)
	}
	t.Logf("QUALIFICATION_ACK_WINDOW_PROOF probe=%s selected=%s distinct=true probe_retry_retained=true real_ACK_SIGKILL=true recovered_records=3 selected_Kafka_records=2 helper_budget=25s", probe, selected)
}
