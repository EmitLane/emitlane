package main

import (
	"context"
	"crypto/sha256"
	"github.com/emitlane/emitlane/broker"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"os"
	"testing"
	"time"
)

// This opts into real, private infrastructure only on the authorized test host.
func TestReleaseFixtureDurabilityAndBrokerRestart(t *testing.T) {
	if os.Getenv("EMITLANE_SOAK_FIXTURE_CHECK") != "1" {
		t.Skip("set EMITLANE_SOAK_FIXTURE_CHECK=1 for owned Docker fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	run := "fixture-" + uuid.NewString()
	e, err := startEnvironment(ctx, run)
	if err != nil {
		t.Fatal(err)
	}
	defer e.close(context.Background())
	if err := e.verifyFixture(ctx); err != nil {
		t.Fatal(err)
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(e.brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	topic := "emitlane-soak-unordered-" + run
	v := newVerifier()
	value := []byte("survives own broker restart")
	send := func() {
		t.Helper()
		id := uuid.New()
		r := &kgo.Record{Topic: topic, Value: value, Headers: []kgo.RecordHeader{{Key: broker.HeaderEventID, Value: []byte(id.String())}}}
		if err := client.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatal(err)
		}
		v.committed(id, expectedEvent{topic: topic, payloadSHA: sha256.Sum256(value), verifyPayload: true})
	}
	send()
	if err := e.kafkaOutage(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := e.kafkaRestart(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	send()
	audit, err := auditKafka(ctx, e.brokers, []string{topic}, v)
	if err != nil {
		t.Fatal(err)
	}
	snap := audit.finalSnapshot()
	if snap.committed != 2 || snap.observed != 2 || snap.records != 2 || snap.lost != 0 || snap.errors != 0 {
		t.Fatalf("audit after restart: %+v", snap)
	}
	t.Logf("SOAK_FIXTURE_PROOF durability=on/on/on shm=%v broker_restart retained_records=%d", e.fixture["postgres_shm_size_bytes"], snap.records)
}

func TestIndependentSoakAuditChecksEveryDuplicatePayloadAndKey(t *testing.T) {
	id := uuid.New()
	payload := []byte("payload")
	v := newVerifier()
	v.committed(id, expectedEvent{ordered: true, topic: "topic", key: "stream", sequence: 1, payloadSHA: sha256.Sum256(payload), verifyPayload: true})
	good := record("topic", id.String(), "stream", 1)
	good.Key = []byte("stream")
	good.Value = payload
	good.Partition = 1
	v.observeForAudit(good)
	bad := *good
	bad.Value = []byte("corrupted")
	v.observeForAudit(&bad)
	if s := v.finalSnapshot(); s.errors != 1 || s.duplicates != 1 {
		t.Fatalf("duplicate hid payload corruption: %+v", s)
	}
	bad = *good
	bad.Key = []byte("other")
	v.observeForAudit(&bad)
	bad = *good
	bad.Partition = 2
	v.observeForAudit(&bad)
	if s := v.finalSnapshot(); s.errors != 3 {
		t.Fatalf("key or partition corruption hidden: %+v", s)
	}
}

func TestReleaseVerdictRequiresEveryCompletedFault(t *testing.T) {
	cfg, _ := profile("release")
	r := Result{Profile: "release", Configuration: cfg, FaultCounts: map[string]int64{}}
	names := []string{"relay_graceful_restart", "relay_crash_takeover", "kafka_outage", "cluster_pause", "relay_membership_change", "kafka_restart"}
	for _, n := range names {
		if verdict(r) == nil {
			t.Fatalf("accepted missing fault %s", n)
		}
		r.FaultCounts[n] = 1
	}
	if err := verdict(r); err != nil {
		t.Fatal(err)
	}
}
