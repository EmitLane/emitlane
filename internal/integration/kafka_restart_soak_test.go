//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// This opt-in fixture regression checks repeated broker startup and durable
// records. The qualification profile separately checks EmitLane recovery.
func TestKafkaRepeatedRestart(t *testing.T) {
	raw := os.Getenv("EMITLANE_KAFKA_RESTART_CYCLES")
	if raw == "" {
		t.Skip("set EMITLANE_KAFKA_RESTART_CYCLES on an isolated test host")
	}
	cycles, err := strconv.Atoi(raw)
	if err != nil || cycles < 2 || cycles > 100 {
		t.Fatal("EMITLANE_KAFKA_RESTART_CYCLES must be between 2 and 100")
	}
	e := startEnv(t)
	t.Cleanup(func() { e.restoreKafka(t) })
	topic := topicName(t, e)
	client, err := kgo.NewClient(kgo.SeedBrokers(e.brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	publish := func(sequence int) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		record := &kgo.Record{Topic: topic, Key: []byte("restart-proof"), Value: []byte(fmt.Sprintf("record-%d", sequence))}
		if err := client.ProduceSync(ctx, record).FirstErr(); err != nil {
			t.Fatalf("publish after restart %d: %v", sequence, err)
		}
	}
	publish(0)
	for cycle := 1; cycle <= cycles; cycle++ {
		e.stopKafka(t)
		time.Sleep(2 * time.Second)
		e.startKafka(t)
		// Read every earlier acknowledgement after restart, then prove that
		// this same container accepts and retains a new record as well.
		publish(cycle)
		records := consumeRecords(t, e.brokers, topic, cycle+1, 30*time.Second)
		for sequence, record := range records {
			if string(record.Key) != "restart-proof" || string(record.Value) != fmt.Sprintf("record-%d", sequence) {
				t.Fatalf("restart %d: record %d changed: key=%q value=%q", cycle, sequence, record.Key, record.Value)
			}
		}
		t.Logf("restart %d/%d: %d acknowledged records retained", cycle, cycles, len(records))
	}
}
