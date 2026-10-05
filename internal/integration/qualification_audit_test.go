//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/emitlane/emitlane/broker"
	managed "github.com/emitlane/emitlane/consumer"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type qualificationOrderAudit struct {
	sequence  map[string]int64
	partition map[string]int32
}

func (a *qualificationOrderAudit) check(record *kgo.Record, p qualificationPayload) error {
	if p.Stream == "" {
		if p.StreamSequence != 0 {
			return fmt.Errorf("unordered record has sequence")
		}
		return nil
	}
	if a.sequence == nil {
		a.sequence = make(map[string]int64)
		a.partition = make(map[string]int32)
	}
	if len(a.sequence) >= 8 {
		if _, ok := a.sequence[p.Stream]; !ok {
			return fmt.Errorf("unexpected ordered stream %q", p.Stream)
		}
	}
	if headerValueNoTest(record, broker.HeaderOrderingKey) != p.Stream || headerValueNoTest(record, broker.HeaderSequence) != strconv.FormatInt(p.StreamSequence, 10) {
		return fmt.Errorf("ordering header/payload mismatch")
	}
	last := a.sequence[p.Stream]
	if p.StreamSequence < last || p.StreamSequence > last+1 || p.StreamSequence < 1 {
		return fmt.Errorf("stream %s regressed/skipped from %d to %d", p.Stream, last, p.StreamSequence)
	}
	if partition, ok := a.partition[p.Stream]; ok && partition != record.Partition {
		return fmt.Errorf("stream %s changed Kafka partition", p.Stream)
	}
	a.partition[p.Stream] = record.Partition
	a.sequence[p.Stream] = p.StreamSequence
	return nil
}
func headerValueNoTest(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// Stream Kafka in bounded batches. Expected/seen IDs live in PostgreSQL, not
// an ever-growing Go map or slice. Audit all snapshot offsets, including any
// unexpected trailing records, rather than stopping at matching counts.
func auditQualificationKafka(t *testing.T, ctx context.Context, e *env, topic string, seed int64) (int64, int64) {
	t.Helper()
	meta, err := kgo.NewClient(kgo.SeedBrokers(e.brokers...))
	if err != nil {
		t.Fatal(err)
	}
	ends, err := kadm.NewClient(meta).ListEndOffsets(ctx, topic)
	meta.Close()
	if err != nil {
		t.Fatal(err)
	}
	starts := make(map[int32]kgo.Offset)
	targets := make(map[int32]int64)
	for partition, end := range ends[topic] {
		if end.Err != nil {
			t.Fatal(end.Err)
		}
		starts[partition] = kgo.NewOffset().AtStart()
		targets[partition] = end.Offset
	}
	if len(targets) != 8 {
		t.Fatalf("audit expected eight partitions, got %d", len(targets))
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(e.brokers...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: starts}), kgo.FetchMaxBytes(1<<20), kgo.FetchMaxWait(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	positions := make(map[int32]int64)
	var count int64
	order := qualificationOrderAudit{}
	for {
		complete := true
		for partition, target := range targets {
			if positions[partition] < target {
				complete = false
			}
		}
		if complete {
			break
		}
		fetches := client.PollRecords(ctx, 250)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("qualification Kafka snapshot: %v", errs[0])
		}
		if ctx.Err() != nil {
			t.Fatalf("qualification Kafka audit: %v", ctx.Err())
		}
		batch := fetches.Records()
		if len(batch) == 0 {
			continue
		}
		if err := auditQualificationBatch(ctx, e, &order, batch, seed); err != nil {
			t.Fatal(err)
		}
		for _, record := range batch {
			if record.Offset >= targets[record.Partition] {
				continue
			}
			positions[record.Partition] = record.Offset + 1
			count++
		}
	}
	var unique int64
	if err := e.pool.QueryRow(ctx, `SELECT COUNT(*) FROM public.soak_observed`).Scan(&unique); err != nil {
		t.Fatal(err)
	}
	return count, count - unique
}

func auditQualificationBatch(ctx context.Context, e *env, order *qualificationOrderAudit, records []*kgo.Record, seed int64) error {
	ids := make([]string, 0, len(records))
	payloads := make([]qualificationPayload, 0, len(records))
	for _, record := range records {
		var p qualificationPayload
		if err := json.Unmarshal(record.Value, &p); err != nil {
			return err
		}
		m := managed.Message{}
		for _, h := range record.Headers {
			m.Headers = append(m.Headers, managed.Header{Key: h.Key, Value: h.Value})
		}
		id, err := managed.ResolveEventID(m)
		if err != nil {
			return err
		}
		ids = append(ids, id.String())
		payloads = append(payloads, p)
	}
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	rows, err := tx.Query(ctx, `SELECT event_id::text,sequence,stream,stream_sequence,poison FROM public.soak_expected WHERE event_id=ANY($1::uuid[])`, ids)
	if err != nil {
		return err
	}
	expected := make(map[string]qualificationPayload, len(records))
	for rows.Next() {
		var id string
		var p qualificationPayload
		p.Seed = seed
		if err := rows.Scan(&id, &p.Sequence, &p.Stream, &p.StreamSequence, &p.Poison); err != nil {
			rows.Close()
			return err
		}
		expected[id] = p
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for i, record := range records {
		want, ok := expected[ids[i]]
		if !ok {
			return fmt.Errorf("unexpected Kafka ID %s", ids[i])
		}
		key := ids[i]
		if want.Stream != "" {
			key = want.Stream
		}
		if payloads[i] != want || string(record.Key) != key {
			return fmt.Errorf("Kafka payload/key mismatch for %s", ids[i])
		}
		if err := order.check(record, payloads[i]); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO public.soak_observed(event_id,records)
 SELECT id,COUNT(*) FROM unnest($1::uuid[]) AS id GROUP BY id
 ON CONFLICT(event_id) DO UPDATE SET records=soak_observed.records+EXCLUDED.records`, ids); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func auditQualificationDatabase(t *testing.T, ctx context.Context, e *env, topic, name string) int64 {
	t.Helper()
	var mismatch, total, audits, repairs, poisons int64
	// FULL JOIN detects substitutions/extra rows as well as missing rows. The
	// business transaction is authoritative even after delivered retention.
	if err := e.pool.QueryRow(ctx, `
SELECT COUNT(*) FILTER(WHERE x.event_id IS NULL OR b.id IS NULL OR b.amount IS DISTINCT FROM x.sequence
 OR i.status IS DISTINCT FROM 'processed' OR p.amount IS DISTINCT FROM x.sequence OR a.event_id IS NULL
 OR (o.id IS NULL AND r.event_id IS NULL) OR (o.id IS NOT NULL AND o.status <> 'delivered')),
 COUNT(x.event_id)
FROM public.soak_expected x
FULL JOIN public.business_orders b ON b.id='soak-'||x.event_id::text
FULL JOIN emitlane.inbox_events i ON i.consumer=$2 AND i.event_id=x.event_id
FULL JOIN public.business_payments p ON p.order_id=x.event_id::text
FULL JOIN public.soak_observed a ON a.event_id=x.event_id
FULL JOIN (SELECT id,status FROM emitlane.outbox_events WHERE destination=$1) o ON o.id=x.event_id
FULL JOIN public.soak_pruned r ON r.event_id=x.event_id`, topic, name).Scan(&mismatch, &total); err != nil {
		t.Fatal(err)
	}
	if total == 0 || mismatch != 0 {
		t.Fatalf("database audit: committed=%d identity/effect mismatches=%d", total, mismatch)
	}
	if err := e.pool.QueryRow(ctx, `SELECT COUNT(*) FROM emitlane.admin_audit_log WHERE action='inbox.retry' AND actor='qualification' AND reason='poison input repaired'`).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if err := e.pool.QueryRow(ctx, `SELECT COUNT(*) FILTER(WHERE repaired),COUNT(*) FILTER(WHERE poison) FROM public.soak_expected`).Scan(&repairs, &poisons); err != nil {
		t.Fatal(err)
	}
	if poisons == 0 || repairs != poisons || audits != repairs {
		t.Fatalf("operator audit: poisons=%d repairs=%d audit rows=%d", poisons, repairs, audits)
	}
	var streamMismatch int64
	if err := e.pool.QueryRow(ctx, `SELECT COUNT(*) FROM public.soak_stream_progress s JOIN public.soak_effect_progress e USING(stream) WHERE s.sequence<>e.sequence`).Scan(&streamMismatch); err != nil {
		t.Fatal(err)
	}
	if streamMismatch != 0 {
		t.Fatalf("%d business streams did not reach their committed sequence", streamMismatch)
	}
	return total
}

func TestQualificationOrderAuditRejectsRegressionGapAndPartitionChange(t *testing.T) {
	record := func(sequence int64, partition int32) *kgo.Record {
		return &kgo.Record{Partition: partition, Headers: []kgo.RecordHeader{
			{Key: broker.HeaderOrderingKey, Value: []byte("stream-0")}, {Key: broker.HeaderSequence, Value: []byte(strconv.FormatInt(sequence, 10))},
		}}
	}
	for _, tc := range []struct {
		name      string
		sequence  int64
		partition int32
	}{{"regression", 1, 0}, {"gap", 4, 0}, {"partition", 3, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			a := qualificationOrderAudit{}
			for _, seq := range []int64{1, 1, 2} {
				if err := a.check(record(seq, 0), qualificationPayload{Stream: "stream-0", StreamSequence: seq}); err != nil {
					t.Fatal(err)
				}
			}
			if err := a.check(record(tc.sequence, tc.partition), qualificationPayload{Stream: "stream-0", StreamSequence: tc.sequence}); err == nil {
				t.Fatal("corrupt ordered stream accepted")
			}
		})
	}
}
