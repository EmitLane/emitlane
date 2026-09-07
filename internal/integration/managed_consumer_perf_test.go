//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"

	managed "github.com/emitlane/emitlane/consumer"
	"github.com/emitlane/emitlane/inbox"
)

func TestManagedConsumerPerformanceProfiles(t *testing.T) {
	if os.Getenv("EMITLANE_CONSUMER_PERF") != "1" {
		t.Skip("set EMITLANE_CONSUMER_PERF=1 to run real managed consumer performance profiles")
	}
	e := startEnv(t)
	profiles := []struct {
		name       string
		records    int
		unique     int
		partitions int32
		workers    int
		retryFirst bool
	}{
		{name: "single-partition", records: 400, unique: 400, partitions: 1, workers: 1},
		{name: "many-partitions", records: 800, unique: 800, partitions: 8, workers: 4},
		{name: "duplicate-heavy", records: 800, unique: 80, partitions: 8, workers: 4},
		{name: "retry-heavy", records: 300, unique: 300, partitions: 8, workers: 4, retryFirst: true},
	}
	for _, profile := range profiles {
		profile := profile
		t.Run(profile.name, func(t *testing.T) {
			runManagedConsumerPerformanceProfile(t, e, profile.name, profile.records, profile.unique,
				profile.partitions, profile.workers, profile.retryFirst)
		})
	}
}

func runManagedConsumerPerformanceProfile(
	t *testing.T,
	e *env,
	name string,
	recordCount, uniqueCount int,
	partitions int32,
	workers int,
	retryFirst bool,
) {
	t.Helper()
	topic := "managed-perf-" + name + "-" + uuid.NewString()
	group := "managed-perf-group-" + uuid.NewString()
	consumerName := "managed-perf-" + name + "-" + uuid.NewString()
	e.ensureTopicPartitions(t, topic, partitions)

	ids := make([]uuid.UUID, uniqueCount)
	for index := range ids {
		ids[index] = uuid.New()
	}
	wantOffsets := produceManagedPerformanceRecords(t, e, topic, ids, recordCount, partitions)

	profiler := newConsumerProfiler()
	store := &profiledInboxStore{Store: newManagedInboxStore(t, e), profiler: profiler}
	factory := &profiledSourceFactory{inner: newManagedFactory(t, e, topic, group), profiler: profiler}
	config := managedTestConfig(consumerName, "managed-perf")
	config.Concurrency = workers
	config.BaseDelay, config.MaxDelay, config.Jitter = 5*time.Millisecond, 5*time.Millisecond, 0
	config.MaintenancePoll = 5 * time.Millisecond
	var retryAttempts sync.Map
	handler := func(ctx context.Context, tx pgx.Tx, message managed.Message) error {
		if retryFirst && message.Attempt == 1 {
			retryAttempts.Store(message.EventID, struct{}{})
			return fmt.Errorf("injected performance-profile retry")
		}
		_, err := tx.Exec(ctx, `INSERT INTO public.business_payments (order_id, amount) VALUES ($1, $2)`,
			message.EventID.String(), message.Attempt)
		return err
	}

	started := time.Now()
	run := launchManagedWithStore(t, e, config, store, factory, handler)
	defer run.stop(t)
	waitPerformanceProfile(t, e, group, topic, consumerName, uniqueCount, wantOffsets, 90*time.Second)
	elapsed := time.Since(started)
	result := profiler.snapshot(elapsed, recordCount)

	var effects, attempts int
	if err := e.pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM public.business_payments WHERE order_id=ANY($1)`,
		uuidStrings(ids)).Scan(&effects); err != nil {
		t.Fatal(err)
	}
	if err := e.pool.QueryRow(context.Background(), `SELECT COALESCE(SUM(attempts), 0) FROM emitlane.inbox_events WHERE consumer=$1`,
		consumerName).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if effects != uniqueCount {
		t.Fatalf("protected effects=%d, want %d", effects, uniqueCount)
	}
	if retryFirst && attempts != uniqueCount*2 {
		t.Fatalf("retry-heavy attempts=%d, want %d", attempts, uniqueCount*2)
	}

	t.Logf("consumer_perf profile=%s records=%d unique=%d partitions=%d workers=%d elapsed=%s records_per_second=%.2f processing_ms_p50=%.3f processing_ms_p95=%.3f processing_ms_p99=%.3f db_tx_ms_p50=%.3f db_tx_ms_p95=%.3f db_tx_ms_p99=%.3f offset_commit_ms_p50=%.3f offset_commit_ms_p95=%.3f offset_commit_ms_p99=%.3f inbox_claims=%d inbox_claims_per_second=%.2f inbox_claim_ms_p50=%.3f inbox_claim_ms_p95=%.3f inbox_claim_ms_p99=%.3f",
		name, recordCount, uniqueCount, partitions, workers, elapsed.Round(time.Millisecond), result.recordsPerSecond,
		result.processing.p50, result.processing.p95, result.processing.p99,
		result.databaseTx.p50, result.databaseTx.p95, result.databaseTx.p99,
		result.offsetCommit.p50, result.offsetCommit.p95, result.offsetCommit.p99,
		result.claims, result.claimsPerSecond, result.claim.p50, result.claim.p95, result.claim.p99)
}

func produceManagedPerformanceRecords(t *testing.T, e *env, topic string, ids []uuid.UUID, count int, partitions int32) map[int32]int64 {
	t.Helper()
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(e.brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	results := make(chan error, count)
	wantOffsets := make(map[int32]int64, partitions)
	for index := 0; index < count; index++ {
		identityIndex := index % len(ids)
		partition := int32(identityIndex) % partitions
		wantOffsets[partition]++
		eventID := ids[identityIndex]
		record := &kgo.Record{
			Topic: topic, Partition: partition, Key: []byte(eventID.String()), Value: []byte("performance-profile"),
			Headers: []kgo.RecordHeader{{Key: managed.EventIDHeader, Value: []byte(eventID.String())}},
		}
		producer.Produce(ctx, record, func(_ *kgo.Record, err error) { results <- err })
	}
	for range count {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	return wantOffsets
}

func waitPerformanceProfile(t *testing.T, e *env, group, topic, consumerName string, unique int, offsets map[int32]int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var processed int
		if err := e.pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM emitlane.inbox_events WHERE consumer=$1 AND status='processed'`, consumerName).Scan(&processed); err == nil && processed == unique {
			complete := true
			for partition, want := range offsets {
				if committedOffset(t, e, group, topic, partition) != want {
					complete = false
					break
				}
			}
			if complete {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("performance profile did not quiesce within %s", timeout)
}

func uuidStrings(ids []uuid.UUID) []string {
	values := make([]string, len(ids))
	for index, id := range ids {
		values[index] = id.String()
	}
	return values
}

type profiledInboxStore struct {
	inbox.Store
	profiler *consumerProfiler
}

func (s *profiledInboxStore) Claim(ctx context.Context, request inbox.ClaimRequest) (inbox.ClaimResult, error) {
	started := time.Now()
	result, err := s.Store.Claim(ctx, request)
	finished := time.Now()
	s.profiler.recordClaim(profileRecordKey{topic: request.Source.Topic, partition: request.Source.Partition, offset: request.Source.Offset},
		finished.Sub(started), result.Disposition == inbox.Claimed && err == nil, finished)
	return result, err
}

type profiledSourceFactory struct {
	inner    managed.SourceFactory
	profiler *consumerProfiler
}

func (f *profiledSourceFactory) NewSource(workerID string, listener managed.RebalanceListener) (managed.Source, error) {
	source, err := f.inner.NewSource(workerID, listener)
	if err != nil {
		return nil, err
	}
	return &profiledSource{Source: source, profiler: f.profiler}, nil
}

type profiledSource struct {
	managed.Source
	profiler *consumerProfiler
}

func (s *profiledSource) Poll(ctx context.Context) (managed.SourceRecord, error) {
	record, err := s.Source.Poll(ctx)
	if err == nil {
		s.profiler.recordPoll(record)
	}
	return record, err
}

func (s *profiledSource) Commit(ctx context.Context, record managed.SourceRecord) error {
	started := time.Now()
	err := s.Source.Commit(ctx, record)
	finished := time.Now()
	if err == nil {
		s.profiler.recordCommit(record, finished.Sub(started), finished)
	}
	return err
}

type profileRecordKey struct {
	topic     string
	partition int32
	offset    int64
}

func profileKey(record managed.SourceRecord) profileRecordKey {
	return profileRecordKey{topic: record.Topic, partition: record.Partition, offset: record.Offset}
}

type consumerProfiler struct {
	mu             sync.Mutex
	polls          map[profileRecordKey]time.Time
	claims         map[profileRecordKey]time.Time
	processing     []time.Duration
	databaseTx     []time.Duration
	offsetCommits  []time.Duration
	claimDurations []time.Duration
}

func newConsumerProfiler() *consumerProfiler {
	return &consumerProfiler{polls: make(map[profileRecordKey]time.Time), claims: make(map[profileRecordKey]time.Time)}
}

func (p *consumerProfiler) recordPoll(record managed.SourceRecord) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := profileKey(record)
	if _, exists := p.polls[key]; !exists {
		p.polls[key] = time.Now()
	}
}

func (p *consumerProfiler) recordClaim(key profileRecordKey, duration time.Duration, claimed bool, finished time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.claimDurations = append(p.claimDurations, duration)
	if claimed {
		p.claims[key] = finished
	}
}

func (p *consumerProfiler) recordCommit(record managed.SourceRecord, commitDuration time.Duration, finished time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := profileKey(record)
	if started, ok := p.polls[key]; ok {
		p.processing = append(p.processing, finished.Sub(started))
		delete(p.polls, key)
	}
	if started, ok := p.claims[key]; ok {
		p.databaseTx = append(p.databaseTx, finished.Add(-commitDuration).Sub(started))
		delete(p.claims, key)
	}
	p.offsetCommits = append(p.offsetCommits, commitDuration)
}

type durationPercentiles struct {
	p50 float64
	p95 float64
	p99 float64
}

type consumerProfileResult struct {
	recordsPerSecond float64
	claimsPerSecond  float64
	claims           int
	processing       durationPercentiles
	databaseTx       durationPercentiles
	offsetCommit     durationPercentiles
	claim            durationPercentiles
}

func (p *consumerProfiler) snapshot(elapsed time.Duration, records int) consumerProfileResult {
	p.mu.Lock()
	defer p.mu.Unlock()
	return consumerProfileResult{
		recordsPerSecond: float64(records) / elapsed.Seconds(),
		claimsPerSecond:  float64(len(p.claimDurations)) / elapsed.Seconds(),
		claims:           len(p.claimDurations),
		processing:       percentiles(p.processing),
		databaseTx:       percentiles(p.databaseTx),
		offsetCommit:     percentiles(p.offsetCommits),
		claim:            percentiles(p.claimDurations),
	}
}

func percentiles(values []time.Duration) durationPercentiles {
	if len(values) == 0 {
		return durationPercentiles{}
	}
	ordered := append([]time.Duration(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	at := func(percentile float64) float64 {
		index := int(float64(len(ordered)-1) * percentile)
		return float64(ordered[index]) / float64(time.Millisecond)
	}
	return durationPercentiles{p50: at(0.50), p95: at(0.95), p99: at(0.99)}
}

func launchManagedWithStore(t *testing.T, e *env, config managed.Config, store inbox.Store, factory managed.SourceFactory, handler managed.Handler) *runningManaged {
	t.Helper()
	runtime, err := managed.New(config, e.pool, store, factory, handler, managed.WithLogger(e.log))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	run := &runningManaged{cancel: cancel, done: make(chan error, 1)}
	go func() { run.done <- runtime.Run(ctx) }()
	t.Cleanup(func() { run.stop(t) })
	return run
}
