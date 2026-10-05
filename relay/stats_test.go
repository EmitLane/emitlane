package relay

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/emitlane/emitlane/telemetry"
)

type snapshotStore struct {
	*memoryStore
	snapshot Stats
	err      error
}

func (s *snapshotStore) StatsSnapshot(context.Context) (Stats, error) {
	return s.snapshot, s.err
}

func TestStatsFailurePreservesLastSuccessfulSnapshot(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := telemetry.NewMetrics(registry)
	if err != nil {
		t.Fatal(err)
	}
	store := &snapshotStore{memoryStore: &memoryStore{}, snapshot: Stats{Pending: 7, OldestPendingSeconds: 42}}
	r := testRelay(t, store, &trackingPublisher{}, func(cfg *Config) { cfg.StatsInterval = time.Second }, WithMetrics(metrics))
	r.refreshStats(context.Background())
	value := func(name string) float64 {
		t.Helper()
		families, err := registry.Gather()
		if err != nil {
			t.Fatal(err)
		}
		for _, family := range families {
			if family.GetName() == name {
				return family.Metric[0].GetGauge().GetValue()
			}
		}
		t.Fatalf("missing metric %s", name)
		return 0
	}
	lastSuccess := value("emitlane_stats_last_success_timestamp_seconds")
	if lastSuccess <= 0 || value("emitlane_stats_interval_seconds") != r.cfg.StatsInterval.Seconds() {
		t.Fatal("successful snapshot did not declare its freshness and interval")
	}
	store.snapshot = Stats{Pending: 99}
	store.err = errors.New("database unavailable")
	r.refreshStats(context.Background())
	if value("emitlane_pending_events") != 7 || value("emitlane_oldest_pending_seconds") != 42 || value("emitlane_stats_last_success_timestamp_seconds") != lastSuccess {
		t.Fatal("failed snapshot replaced the last successful observation")
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == "emitlane_stats_snapshot_failures_total" && family.Metric[0].GetCounter().GetValue() != 1 {
			t.Fatal("failed snapshot was not counted exactly once")
		}
	}
	store.err = nil
	r.refreshStats(context.Background())
	if value("emitlane_pending_events") != 99 || value("emitlane_stats_last_success_timestamp_seconds") < lastSuccess {
		t.Fatal("snapshot did not recover after the database became available")
	}
}

func TestDisabledStatsIntervalIsObservable(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := telemetry.NewMetrics(registry)
	if err != nil {
		t.Fatal(err)
	}
	testRelay(t, &memoryStore{}, &trackingPublisher{}, func(cfg *Config) { cfg.StatsInterval = 0 }, WithMetrics(metrics))
	metrics.RecordStatsSnapshot(false, time.Time{})
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == "emitlane_stats_interval_seconds" || family.GetName() == "emitlane_stats_last_success_timestamp_seconds" {
			if family.Metric[0].GetGauge().GetValue() != 0 {
				t.Fatalf("disabled stats reported a snapshot: %s", family.GetName())
			}
		}
	}
}
