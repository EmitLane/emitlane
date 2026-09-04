package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/emitlane/emitlane/broker"
)

func TestProfiles(t *testing.T) {
	for _, name := range []string{"quick", "standard", "release"} {
		cfg, err := profile(name)
		if err != nil {
			t.Fatalf("profile(%s): %v", name, err)
		}
		if cfg.Profile != name || cfg.Relays < 2 || cfg.OrderedStreams < 50 || cfg.OrderedPercent != 80 || !cfg.Faults {
			t.Fatalf("bad %s profile: %+v", name, cfg)
		}
		if cfg.RelayMaxAttempts != 100 || cfg.RelayBaseDelay != 500*time.Millisecond || cfg.RelayMaxDelay != 5*time.Second {
			t.Fatalf("unsafe transient-fault retry policy for %s: attempts=%d base=%s max=%s", name, cfg.RelayMaxAttempts, cfg.RelayBaseDelay, cfg.RelayMaxDelay)
		}
		if err := cfg.validate(); err != nil {
			t.Fatalf("validate %s: %v", name, err)
		}
	}
	if _, err := profile("production"); err == nil {
		t.Fatal("unknown profile accepted")
	}
	standard, _ := profile("standard")
	if got := standard.Duration.Seconds() * float64(standard.EventsPerSecond); got <= 100000 {
		t.Fatalf("standard target %.0f must exceed 100k", got)
	}
}

func TestSeedReproducibility(t *testing.T) {
	a, b := seededRand(42), seededRand(42)
	for range 100 {
		if a.Uint64() != b.Uint64() {
			t.Fatal("same seed diverged")
		}
	}
}

func record(topic, id, key string, sequence int64) *kgo.Record {
	headers := []kgo.RecordHeader{{Key: broker.HeaderEventID, Value: []byte(id)}}
	if key != "" {
		headers = append(headers, kgo.RecordHeader{Key: broker.HeaderOrderingKey, Value: []byte(key)}, kgo.RecordHeader{Key: broker.HeaderSequence, Value: []byte(strings.TrimSpace(intText(sequence)))})
	}
	return &kgo.Record{Topic: topic, Headers: headers}
}

func intText(value int64) string {
	if value == 0 {
		return "0"
	}
	negative := value < 0
	if negative {
		value = -value
	}
	var buf [20]byte
	i := len(buf)
	for value > 0 {
		i--
		buf[i] = byte('0' + value%10)
		value /= 10
	}
	if negative {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func TestVerifierAllowsAdjacentDuplicatesAndDetectsRegression(t *testing.T) {
	v := newVerifier()
	now := time.Now()
	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	for i, id := range ids {
		v.committed(id, expectedEvent{ordered: true, stream: "topic/order:1", sequence: int64(i + 1), committedAt: now})
	}
	v.observe(record("topic", ids[0].String(), "order:1", 1), now)
	v.observe(record("topic", ids[0].String(), "order:1", 1), now)
	v.observe(record("topic", ids[1].String(), "order:1", 2), now)
	snap := v.snapshot()
	if snap.duplicates != 1 || snap.regressions != 0 || snap.lost != 2 {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}
	v.observe(record("topic", ids[2].String(), "order:1", 1), now)
	if got := v.snapshot().regressions; got != 1 {
		t.Fatalf("regressions=%d", got)
	}
}

func TestVerifierDetectsSkipAndLostEvents(t *testing.T) {
	v := newVerifier()
	now := time.Now()
	first, third, lost := uuid.New(), uuid.New(), uuid.New()
	for _, item := range []struct {
		id       uuid.UUID
		sequence int64
	}{{first, 1}, {third, 3}, {lost, 2}} {
		v.committed(item.id, expectedEvent{ordered: true, committedAt: now, sequence: item.sequence})
	}
	v.observe(record("topic", first.String(), "stream", 1), now)
	v.observe(record("topic", third.String(), "stream", 3), now)
	s := v.snapshot()
	if s.skips != 1 || s.lost != 1 || s.observed != 2 {
		t.Fatalf("snapshot=%+v", s)
	}
}

func TestVerdict(t *testing.T) {
	if err := verdict(Result{}); err != nil {
		t.Fatal(err)
	}
	if err := verdict(Result{LostEvents: 1}); err == nil || !strings.Contains(err.Error(), "lost") {
		t.Fatalf("verdict=%v", err)
	}
	if err := verdict(Result{InfrastructureErrors: 2}); err == nil || !strings.Contains(err.Error(), "infrastructure") {
		t.Fatalf("verdict=%v", err)
	}
}

func TestReportGenerationDeterministic(t *testing.T) {
	r := Result{RunID: "run-1", Profile: "release", Seed: 7, GitCommit: "abc", OS: "darwin", Arch: "arm64", DurationSeconds: 60, CommittedEvents: 2, ObservedUniqueEvents: 2, BrokerRecords: 3, DuplicateRecords: 1}
	a, b := reportMarkdown(r), reportMarkdown(r)
	if a != b {
		t.Fatal("report is not deterministic")
	}
	for _, want := range []string{"**Result: PASS**", "timeline.svg", "Committed events | 2", "At-least-once duplicates | 1", "PASS: all committed event IDs"} {
		if !strings.Contains(a, want) {
			t.Fatalf("missing %q in report", want)
		}
	}
}

func TestTimelineSVG(t *testing.T) {
	points := []timelinePoint{
		{Elapsed: 0, Phase: "warmup"},
		{Elapsed: 5 * time.Second, Phase: "running", Committed: 100, Observed: 80, Backlog: 20, KafkaOutages: 1},
		{Elapsed: 10 * time.Second, Phase: "recovering", Committed: 100, Observed: 100},
	}
	a, b := timelineSVG("run<&>", points), timelineSVG("run<&>", points)
	if a != b {
		t.Fatal("timeline is not deterministic")
	}
	for _, want := range []string{"<svg", "Committed", "Observed", "Backlog", ">K</text>", "run&lt;&amp;&gt;"} {
		if !strings.Contains(a, want) {
			t.Fatalf("timeline missing %q", want)
		}
	}
	if strings.Contains(a, "NaN") {
		t.Fatal("timeline contains NaN")
	}
}

func TestStateAndProgressSerialization(t *testing.T) {
	dir := t.TempDir()
	state := State{RunID: "r", State: "running", Phase: "warmup", UpdatedAt: time.Unix(1, 0).UTC()}
	if err := writeJSON(filepath.Join(dir, "state.json"), state); err != nil {
		t.Fatal(err)
	}
	var decoded State
	if err := readJSON(filepath.Join(dir, "state.json"), &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(state, decoded) {
		t.Fatalf("decoded=%+v want=%+v", decoded, state)
	}
	progress := Progress{RunID: "r", Elapsed: 5 * time.Second, NotObservedYet: 4}
	data, err := json.Marshal(progress)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"not_observed_yet":4`) {
		t.Fatal(string(data))
	}
}

func TestCurrentRunHandling(t *testing.T) {
	root := t.TempDir()
	if err := writeCurrent(root, "20260904-120501-a1b2c3"); err != nil {
		t.Fatal(err)
	}
	runDir, err := currentRun(root)
	if err != nil {
		t.Fatal(err)
	}
	if got := filepath.Base(runDir); got != "20260904-120501-a1b2c3" {
		t.Fatalf("run=%s", got)
	}
	if err := os.WriteFile(filepath.Join(root, "current"), []byte("../escape\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := currentRun(root); err == nil {
		t.Fatal("unsafe current run accepted")
	}
}
