package main

import (
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/emitlane/emitlane/broker"
)

type expectedEvent struct {
	ordered     bool
	stream      string
	sequence    int64
	committedAt time.Time
}

type verifier struct {
	mu                 sync.Mutex
	expected           map[uuid.UUID]expectedEvent
	observed           map[uuid.UUID]time.Time
	lastSequence       map[string]int64
	latencies          []float64
	orderedCommitted   int64
	unorderedCommitted int64
	brokerRecords      int64
	regressions        int64
	skips              int64
	errors             int64
	diagnostics        []Diagnostic
}

func newVerifier() *verifier {
	return &verifier{expected: make(map[uuid.UUID]expectedEvent), observed: make(map[uuid.UUID]time.Time), lastSequence: make(map[string]int64)}
}

func (v *verifier) committed(id uuid.UUID, meta expectedEvent) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.expected[id] = meta
	if meta.ordered {
		v.orderedCommitted++
	} else {
		v.unorderedCommitted++
	}
	if at, ok := v.observed[id]; ok {
		v.latencies = append(v.latencies, float64(at.Sub(meta.committedAt).Microseconds())/1000)
	}
}

func header(record *kgo.Record, key string) string {
	for _, h := range record.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (v *verifier) observe(record *kgo.Record, now time.Time) {
	id, err := uuid.Parse(header(record, broker.HeaderEventID))
	if err != nil {
		v.mu.Lock()
		defer v.mu.Unlock()
		v.brokerRecords++
		v.errors++
		v.addDiagnostic(Diagnostic{Kind: "invalid_event_id", Detail: err.Error()})
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.brokerRecords++
	if _, duplicate := v.observed[id]; !duplicate {
		v.observed[id] = now
		if meta, ok := v.expected[id]; ok {
			v.latencies = append(v.latencies, float64(now.Sub(meta.committedAt).Microseconds())/1000)
		}
	}
	orderingKey := header(record, broker.HeaderOrderingKey)
	if orderingKey == "" {
		return
	}
	sequence, err := strconv.ParseInt(header(record, broker.HeaderSequence), 10, 64)
	if err != nil {
		v.errors++
		v.addDiagnostic(Diagnostic{Kind: "invalid_sequence", EventID: id.String(), Detail: err.Error()})
		return
	}
	stream := record.Topic + "/" + orderingKey
	previous := v.lastSequence[stream]
	if previous != 0 && sequence < previous {
		v.regressions++
		v.addDiagnostic(Diagnostic{Kind: "ordering_regression", EventID: id.String(), Stream: stream, PreviousSequence: previous, ObservedSequence: sequence})
	} else if sequence > previous+1 {
		v.skips++
		v.addDiagnostic(Diagnostic{Kind: "ordering_skip", EventID: id.String(), Stream: stream, PreviousSequence: previous, ObservedSequence: sequence})
	}
	if sequence > previous {
		v.lastSequence[stream] = sequence
	}
}

func (v *verifier) addDiagnostic(d Diagnostic) {
	if len(v.diagnostics) < 100 {
		v.diagnostics = append(v.diagnostics, d)
	}
}

type verifierSnapshot struct {
	committed, ordered, unordered, observed, records, duplicates, lost, regressions, skips, errors int64
	latencies                                                                                      []float64
	diagnostics                                                                                    []Diagnostic
}

func (v *verifier) snapshot() verifierSnapshot {
	return v.snapshotDetails(false)
}

func (v *verifier) finalSnapshot() verifierSnapshot {
	return v.snapshotDetails(true)
}

func (v *verifier) snapshotDetails(includeLostDiagnostics bool) verifierSnapshot {
	v.mu.Lock()
	defer v.mu.Unlock()
	committed := int64(len(v.expected))
	observedExpected := int64(0)
	diagnostics := append([]Diagnostic(nil), v.diagnostics...)
	type missingEvent struct {
		id   uuid.UUID
		meta expectedEvent
	}
	var missing []missingEvent
	for id, meta := range v.expected {
		if _, ok := v.observed[id]; ok {
			observedExpected++
		} else if includeLostDiagnostics {
			missing = append(missing, missingEvent{id: id, meta: meta})
		}
	}
	if includeLostDiagnostics {
		sort.Slice(missing, func(i, j int) bool { return missing[i].id.String() < missing[j].id.String() })
		for _, event := range missing {
			if len(diagnostics) >= 100 {
				break
			}
			diagnostics = append(diagnostics, Diagnostic{Kind: "lost_event", EventID: event.id.String(), Stream: event.meta.stream, ObservedSequence: event.meta.sequence})
		}
	}
	unexpected := int64(0)
	for id := range v.observed {
		if _, ok := v.expected[id]; !ok {
			unexpected++
		}
	}
	return verifierSnapshot{
		committed: committed, ordered: v.orderedCommitted, unordered: v.unorderedCommitted,
		observed: observedExpected, records: v.brokerRecords,
		duplicates: max(int64(0), v.brokerRecords-int64(len(v.observed))), lost: committed - observedExpected,
		regressions: v.regressions, skips: v.skips, errors: v.errors + unexpected,
		latencies: append([]float64(nil), v.latencies...), diagnostics: diagnostics,
	}
}
