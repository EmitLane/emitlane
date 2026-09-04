package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	mathrand "math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	adminapi "github.com/emitlane/emitlane/internal/admin"
	"github.com/emitlane/emitlane/outbox"
)

type faultStats struct {
	restarts       atomic.Int64
	crashes        atomic.Int64
	outages        atomic.Int64
	outageNanos    atomic.Int64
	pauses         atomic.Int64
	memberships    atomic.Int64
	acquisitions   atomic.Int64
	handoffs       atomic.Int64
	infrastructure atomic.Int64
	mu             sync.Mutex
	durations      map[string]float64
}

func newFaultStats() *faultStats {
	return &faultStats{durations: map[string]float64{
		"relay_graceful_restart":  0,
		"relay_crash_takeover":    0,
		"kafka_outage":            0,
		"cluster_pause":           0,
		"relay_membership_change": 0,
	}}
}
func (s *faultStats) addDuration(name string, d time.Duration) {
	s.mu.Lock()
	s.durations[name] += d.Seconds()
	s.mu.Unlock()
}
func (s *faultStats) durationSnapshot() map[string]float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]float64, len(s.durations))
	for k, v := range s.durations {
		out[k] = v
	}
	return out
}

type soakRuntime struct {
	runDir          string
	cfg             Config
	started         time.Time
	phaseMu         sync.RWMutex
	phase           string
	progressStop    chan struct{}
	progressDone    chan struct{}
	progressOnce    sync.Once
	progressWriteMu sync.Mutex
	timelineMu      sync.Mutex
	timeline        []timelinePoint
	verifier        *verifier
	faults          *faultStats
	relays          *relayGroup
	env             *soakEnvironment
	orderedTopic    string
	unorderedTopic  string
}

func runSoak(ctx context.Context, runDir string, cfg Config) error {
	r := &soakRuntime{runDir: runDir, cfg: cfg, started: time.Now().UTC(), phase: "initializing", progressStop: make(chan struct{}), progressDone: make(chan struct{}), verifier: newVerifier(), faults: newFaultStats(), orderedTopic: "emitlane-soak-ordered-" + cfg.RunID, unorderedTopic: "emitlane-soak-unordered-" + cfg.RunID}
	branch, commit := gitIdentity()
	base := Result{RunID: cfg.RunID, Profile: cfg.Profile, Seed: cfg.Seed, StartedAt: r.started, GitBranch: branch, GitCommit: commit, GoVersion: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH, CPU: platformCPU(), DockerVersion: dockerVersion(), KafkaVersion: kafkaImage, Configuration: cfg, OrderedStreams: cfg.OrderedStreams, FaultDurations: map[string]float64{}}
	_ = writeJSON(filepath.Join(runDir, "metadata.json"), base)
	r.writeProgress("running")
	go r.progressLoop()
	defer r.stopProgress()

	initCtx, cancelInit := context.WithTimeout(ctx, 4*time.Minute)
	env, err := startEnvironment(initCtx, cfg.RunID)
	cancelInit()
	if err != nil {
		if ctx.Err() != nil {
			return r.finish(base, "aborted", errors.New("stopped by operator during initialization"), 130)
		}
		return r.finish(base, "failed", fmt.Errorf("initialize local infrastructure: %w", err), 1)
	}
	r.env = env
	base.PostgreSQLVersion = env.postgresVersion
	_ = writeJSON(filepath.Join(runDir, "metadata.json"), base)
	cleanupCtx := context.Background()
	defer func() { c, cancel := context.WithTimeout(cleanupCtx, 90*time.Second); defer cancel(); env.close(c) }()
	r.relays = newRelayGroup(cfg.RunID, env.databaseURL, env.brokers, cfg)
	defer r.relays.stopAll()
	for i := 0; i < cfg.Relays; i++ {
		if _, err := r.relays.start(); err != nil {
			return r.finish(base, "failed", fmt.Errorf("start relay: %w", err), 1)
		}
	}

	consumer, err := kgo.NewClient(kgo.SeedBrokers(env.brokers...), kgo.ConsumeTopics(r.orderedTopic, r.unorderedTopic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchMaxWait(250*time.Millisecond))
	if err != nil {
		return r.finish(base, "failed", err, 1)
	}
	defer consumer.Close()
	consumerCtx, stopConsumer := context.WithCancel(context.Background())
	consumerDone := make(chan struct{})
	go r.consume(consumerCtx, consumer, consumerDone)
	var stopConsumerOnce sync.Once
	stopVerifierConsumer := func() {
		stopConsumerOnce.Do(func() {
			stopConsumer()
			<-consumerDone
		})
	}
	defer stopVerifierConsumer()

	r.setPhase("warmup")
	if !waitContext(ctx, cfg.Warmup) {
		return r.finish(base, "aborted", errors.New("stopped by operator during warmup"), 130)
	}
	var initiallyOwned int64
	if err := env.pool.QueryRow(ctx, `SELECT COUNT(*) FROM emitlane.ordering_partitions WHERE lease_owner IS NOT NULL`).Scan(&initiallyOwned); err == nil {
		r.faults.acquisitions.Store(initiallyOwned)
	}
	r.setPhase("running")
	runningCtx, stopRunning := context.WithTimeout(ctx, cfg.Duration)
	producerDone := make(chan error, 1)
	go func() { producerDone <- r.produce(runningCtx, seededRand(cfg.Seed)) }()
	faultDone := make(chan struct{})
	go func() { defer close(faultDone); r.injectFaults(runningCtx, seededRand(cfg.Seed^0xd1b54a32d192ed03)) }()
	<-runningCtx.Done()
	stopRunning()
	producerErr := <-producerDone
	<-faultDone
	if ctx.Err() != nil {
		return r.finish(base, "aborted", errors.New("stopped by operator"), 130)
	}
	if producerErr != nil && !errors.Is(producerErr, context.DeadlineExceeded) && !errors.Is(producerErr, context.Canceled) {
		r.faults.infrastructure.Add(1)
		log.Printf("producer error: %v", producerErr)
	}

	r.setPhase("recovering")
	recoveryStart := time.Now()
	recoveryErr := r.recover(ctx)
	base.BacklogRecoverySecs = time.Since(recoveryStart).Seconds()
	if ctx.Err() != nil {
		return r.finish(base, "aborted", errors.New("stopped by operator during recovery"), 130)
	}
	if recoveryErr != nil {
		log.Printf("recovery did not quiesce: %v", recoveryErr)
	}

	r.setPhase("verifying")
	r.relays.stopAll()
	stopVerifierConsumer()
	auditCtx, cancelAudit := context.WithTimeout(ctx, finalAuditTimeout)
	audit, auditErr := auditKafka(auditCtx, r.env.brokers, []string{r.orderedTopic, r.unorderedTopic}, r.verifier)
	cancelAudit()
	if ctx.Err() != nil {
		return r.finish(base, "aborted", errors.New("stopped by operator during final Kafka audit"), 130)
	}
	if auditErr != nil {
		r.faults.infrastructure.Add(1)
		auditErr = fmt.Errorf("final Kafka audit: %w", auditErr)
		log.Printf("%v", auditErr)
		recoveryErr = errors.Join(recoveryErr, auditErr)
	} else {
		auditSnapshot := audit.snapshot()
		log.Printf("final Kafka audit completed records=%d observed=%d lost=%d regressions=%d skips=%d",
			auditSnapshot.records, auditSnapshot.observed, auditSnapshot.lost,
			auditSnapshot.regressions, auditSnapshot.skips)
		r.verifier.adoptAudit(audit)
	}
	return r.finish(base, "completed", recoveryErr, 0)
}

func waitContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (r *soakRuntime) setPhase(phase string) {
	r.phaseMu.Lock()
	r.phase = phase
	r.phaseMu.Unlock()
	_ = writeJSON(filepath.Join(r.runDir, "state.json"), State{RunID: r.cfg.RunID, State: "running", Phase: phase, UpdatedAt: time.Now().UTC()})
	log.Printf("phase=%s", phase)
	r.writeProgress("running")
}

func (r *soakRuntime) currentPhase() string {
	r.phaseMu.RLock()
	defer r.phaseMu.RUnlock()
	return r.phase
}

func (r *soakRuntime) progressLoop() {
	defer close(r.progressDone)
	ticker := time.NewTicker(progressPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-r.progressStop:
			return
		case <-ticker.C:
			r.writeProgress("running")
		}
	}
}

func (r *soakRuntime) stopProgress() {
	r.progressOnce.Do(func() {
		close(r.progressStop)
		<-r.progressDone
	})
}

func (r *soakRuntime) writeProgress(state string) {
	r.progressWriteMu.Lock()
	defer r.progressWriteMu.Unlock()
	snap := r.verifier.snapshot()
	progress := Progress{RunID: r.cfg.RunID, State: state, Phase: r.currentPhase(), Profile: r.cfg.Profile, Seed: r.cfg.Seed, StartedAt: r.started, UpdatedAt: time.Now().UTC(), Elapsed: time.Since(r.started), CommittedEvents: snap.committed, ObservedUnique: snap.observed, BrokerRecords: snap.records, DuplicateRecords: snap.duplicates, NotObservedYet: snap.lost, OrderedStreams: r.cfg.OrderedStreams, Relays: r.cfg.Relays, RelayRestarts: r.faults.restarts.Load(), RelayCrashTakeovers: r.faults.crashes.Load(), KafkaOutages: r.faults.outages.Load(), PauseCycles: r.faults.pauses.Load(), OrderingRegressions: snap.regressions, OrderingSkips: snap.skips}
	r.timelineMu.Lock()
	r.timeline = append(r.timeline, timelinePoint{Elapsed: progress.Elapsed, Phase: progress.Phase, Committed: progress.CommittedEvents, Observed: progress.ObservedUnique, Backlog: progress.NotObservedYet, Restarts: progress.RelayRestarts, Crashes: progress.RelayCrashTakeovers, KafkaOutages: progress.KafkaOutages, PauseCycles: progress.PauseCycles, MembershipChanges: r.faults.memberships.Load()})
	r.timelineMu.Unlock()
	_ = writeJSON(filepath.Join(r.runDir, "progress.json"), progress)
}

func (r *soakRuntime) timelineSnapshot() []timelinePoint {
	r.timelineMu.Lock()
	defer r.timelineMu.Unlock()
	return append([]timelinePoint(nil), r.timeline...)
}

func (r *soakRuntime) consume(ctx context.Context, client *kgo.Client, done chan<- struct{}) {
	defer close(done)
	for ctx.Err() == nil {
		fetches := client.PollFetches(ctx)
		for _, record := range fetches.Records() {
			r.verifier.observe(record, time.Now())
		}
	}
}

func (r *soakRuntime) produce(ctx context.Context, random *mathrand.Rand) error {
	writer := outbox.NewWriter()
	sequences := make([]int64, r.cfg.OrderedStreams)
	for i := range sequences {
		sequences[i] = 1
	}
	payload := make([]byte, r.cfg.PayloadBytes)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}
	interval := 100 * time.Millisecond
	perTick := float64(r.cfg.EventsPerSecond) * interval.Seconds()
	credit := 0.0
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			credit += perTick
			count := int(credit)
			credit -= float64(count)
			for i := 0; i < count; i++ {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if random.IntN(100) < 2 {
					if err := enqueueRollback(ctx, r.env.pool, writer, r.unorderedTopic, payload); err != nil {
						return err
					}
					continue
				}
				if random.IntN(100) < r.cfg.OrderedPercent {
					streamIndex := random.IntN(r.cfg.OrderedStreams)
					sequence := sequences[streamIndex]
					if random.IntN(100) < 15 {
						if err := r.enqueueOrdered(ctx, writer, streamIndex, sequence+1, payload); err != nil {
							return err
						}
						if err := r.enqueueOrdered(ctx, writer, streamIndex, sequence, payload); err != nil {
							return err
						}
						sequences[streamIndex] += 2
						i++
					} else {
						if err := r.enqueueOrdered(ctx, writer, streamIndex, sequence, payload); err != nil {
							return err
						}
						sequences[streamIndex]++
					}
				} else if err := r.enqueueUnordered(ctx, writer, payload); err != nil {
					return err
				}
			}
		}
	}
}

func enqueueRollback(ctx context.Context, pool *pgxpool.Pool, writer *outbox.Writer, topic string, payload []byte) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	_, err = writer.Enqueue(ctx, tx, outbox.Event{ID: uuid.NewString(), Destination: topic, Type: "soak.rollback", Payload: payload})
	return err
}

func (r *soakRuntime) enqueueOrdered(ctx context.Context, writer *outbox.Writer, streamIndex int, sequence int64, payload []byte) error {
	id := uuid.New()
	committedAt := time.Now()
	tx, err := r.env.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	stream := "stream:" + strconv.Itoa(streamIndex)
	if _, err := writer.Enqueue(ctx, tx, outbox.Event{ID: id.String(), Destination: r.orderedTopic, Type: "soak.ordered", Payload: payload, OrderingKey: stream, Sequence: sequence}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	r.verifier.committed(id, expectedEvent{ordered: true, stream: r.orderedTopic + "/" + stream, sequence: sequence, committedAt: committedAt})
	return nil
}

func (r *soakRuntime) enqueueUnordered(ctx context.Context, writer *outbox.Writer, payload []byte) error {
	id := uuid.New()
	committedAt := time.Now()
	tx, err := r.env.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err := writer.Enqueue(ctx, tx, outbox.Event{ID: id.String(), Destination: r.unorderedTopic, Type: "soak.unordered", Payload: payload}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	r.verifier.committed(id, expectedEvent{committedAt: committedAt})
	return nil
}

func (r *soakRuntime) injectFaults(ctx context.Context, random *mathrand.Rand) {
	if !r.cfg.Faults {
		return
	}
	interval := r.cfg.FaultInterval
	if scaled := r.cfg.Duration / 6; scaled < interval {
		interval = max(3*time.Second, scaled)
	}
	timer := time.NewTimer(min(5*time.Second, interval))
	defer timer.Stop()
	order := random.Perm(5)
	index := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		fault := order[index%len(order)]
		index++
		if index%len(order) == 0 {
			order = random.Perm(5)
		}
		started := time.Now()
		beforeEpoch := r.partitionEpoch(ctx)
		var err error
		switch fault {
		case 0:
			log.Print("fault=relay_graceful_restart begin")
			err = r.relays.stopIndex(random.IntN(max(1, r.relays.count())), true)
			if err == nil {
				_, err = r.relays.start()
			}
			if err == nil {
				r.faults.restarts.Add(1)
			}
			r.faults.addDuration("relay_graceful_restart", time.Since(started))
		case 1:
			log.Print("fault=relay_crash_takeover begin")
			err = r.relays.stopIndex(random.IntN(max(1, r.relays.count())), false)
			if err == nil {
				if !waitContext(ctx, 4500*time.Millisecond) {
					return
				}
				_, err = r.relays.start()
			}
			if err == nil {
				r.faults.crashes.Add(1)
			}
			r.faults.addDuration("relay_crash_takeover", time.Since(started))
		case 2:
			d := time.Duration(1+random.IntN(3)) * time.Second
			log.Printf("fault=kafka_outage duration=%s begin", d)
			err = r.env.kafkaOutage(ctx, d)
			if err == nil {
				r.faults.outages.Add(1)
				r.faults.outageNanos.Add(d.Nanoseconds())
			}
			r.faults.addDuration("kafka_outage", time.Since(started))
		case 3:
			d := time.Duration(1+random.IntN(2)) * time.Second
			log.Printf("fault=cluster_pause duration=%s begin", d)
			err = r.setPaused(ctx, true)
			if err == nil && waitContext(ctx, d) {
				err = r.setPaused(ctx, false)
				if err == nil {
					r.faults.pauses.Add(1)
				}
			}
			r.faults.addDuration("cluster_pause", time.Since(started))
		case 4:
			log.Print("fault=relay_membership_change begin")
			_, err = r.relays.start()
			if err == nil && waitContext(ctx, 2*time.Second) {
				err = r.relays.stopIndex(r.relays.count()-1, true)
			}
			if err == nil {
				r.faults.memberships.Add(1)
			}
			r.faults.addDuration("relay_membership_change", time.Since(started))
		}
		if err != nil && ctx.Err() == nil {
			r.faults.infrastructure.Add(1)
			log.Printf("fault=%d error=%v", fault, err)
		} else {
			log.Printf("fault=%d end elapsed=%s", fault, time.Since(started))
		}
		if err == nil && ctx.Err() == nil && (fault == 0 || fault == 1 || fault == 4) {
			if !waitContext(ctx, 350*time.Millisecond) {
				return
			}
			delta := max(int64(0), r.partitionEpoch(ctx)-beforeEpoch)
			if fault == 1 {
				r.faults.acquisitions.Add(delta)
				r.faults.handoffs.Add(delta)
			} else {
				r.faults.acquisitions.Add(delta / 2)
				r.faults.handoffs.Add(delta / 2)
			}
		}
		timer.Reset(interval)
	}
}

func (r *soakRuntime) partitionEpoch(ctx context.Context) int64 {
	var epoch int64
	if err := r.env.pool.QueryRow(ctx, `SELECT COALESCE(SUM(epoch),0) FROM emitlane.ordering_partitions`).Scan(&epoch); err != nil {
		return 0
	}
	return epoch
}

func (r *soakRuntime) setPaused(ctx context.Context, paused bool) error {
	_, err := r.env.store.SetPaused(ctx, paused, adminapi.Mutation{Actor: "local-soak", Reason: "seeded local soak fault"})
	return err
}

func (r *soakRuntime) recover(ctx context.Context) error {
	recoveryCtx, cancel := context.WithTimeout(ctx, r.cfg.RecoveryTimeout)
	defer cancel()
	if err := r.env.restoreKafka(recoveryCtx); err != nil {
		r.faults.infrastructure.Add(1)
		return fmt.Errorf("restore Kafka: %w", err)
	}
	if err := r.setPaused(recoveryCtx, false); err != nil {
		r.faults.infrastructure.Add(1)
		return err
	}
	for r.relays.count() < r.cfg.Relays {
		if _, err := r.relays.start(); err != nil {
			r.faults.infrastructure.Add(1)
			return err
		}
	}
	for r.relays.count() > r.cfg.Relays {
		if err := r.relays.stopIndex(r.relays.count()-1, true); err != nil {
			r.faults.infrastructure.Add(1)
			return err
		}
	}
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var last string
	for {
		pending, inflight, dead, blocked, gaps, err := queueState(recoveryCtx, r.env.pool)
		if err == nil {
			last = fmt.Sprintf("pending=%d inflight=%d dead=%d blocked=%d gaps=%d", pending, inflight, dead, blocked, gaps)
			if pending == 0 && inflight == 0 && dead == 0 && blocked == 0 && gaps == 0 {
				return nil
			}
		}
		select {
		case <-recoveryCtx.Done():
			return fmt.Errorf("recovery timeout: %s", last)
		case <-ticker.C:
		}
	}
}

func queueState(ctx context.Context, pool *pgxpool.Pool) (pending, inflight, dead, blocked, gaps int64, err error) {
	err = pool.QueryRow(ctx, `
WITH stream_state AS (
 SELECT s.destination,s.ordering_key,
 CASE WHEN n.status='dead' THEN 'dead_blocked' WHEN n.status='inflight' THEN 'inflight'
      WHEN n.status='pending' AND n.available_at>NOW() THEN 'retry_wait'
      WHEN n.id IS NOT NULL THEN 'ready' WHEN f.ordering_sequence IS NOT NULL THEN 'gap' ELSE 'ready' END state
 FROM emitlane.ordering_streams s
 LEFT JOIN LATERAL (SELECT id,status,available_at FROM emitlane.outbox_events e WHERE e.destination=s.destination AND e.ordering_key=s.ordering_key AND e.ordering_sequence=s.next_sequence LIMIT 1) n ON TRUE
 LEFT JOIN LATERAL (SELECT ordering_sequence FROM emitlane.outbox_events e WHERE e.destination=s.destination AND e.ordering_key=s.ordering_key AND e.ordering_sequence>s.next_sequence ORDER BY ordering_sequence LIMIT 1) f ON TRUE)
SELECT COUNT(*) FILTER(WHERE status='pending'),COUNT(*) FILTER(WHERE status='inflight'),COUNT(*) FILTER(WHERE status='dead'),
       (SELECT COUNT(*) FROM stream_state WHERE state IN ('retry_wait','gap','dead_blocked')),
       (SELECT COUNT(*) FROM stream_state WHERE state='gap') FROM emitlane.outbox_events`).Scan(&pending, &inflight, &dead, &blocked, &gaps)
	return
}

func (r *soakRuntime) finish(result Result, requestedState string, prior error, requestedCode int) error {
	r.stopProgress()
	if requestedState == "aborted" {
		r.setPhase("aborted")
	}
	snap := r.verifier.finalSnapshot()
	result.EndedAt = time.Now().UTC()
	result.DurationSeconds = result.EndedAt.Sub(result.StartedAt).Seconds()
	result.CommittedEvents = snap.committed
	result.OrderedCommitted = snap.ordered
	result.UnorderedCommitted = snap.unordered
	result.ObservedUniqueEvents = snap.observed
	result.BrokerRecords = snap.records
	result.DuplicateRecords = snap.duplicates
	result.LostEvents = snap.lost
	result.OrderingRegressions = snap.regressions
	result.OrderingSkips = snap.skips
	result.Diagnostics = snap.diagnostics
	result.RelayRestarts = r.faults.restarts.Load()
	result.RelayCrashTakeovers = r.faults.crashes.Load()
	result.KafkaOutages = r.faults.outages.Load()
	result.KafkaOutageSeconds = time.Duration(r.faults.outageNanos.Load()).Seconds()
	result.PauseCycles = r.faults.pauses.Load()
	result.InfrastructureErrors = r.faults.infrastructure.Load() + snap.errors
	result.FaultDurations = r.faults.durationSnapshot()
	if r.env != nil && r.env.pool != nil {
		result.PendingFinal, result.InflightFinal, result.DeadFinal, result.BlockedStreamsFinal, result.GapStreamsFinal, _ = queueState(context.Background(), r.env.pool)
	}
	result.PartitionAcquisitions = r.faults.acquisitions.Load()
	result.PartitionHandoffs = r.faults.handoffs.Load()
	if r.cfg.Duration > 0 {
		result.ThroughputEventsSec = float64(result.CommittedEvents) / r.cfg.Duration.Seconds()
	}
	result.LatencyP50Millis = percentile(snap.latencies, .50)
	result.LatencyP95Millis = percentile(snap.latencies, .95)
	result.LatencyP99Millis = percentile(snap.latencies, .99)
	state := requestedState
	code := requestedCode
	reason := ""
	if state == "completed" {
		if prior != nil {
			reason = prior.Error()
		}
		if err := verdict(result); err != nil {
			if reason != "" {
				reason += "; "
			}
			reason += err.Error()
		}
		if reason != "" {
			state = "failed"
			code = 1
		}
	}
	if prior != nil && reason == "" {
		reason = prior.Error()
	}
	result.FinalState = state
	result.FailureReason = reason
	result.ExitCode = code
	_ = writeJSON(filepath.Join(r.runDir, "result.json"), result)
	_ = os.WriteFile(filepath.Join(r.runDir, "report.md"), []byte(reportMarkdown(result)), 0o644)
	_ = os.WriteFile(filepath.Join(r.runDir, "exit_code"), []byte(strconv.Itoa(code)+"\n"), 0o644)
	r.phaseMu.Lock()
	r.phase = state
	r.phaseMu.Unlock()
	r.writeProgress(state)
	_ = os.WriteFile(filepath.Join(r.runDir, "timeline.svg"), []byte(timelineSVG(r.cfg.RunID, r.timelineSnapshot())), 0o644)
	_ = writeJSON(filepath.Join(r.runDir, "state.json"), State{RunID: r.cfg.RunID, State: state, Phase: state, UpdatedAt: time.Now().UTC(), Reason: reason})
	log.Printf("soak result=%s committed=%d observed=%d duplicates=%d regressions=%d skips=%d reason=%q", state, result.CommittedEvents, result.ObservedUniqueEvents, result.DuplicateRecords, result.OrderingRegressions, result.OrderingSkips, reason)
	if code != 0 {
		if prior == nil {
			prior = errors.New(reason)
		}
		return &exitError{code: code, err: prior}
	}
	return nil
}
