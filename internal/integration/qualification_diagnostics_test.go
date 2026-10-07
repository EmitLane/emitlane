//go:build integration

package integration_test

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/testcontainers/testcontainers-go"

	managed "github.com/emitlane/emitlane/consumer"
	pgstore "github.com/emitlane/emitlane/storage/postgres"
	"github.com/emitlane/emitlane/telemetry"
)

const qualificationKafkaLogDirectory = "/tmp/qualification-kafka-logs"

func qualificationKafkaOptions() testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		if os.Getenv("EMITLANE_QUALIFICATION_DURATION") == "" {
			return nil
		}
		return testcontainers.WithEnv(map[string]string{"KAFKA_LOG_DIRS": qualificationKafkaLogDirectory})(req)
	}
}

// Testcontainers' PostgreSQL module defaults to fsync=off. Qualification must
// explicitly use durable settings; other integration profiles are unchanged.
func qualificationPostgresOptions() testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		if os.Getenv("EMITLANE_QUALIFICATION_DURATION") == "" {
			return nil
		}
		return testcontainers.WithCmd("postgres", "-c", "fsync=on", "-c", "synchronous_commit=on",
			"-c", "full_page_writes=on", "-c", "shared_preload_libraries=pg_stat_statements",
			"-c", "track_io_timing=on")(req)
	}
}

type qualificationTiming struct {
	Count        int64   `json:"count"`
	Errors       int64   `json:"errors"`
	TotalSeconds float64 `json:"total_seconds"`
	MaxSeconds   float64 `json:"max_seconds"`
}

// Query parameters, event IDs and payloads are never retained. The number of
// query shapes is capped, and interval observations are drained every sample.
type qualificationQueryTracer struct {
	mu      sync.Mutex
	queries map[string]qualificationTiming
}
type qualificationQueryKey struct{}
type qualificationQueryStart struct {
	at    time.Time
	query string
}

func (q *qualificationQueryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, qualificationQueryKey{}, qualificationQueryStart{time.Now(), strings.Join(strings.Fields(data.SQL), " ")})
}
func (q *qualificationQueryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	start, ok := ctx.Value(qualificationQueryKey{}).(qualificationQueryStart)
	if ok {
		q.observe(start.query, time.Since(start.at), data.Err)
	}
}
func (q *qualificationQueryTracer) observe(key string, duration time.Duration, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.queries == nil {
		q.queries = make(map[string]qualificationTiming)
	}
	if _, ok := q.queries[key]; !ok && len(q.queries) >= 128 {
		key = "other"
	}
	v := q.queries[key]
	v.Count++
	v.TotalSeconds += duration.Seconds()
	v.MaxSeconds = max(v.MaxSeconds, duration.Seconds())
	if err != nil {
		v.Errors++
	}
	q.queries[key] = v
}
func (q *qualificationQueryTracer) drain() map[string]qualificationTiming {
	q.mu.Lock()
	defer q.mu.Unlock()
	values := q.queries
	q.queries = make(map[string]qualificationTiming)
	return values
}

type qualificationDiagnostics struct {
	output            string
	e                 *env
	pool              *pgxpool.Pool
	tracer            *qualificationQueryTracer
	registry          *prometheus.Registry
	metrics           *telemetry.Metrics
	file, logs        *os.File
	cancel            context.CancelFunc
	done              chan struct{}
	topic, group      string
	previousMutexRate int
	writeErrors       atomic.Int64
}

func startQualificationDiagnostics(t *testing.T, original *env, output string) (*env, *qualificationDiagnostics) {
	t.Helper()
	d := &qualificationDiagnostics{output: output, tracer: &qualificationQueryTracer{}, registry: prometheus.NewRegistry()}
	var err error
	d.metrics, err = telemetry.NewMetrics(d.registry)
	if err != nil {
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(original.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.Tracer = d.tracer
	config.ConnConfig.RuntimeParams["application_name"] = "qualification-workload"
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	copyEnv := *original
	copyEnv.pool = pool
	copyEnv.store, err = pgstore.NewStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	d.e = &copyEnv
	d.logs, err = os.OpenFile(filepath.Join(output, "runtime.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	copyEnv.log = slog.New(slog.NewJSONHandler(d.logs, nil))
	diagnosticConfig := config.Copy()
	diagnosticConfig.MaxConns = 1
	diagnosticConfig.ConnConfig.Tracer = nil
	diagnosticConfig.ConnConfig.RuntimeParams["application_name"] = "qualification-diagnostics"
	d.pool, err = pgxpool.NewWithConfig(context.Background(), diagnosticConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.pool.Close)
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	if _, err := d.pool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements"); err != nil {
		t.Fatal(err)
	}
	var durable bool
	if err := d.pool.QueryRow(ctx, "SELECT current_setting('fsync')='on' AND current_setting('synchronous_commit')='on' AND current_setting('full_page_writes')='on'").Scan(&durable); err != nil || !durable {
		t.Fatalf("qualification durability settings: enabled=%t error=%v", durable, err)
	}
	d.file, err = os.OpenFile(filepath.Join(output, "diagnostics.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	fixture, err := json.Marshal(map[string]any{"postgres_container": original.postgres.GetContainerID(), "kafka_container": original.kafka.GetContainerID(), "kafka_log_directory": qualificationKafkaLogDirectory})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(output, "fixture.json"), fixture, 0600); err != nil {
		t.Fatal(err)
	}
	runtime.SetBlockProfileRate(1_000_000)
	d.previousMutexRate = runtime.SetMutexProfileFraction(10)
	return &copyEnv, d
}

func (d *qualificationDiagnostics) start(topic, group string) {
	d.topic, d.group = topic, group
	ctx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel
	d.done = make(chan struct{})
	go func() {
		defer close(d.done)
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		nextProfile := time.Now()
		for {
			d.sample(ctx, "sample")
			if time.Now().After(nextProfile) {
				d.cpuProfile(ctx)
				nextProfile = time.Now().Add(time.Hour)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (d *qualificationDiagnostics) sample(parent context.Context, kind string) {
	ctx, stop := context.WithTimeout(parent, 5*time.Second)
	defer stop()
	stat := d.e.pool.Stat()
	row := map[string]any{"kind": kind, "time": time.Now().UTC(), "pool": map[string]any{
		"acquired": stat.AcquiredConns(), "total": stat.TotalConns(), "max": stat.MaxConns(),
		"acquire_count": stat.AcquireCount(), "acquire_seconds": stat.AcquireDuration().Seconds(),
		"empty_acquire_count": stat.EmptyAcquireCount(), "cancelled_acquire_count": stat.CanceledAcquireCount()},
		"client_timings_interval": d.tracer.drain()}
	if metrics, err := d.registry.Gather(); err == nil {
		row["metrics"] = metrics
	} else {
		row["metrics_error"] = err.Error()
	}
	if d.topic != "" {
		if kafka, err := qualificationKafkaSnapshot(ctx, d.e.brokers, d.topic, d.group); err == nil {
			row["kafka"] = kafka
		} else {
			row["kafka_error"] = err.Error()
		}
	}
	queries := map[string]string{
		"database":   `SELECT row_to_json(s) FROM (SELECT pg_database_size(current_database()) AS bytes, xact_commit,xact_rollback,blks_read,blks_hit,temp_files,temp_bytes,deadlocks,blk_read_time,blk_write_time,stats_reset FROM pg_stat_database WHERE datname=current_database()) s`,
		"waits":      `SELECT COALESCE(json_agg(s),'[]'::json) FROM (SELECT application_name,state,wait_event_type,wait_event,COUNT(*) AS connections,MAX(EXTRACT(EPOCH FROM NOW()-query_start)) AS longest_query_seconds FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() GROUP BY application_name,state,wait_event_type,wait_event) s`,
		"tables":     `SELECT COALESCE(json_agg(s),'[]'::json) FROM (SELECT relname,n_live_tup,n_dead_tup,autovacuum_count,autoanalyze_count,seq_scan,idx_scan FROM pg_stat_user_tables ORDER BY n_live_tup DESC LIMIT 20) s`,
		"statements": `SELECT COALESCE(json_agg(s),'[]'::json) FROM (SELECT queryid,query,calls,total_exec_time,mean_exec_time,rows,shared_blks_hit,shared_blks_read,blk_read_time,blk_write_time,temp_blks_read,temp_blks_written FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) ORDER BY total_exec_time DESC LIMIT 20) s`,
		"durability": `SELECT row_to_json(s) FROM (SELECT current_setting('fsync') AS fsync,current_setting('synchronous_commit') AS synchronous_commit,current_setting('full_page_writes') AS full_page_writes) s`,
	}
	// Fixed order makes partial samples predictable if an outage consumes the deadline.
	keys := make([]string, 0, len(queries))
	for key := range queries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		var raw []byte
		if err := d.pool.QueryRow(ctx, queries[key]).Scan(&raw); err != nil {
			row[key+"_error"] = err.Error()
		} else {
			row[key] = json.RawMessage(raw)
		}
	}
	if err := json.NewEncoder(d.file).Encode(row); err != nil {
		d.writeErrors.Add(1)
		fmt.Fprintln(d.logs, "diagnostics write failed:", err)
	}
	if err := d.file.Sync(); err != nil {
		d.writeErrors.Add(1)
	}
}

func (d *qualificationDiagnostics) cpuProfile(ctx context.Context) {
	path := filepath.Join(d.output, "cpu-"+time.Now().UTC().Format("20060102T150405")+".pprof")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return
	}
	defer f.Close()
	if err := pprof.StartCPUProfile(f); err != nil {
		return
	}
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
	pprof.StopCPUProfile()
}

// Called after workload goroutines stop, but before test cleanups drop tables
// or TestMain terminates containers. Failure evidence is a private, self-contained
// pg_dump archive, not a permanently running container or a public endpoint.
func (d *qualificationDiagnostics) finish(t *testing.T, passed bool) {
	if d.cancel != nil {
		d.cancel()
		<-d.done
	}
	d.sample(context.Background(), "final")
	runtime.GC()
	for _, name := range []string{"heap", "goroutine", "block", "mutex"} {
		f, err := os.OpenFile(filepath.Join(d.output, name+".pprof"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err == nil {
			_ = pprof.Lookup(name).WriteTo(f, 0)
			_ = f.Close()
		}
	}
	runtime.SetBlockProfileRate(0)
	runtime.SetMutexProfileFraction(d.previousMutexRate)
	if !passed {
		d.failureDump(t)
	}
	_ = d.file.Close()
	_ = d.logs.Close()
	if n := d.writeErrors.Load(); n > 0 {
		t.Errorf("diagnostic evidence write failures: %d", n)
	}
}

func (d *qualificationDiagnostics) failureDump(t *testing.T) {
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Minute)
	defer stop()
	proof := map[string]any{"kind": "failure_artifacts", "time": time.Now().UTC()}
	var plan []byte
	if err := d.pool.QueryRow(ctx, `EXPLAIN (FORMAT JSON) SELECT COUNT(*) FROM public.soak_expected x LEFT JOIN emitlane.inbox_events i ON i.consumer='qualification-v1' AND i.event_id=x.event_id LEFT JOIN emitlane.outbox_events o ON o.id=x.event_id WHERE i.status IS DISTINCT FROM 'processed' OR (o.id IS NOT NULL AND o.status<>'delivered')`).Scan(&plan); err != nil {
		proof["explain_error"] = err.Error()
	} else {
		proof["recovery_plan"] = json.RawMessage(plan)
	}
	partial := filepath.Join(d.output, "failure-database.dump.partial")
	f, err := os.OpenFile(partial, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err == nil {
		stderr, logErr := os.OpenFile(filepath.Join(d.output, "failure-dump.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if logErr != nil {
			err = logErr
		} else {
			command := exec.CommandContext(ctx, "docker", "exec", d.e.postgres.GetContainerID(), "pg_dump", "-U", "emitlane", "-d", "emitlane", "-Fc")
			command.Stdout, command.Stderr = f, stderr
			err = command.Run()
			_ = stderr.Close()
		}
		if syncErr := f.Sync(); err == nil {
			err = syncErr
		}
		_ = f.Close()
		if err == nil {
			err = os.Rename(partial, filepath.Join(d.output, "failure-database.dump"))
		}
	}
	if err != nil {
		proof["dump_error"] = err.Error()
		t.Errorf("failure database dump: %v", err)
	} else {
		proof["dump"] = "failure-database.dump"
	}
	// Stop only this fixture's broker before copying the KRaft metadata and all
	// record segments. A live filesystem copy is not consistent failure evidence.
	id := d.e.kafka.GetContainerID()
	stopBroker := exec.CommandContext(ctx, "docker", "stop", "--time=30", id)
	if out, err := stopBroker.CombinedOutput(); err != nil {
		proof["kafka_archive_error"] = fmt.Sprintf("stop: %v %s", err, out)
		t.Errorf("failure Kafka stop: %v", err)
	} else {
		path := filepath.Join(d.output, "failure-kafka.tar.gz.partial")
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err == nil {
			compressed := gzip.NewWriter(f)
			copyLogs := exec.CommandContext(ctx, "docker", "cp", id+":"+qualificationKafkaLogDirectory, "-")
			copyLogs.Stdout = compressed
			copyLogs.Stderr = d.logs
			err = copyLogs.Run()
			if closeErr := compressed.Close(); err == nil {
				err = closeErr
			}
			if syncErr := f.Sync(); err == nil {
				err = syncErr
			}
			_ = f.Close()
			if err == nil {
				err = os.Rename(path, filepath.Join(d.output, "failure-kafka.tar.gz"))
			}
		}
		if err != nil {
			proof["kafka_archive_error"] = err.Error()
			t.Errorf("failure Kafka archive: %v", err)
		} else {
			proof["kafka_archive"] = "failure-kafka.tar.gz"
		}
	}
	if err := json.NewEncoder(d.file).Encode(proof); err != nil {
		d.writeErrors.Add(1)
	}
	if err := d.file.Sync(); err != nil {
		d.writeErrors.Add(1)
	}
}

type qualificationDiagnosticFactory struct {
	inner  managed.SourceFactory
	tracer *qualificationQueryTracer
}

func (f *qualificationDiagnosticFactory) NewSource(id string, listener managed.RebalanceListener) (managed.Source, error) {
	s, err := f.inner.NewSource(id, listener)
	if err != nil {
		return nil, err
	}
	return &qualificationDiagnosticSource{Source: s, tracer: f.tracer}, nil
}

type qualificationDiagnosticSource struct {
	managed.Source
	tracer *qualificationQueryTracer
}

func (s *qualificationDiagnosticSource) Commit(ctx context.Context, record managed.SourceRecord) error {
	start := time.Now()
	err := s.Source.Commit(ctx, record)
	s.tracer.observe("kafka.offset_commit", time.Since(start), err)
	return err
}

// A failed dump must remain visibly incomplete. Never overwrite it with a
// successful-looking filename or pretend a truncated artifact is restorable.
func TestQualificationTracerBoundsAndSeparatesFailures(t *testing.T) {
	q := &qualificationQueryTracer{}
	for n := 0; n < 1000; n++ {
		q.observe(fmt.Sprint(n), time.Millisecond, nil)
	}
	q.observe("0", 2*time.Second, io.EOF)
	values := q.drain()
	if len(values) > 129 || values["0"].Count != 2 || values["0"].Errors != 1 || values["0"].MaxSeconds != 2 {
		t.Fatalf("invalid bounded timings: %v", values)
	}
	if len(q.drain()) != 0 {
		t.Fatal("interval timings retained after drain")
	}
}

func TestQualificationPostgresDurabilityIsOptIn(t *testing.T) {
	req := testcontainers.GenericContainerRequest{}
	req.Cmd = []string{"postgres", "-c", "fsync=off"}
	t.Setenv("EMITLANE_QUALIFICATION_DURATION", "")
	if err := qualificationPostgresOptions()(&req); err != nil {
		t.Fatal(err)
	}
	if strings.Join(req.Cmd, " ") != "postgres -c fsync=off" {
		t.Fatal("ordinary fixture changed")
	}
	t.Setenv("EMITLANE_QUALIFICATION_DURATION", "3m")
	if err := qualificationPostgresOptions()(&req); err != nil {
		t.Fatal(err)
	}
	for _, setting := range []string{"fsync=on", "synchronous_commit=on", "full_page_writes=on", "shared_preload_libraries=pg_stat_statements", "track_io_timing=on"} {
		if !strings.Contains(strings.Join(req.Cmd, " "), setting) {
			t.Fatalf("missing %s", setting)
		}
	}
}

// Exercise the exact failure archive path and restore its authoritative ledger
// in a separate database inside this test's disposable PostgreSQL container.
func TestQualificationFailureDumpRestoresLedger(t *testing.T) {
	if os.Getenv("EMITLANE_QUALIFICATION_DIAGNOSTICS_CHECK") != "1" {
		t.Skip("opt-in diagnostic archive round trip")
	}
	t.Setenv("EMITLANE_QUALIFICATION_DURATION", "3m")
	e, d := startQualificationDiagnostics(t, startEnv(t), t.TempDir())
	defer d.finish(t, true)
	ctx, stop := context.WithTimeout(context.Background(), time.Minute)
	defer stop()
	if _, err := e.pool.Exec(ctx, `CREATE TABLE public.soak_expected(event_id uuid PRIMARY KEY, sequence bigint NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, `INSERT INTO public.soak_expected VALUES($1,42)`, uuid.New()); err != nil {
		t.Fatal(err)
	}
	d.failureDump(t)
	if t.Failed() {
		return
	}
	archive, err := os.Open(filepath.Join(d.output, "failure-kafka.tar.gz"))
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	compressed, err := gzip.NewReader(archive)
	if err != nil {
		t.Fatal(err)
	}
	defer compressed.Close()
	reader := tar.NewReader(compressed)
	records := false
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasSuffix(header.Name, ".log") {
			records = true
		}
		if _, err := io.Copy(io.Discard, reader); err != nil {
			t.Fatal(err)
		}
	}
	if !records {
		t.Fatal("Kafka archive contains no record segments")
	}
	id := e.postgres.GetContainerID()
	create := exec.CommandContext(ctx, "docker", "exec", id, "createdb", "-U", "emitlane", "qualification_restore")
	if out, err := create.CombinedOutput(); err != nil {
		t.Fatalf("restore database: %v %s", err, out)
	}
	f, err := os.Open(filepath.Join(d.output, "failure-database.dump"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	restore := exec.CommandContext(ctx, "docker", "exec", "-i", id, "pg_restore", "--exit-on-error", "-U", "emitlane", "-d", "qualification_restore")
	restore.Stdin = f
	if out, err := restore.CombinedOutput(); err != nil {
		t.Fatalf("archive restore: %v %s", err, out)
	}
	check := exec.CommandContext(ctx, "docker", "exec", id, "psql", "-U", "emitlane", "-d", "qualification_restore", "-Atc", "SELECT COUNT(*)=1 AND MIN(sequence)=42 FROM public.soak_expected")
	if out, err := check.CombinedOutput(); err != nil || strings.TrimSpace(string(out)) != "t" {
		t.Fatalf("restored ledger: %v %s", err, out)
	}
}
