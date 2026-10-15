//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	managed "github.com/emitlane/emitlane/consumer"
	"github.com/emitlane/emitlane/integrity"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const rollingPreviousCommit = "5679ce24da54ecbe319896d6749d884575aab0e2"

// Real standalone processes share schema4 and ordered leases during upgrade and
// binary rollback. The previous source is rebuilt with a patched Go toolchain.
func TestRelayRollingUpgradeAndRollbackFromV091(t *testing.T) {
	oldPath, newPath := os.Getenv("EMITLANE_PREVIOUS_RELAY_BINARY"), os.Getenv("EMITLANE_CURRENT_RELAY_BINARY")
	if oldPath == "" && newPath == "" {
		t.Skip("opt-in previous/current standalone relay qualification")
	}
	output := os.Getenv("EMITLANE_ROLLING_OUTPUT")
	if !filepath.IsAbs(oldPath) || !filepath.IsAbs(newPath) || !filepath.IsAbs(output) {
		t.Fatal("absolute previous/current binaries and fresh output directory required")
	}
	commit, _, dirty, _ := soakGitProvenance(t)
	if dirty {
		t.Fatal("rolling evidence requires clean source")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	for path, want := range map[string]string{oldPath: rollingPreviousCommit, newPath: commit} {
		version, err := exec.CommandContext(ctx, path, "version").CombinedOutput()
		if err != nil || !strings.Contains(string(version), want) {
			t.Fatalf("binary provenance mismatch for %s: %s: %v", path, version, err)
		}
	}
	if err := os.Mkdir(output, 0700); err != nil {
		t.Fatal(err)
	}
	proof, err := os.OpenFile(filepath.Join(output, "events.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer proof.Close()
	write := func(v any) {
		t.Helper()
		if err := json.NewEncoder(proof).Encode(v); err != nil {
			t.Fatal(err)
		}
		if err := proof.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	write(map[string]any{"kind": "start", "time": time.Now().UTC(), "commit": commit, "previous_commit": rollingPreviousCommit, "previous_source_version": "v0.9.1 rebuilt with the patched candidate toolchain; not byte-identical to published artifacts"})
	t.Setenv("EMITLANE_QUALIFICATION_DURATION", "3m")
	e := startEnv(t)
	_, err = e.pool.Exec(ctx, `CREATE TABLE public.soak_expected(event_id UUID PRIMARY KEY,sequence BIGINT UNIQUE NOT NULL,stream TEXT NOT NULL,stream_sequence BIGINT NOT NULL,poison BOOLEAN NOT NULL,repaired BOOLEAN NOT NULL DEFAULT FALSE);
CREATE TABLE public.soak_stream_progress(stream TEXT PRIMARY KEY,sequence BIGINT NOT NULL);
INSERT INTO public.soak_stream_progress SELECT 'stream-'||g,0 FROM generate_series(0,7)g;
CREATE TABLE public.soak_effect_progress(stream TEXT PRIMARY KEY,sequence BIGINT NOT NULL);
INSERT INTO public.soak_effect_progress SELECT * FROM public.soak_stream_progress;
CREATE TABLE public.soak_pruned(event_id UUID PRIMARY KEY);
CREATE TABLE public.soak_observed(event_id UUID PRIMARY KEY,records BIGINT NOT NULL);`)
	if err != nil {
		t.Fatal(err)
	}
	topic, group, name := "rolling-"+uuid.NewString(), "rolling-group-"+uuid.NewString(), "rolling-consumer-"+uuid.NewString()
	e.ensureTopicPartitions(t, topic, 8)
	consumer := launchManaged(t, e, qualificationConsumerConfig(name, "rolling-managed"), newManagedInboxStore(t, e), newManagedFactory(t, e, topic, group), func(ctx context.Context, tx pgx.Tx, m managed.Message) error { return qualificationHandler(ctx, tx, m) })
	defer consumer.stop(t)
	oldID, newID := "rolling-old-"+uuid.NewString(), "rolling-new-"+uuid.NewString()
	launch := func(path, id, label string) *rollingRelayProcess {
		t.Helper()
		log, err := os.OpenFile(filepath.Join(output, label+".log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(path, "run")
		cmd.Env = append(os.Environ(), "EMITLANE_DATABASE_URL="+e.databaseURL, "EMITLANE_KAFKA_BROKERS="+strings.Join(e.brokers, ","), "EMITLANE_INSTANCE_ID="+id, "EMITLANE_HTTP_ADDR=127.0.0.1:0", "EMITLANE_ADMIN_ENABLED=false",
			"EMITLANE_RELAY_CONCURRENCY=2", "EMITLANE_RELAY_BATCH_SIZE=20", "EMITLANE_RELAY_POLL_INTERVAL=20ms", "EMITLANE_RELAY_LEASE_DURATION=3s", "EMITLANE_PUBLISH_TIMEOUT=1s", "EMITLANE_SHUTDOWN_TIMEOUT=5s", "EMITLANE_RELAY_HEARTBEAT_INTERVAL=200ms", "EMITLANE_RELAY_STALE_AFTER=2s", "EMITLANE_ORDERING_REBALANCE_INTERVAL=200ms", "EMITLANE_ORDERING_LEASE_DURATION=4s", "EMITLANE_ORDERING_SAFETY_MARGIN=100ms", "EMITLANE_RETENTION_DELIVERED=0")
		cmd.Stdout, cmd.Stderr = log, log
		if err := cmd.Start(); err != nil {
			_ = log.Close()
			t.Fatal(err)
		}
		p := &rollingRelayProcess{cmd: cmd, done: make(chan error, 1), log: log}
		go func() { p.done <- cmd.Wait() }()
		t.Cleanup(func() { p.stop(t) })
		deadline := time.Now().Add(45 * time.Second)
		for time.Now().Before(deadline) {
			var live bool
			if err := e.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM emitlane.relay_instances WHERE instance_id=$1 AND stopped_at IS NULL AND ordering_capable=TRUE AND last_heartbeat_at>NOW()-INTERVAL '2 seconds')`, id).Scan(&live); err == nil && live {
				return p
			}
			select {
			case err := <-p.done:
				p.stopped = true
				_ = log.Close()
				t.Fatalf("relay %s exited before presence: %v", label, err)
			default:
			}
			time.Sleep(25 * time.Millisecond)
		}
		t.Fatalf("relay %s did not register presence", label)
		return nil
	}
	old := launch(oldPath, oldID, "old-before-upgrade")
	var seq int64
	produce := func() {
		t.Helper()
		for range 100 {
			seq++
			p := qualificationPayload{Seed: 20261015, Sequence: seq}
			if seq%2 == 0 {
				p.Stream = fmt.Sprintf("stream-%d", (seq/2)%8)
			}
			if err := enqueueQualification(ctx, e, topic, uuid.NewString(), p); err != nil {
				t.Fatal(err)
			}
		}
	}
	drain := func(label string) {
		t.Helper()
		deadline := time.Now().Add(60 * time.Second)
		for time.Now().Before(deadline) {
			var effects int64
			if err := e.pool.QueryRow(ctx, `SELECT COUNT(*) FROM emitlane.inbox_events WHERE consumer=$1 AND status='processed'`, name).Scan(&effects); err == nil && effects == seq {
				lag, err := qualificationKafkaSnapshot(ctx, e.brokers, topic, group)
				if err == nil && lag["lag"].(int64) == 0 {
					write(map[string]any{"kind": "drained", "phase": label, "events": seq, "lag": 0, "time": time.Now().UTC()})
					return
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("rolling phase %s did not drain %d events", label, seq)
	}
	produce()
	drain("old-only")
	current := launch(newPath, newID, "current-during-upgrade")
	deadline := time.Now().Add(45 * time.Second)
	shared := false
	for time.Now().Before(deadline) {
		var oldOwned, newOwned int
		if err := e.pool.QueryRow(ctx, `SELECT COUNT(*) FILTER(WHERE lease_owner=$1),COUNT(*) FILTER(WHERE lease_owner=$2) FROM emitlane.ordering_partitions WHERE lease_until>NOW()`, oldID, newID).Scan(&oldOwned, &newOwned); err == nil && oldOwned > 0 && newOwned > 0 {
			shared = true
			write(map[string]any{"kind": "mixed_ownership", "old_partitions": oldOwned, "current_partitions": newOwned, "time": time.Now().UTC()})
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !shared {
		t.Fatal("old/current processes did not both own valid ordered partitions")
	}
	produce()
	drain("mixed-version")
	old.stop(t)
	produce()
	drain("current-only-after-upgrade")
	old = launch(oldPath, oldID, "old-during-rollback")
	current.stop(t)
	produce()
	drain("old-only-after-rollback")
	old.stop(t)
	consumer.stop(t)
	records, duplicates := auditQualificationKafka(t, ctx, e, topic, 20261015)
	mismatch, total, err := qualificationDatabaseIdentityAudit(ctx, e, topic, name)
	if err != nil || mismatch != 0 || total != seq {
		t.Fatalf("rolling identity/effect audit=%d/%d/%d: %v", mismatch, total, seq, err)
	}
	var streams int
	if err := e.pool.QueryRow(ctx, `SELECT COUNT(*) FROM public.soak_stream_progress s JOIN public.soak_effect_progress e USING(stream) WHERE s.sequence<>e.sequence OR s.sequence=0`).Scan(&streams); err != nil || streams != 0 {
		t.Fatalf("rolling stream audit=%d: %v", streams, err)
	}
	verifier, err := integrity.NewVerifier(e.pool, integrity.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	report, err := verifier.Check(ctx, integrity.ModeFull)
	if err != nil || report.FindingsTruncated || report.Summary.Violations != 0 || report.Summary.InboxStaleLeases != 0 {
		t.Fatalf("rolling integrity=%+v: %v", report, err)
	}
	write(map[string]any{"kind": "PASS", "commit": commit, "previous_commit": rollingPreviousCommit, "events": total, "protected_effects": total, "kafka_records": records, "allowed_duplicates": duplicates, "integrity": report, "time": time.Now().UTC()})
}

type rollingRelayProcess struct {
	cmd     *exec.Cmd
	done    chan error
	log     *os.File
	stopped bool
}

func (p *rollingRelayProcess) stop(t *testing.T) {
	t.Helper()
	if p.stopped {
		return
	}
	p.stopped = true
	defer p.log.Close()
	if err := p.cmd.Process.Signal(os.Interrupt); err != nil {
		t.Errorf("stop relay: %v", err)
	}
	select {
	case err := <-p.done:
		if err != nil {
			t.Errorf("relay exit: %v", err)
		}
	case <-time.After(12 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.done
		t.Error("relay exceeded graceful-stop deadline")
	}
}
