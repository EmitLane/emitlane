//go:build integration

package integration_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// Only fixture IDs obtained from env are passed to these helpers. No Docker
// inventory, host paths or existing databases are searched or modified.
type qualificationPostgresState struct {
	ID           string          `json:"id"`
	State        json.RawMessage `json:"state"`
	RestartCount int             `json:"restart_count"`
	PGData       string          `json:"pgdata"`
}

func qualificationPostgresInspect(ctx context.Context, id string) (qualificationPostgresState, error) {
	var state qualificationPostgresState
	format := `{"id":{{json .Id}},"state":{{json .State}},"restart_count":{{.RestartCount}}}`
	raw, err := exec.CommandContext(ctx, "docker", "inspect", "--format", format, id).Output()
	if err != nil {
		return state, err
	}
	if err := json.Unmarshal(raw, &state); err != nil {
		return state, err
	}
	if state.ID != id {
		return state, fmt.Errorf("unexpected PostgreSQL fixture ID")
	}
	// Do not persist the container's other environment variables or credentials.
	raw, err = exec.CommandContext(ctx, "docker", "inspect", "--format", `{{range .Config.Env}}{{println .}}{{end}}`, id).Output()
	if err != nil {
		return state, err
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if value, ok := strings.CutPrefix(line, "PGDATA="); ok {
			state.PGData = path.Clean(value)
		}
	}
	if !strings.HasPrefix(state.PGData, "/var/lib/postgresql/") || state.PGData == "/var/lib/postgresql" {
		return state, fmt.Errorf("unexpected fixture PGDATA %q", state.PGData)
	}
	return state, nil
}

func recordQualificationPostgresRestart(t *testing.T, e *env, phase string, started time.Time, cause error, inspect bool) {
	t.Helper()
	output := os.Getenv("EMITLANE_QUALIFICATION_OUTPUT")
	if output == "" {
		return
	}
	row := map[string]any{"time": time.Now().UTC(), "phase": phase, "started_at": started.UTC(), "elapsed_seconds": time.Since(started).Seconds(), "container_id": e.postgres.GetContainerID(), "stop_grace_seconds": 10, "start_deadline_seconds": 120, "sql_readiness_budget_seconds": 60}
	if cause != nil {
		row["error"] = cause.Error()
	}
	if inspect {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		state, err := qualificationPostgresInspect(ctx, e.postgres.GetContainerID())
		cancel()
		if err != nil {
			row["inspect_error"] = err.Error()
		} else {
			row["inspect"] = state
		}
	}
	f, err := os.OpenFile(filepath.Join(output, "postgres-restarts.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err == nil {
		err = json.NewEncoder(f).Encode(row)
		if syncErr := f.Sync(); err == nil {
			err = syncErr
		}
		_ = f.Close()
	}
	if err != nil {
		t.Errorf("record PostgreSQL restart evidence: %v", err)
	}
}

// A .partial file remains on every command/write error. Only successful closed
// streams are renamed, including stderr-only logs from the Docker CLI.
func captureQualificationCommand(ctx context.Context, filename string, args ...string) error {
	f, err := os.OpenFile(filename+".partial", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdout, cmd.Stderr = f, f
	err = cmd.Run()
	if syncErr := f.Sync(); err == nil {
		err = syncErr
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(filename+".partial", filename)
	}
	return err
}

func inspectQualificationPostgresStopped(state qualificationPostgresState) error {
	var status struct {
		Running *bool `json:"Running"`
		Paused  *bool `json:"Paused"`
	}
	if err := json.Unmarshal(state.State, &status); err != nil {
		return err
	}
	if status.Running == nil || status.Paused == nil || *status.Running || *status.Paused {
		return fmt.Errorf("PostgreSQL fixture is not stopped")
	}
	return nil
}

func captureQualificationPostgresDump(ctx context.Context, id, output string) error {
	partial := filepath.Join(output, "failure-database.dump.partial")
	f, err := os.OpenFile(partial, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	stderr, err := os.OpenFile(filepath.Join(output, "failure-dump.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		_ = f.Close()
		return err
	}
	cmd := exec.CommandContext(ctx, "docker", "exec", id, "pg_dump", "-U", "emitlane", "-d", "emitlane", "-Fc")
	cmd.Stdout, cmd.Stderr = f, stderr
	err = cmd.Run()
	_ = stderr.Close()
	if syncErr := f.Sync(); err == nil {
		err = syncErr
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(partial, filepath.Join(output, "failure-database.dump"))
	}
	return err
}

func captureQualificationPostgresFailure(ctx context.Context, id, output string) map[string]any {
	proof := map[string]any{"time": time.Now().UTC(), "container_id": id}
	logsCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	if err := captureQualificationCommand(logsCtx, filepath.Join(output, "failure-postgres.log"), "logs", "--timestamps", id); err != nil {
		proof["postgres_logs_error"] = err.Error()
	} else {
		proof["postgres_logs"] = "failure-postgres.log"
	}
	cancel()
	state, err := qualificationPostgresInspect(ctx, id)
	if err != nil {
		proof["postgres_inspect_error"] = err.Error()
	} else {
		proof["postgres_inspect"] = state
	}
	if err := captureQualificationPostgresDump(ctx, id, output); err != nil {
		proof["dump_error"] = err.Error()
		raw, rawErr := captureQualificationPostgresRaw(ctx, id, output)
		proof["postgres_raw"] = raw
		if rawErr != nil {
			proof["postgres_raw_error"] = rawErr.Error()
		}
	} else {
		proof["dump"] = "failure-database.dump"
	}
	// Retain this proof even if later Kafka capture or the diagnostics writer fails.
	raw, err := json.MarshalIndent(proof, "", "  ")
	if err == nil {
		var f *os.File
		f, err = os.OpenFile(filepath.Join(output, "failure-postgres.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err == nil {
			_, err = f.Write(append(raw, '\n'))
			if syncErr := f.Sync(); err == nil {
				err = syncErr
			}
			_ = f.Close()
		}
	}
	if err != nil {
		proof["postgres_proof_error"] = err.Error()
	}
	return proof
}

func captureQualificationPostgresRaw(ctx context.Context, id, output string) (map[string]any, error) {
	proof := map[string]any{"scope": "stopped fixture byte copy; may require crash recovery; not a logical dump or proof of restore"}
	stopCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	stopOut, err := exec.CommandContext(stopCtx, "docker", "stop", "--time=10", id).CombinedOutput()
	cancel()
	if err != nil {
		return proof, fmt.Errorf("stop owned PostgreSQL fixture: %w: %s", err, stopOut)
	}
	before, err := qualificationPostgresInspect(ctx, id)
	proof["before_copy"] = before
	if err != nil {
		return proof, err
	}
	if err := inspectQualificationPostgresStopped(before); err != nil {
		return proof, err
	}
	filename := filepath.Join(output, "failure-postgres-data.tar.gz")
	f, err := os.OpenFile(filename+".partial", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return proof, err
	}
	compressed := gzip.NewWriter(f)
	cmd := exec.CommandContext(ctx, "docker", "cp", id+":"+before.PGData+"/.", "-")
	cmd.Stdout = compressed
	var stderr strings.Builder
	cmd.Stderr = &stderr
	err = cmd.Run()
	if closeErr := compressed.Close(); err == nil {
		err = closeErr
	}
	if syncErr := f.Sync(); err == nil {
		err = syncErr
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return proof, fmt.Errorf("copy PostgreSQL data: %w: %s", err, stderr.String())
	}
	after, err := qualificationPostgresInspect(ctx, id)
	proof["after_copy"] = after
	if err != nil {
		return proof, err
	}
	if err := inspectQualificationPostgresStopped(after); err != nil {
		return proof, err
	}
	if before.ID != after.ID || before.PGData != after.PGData || before.RestartCount != after.RestartCount || string(before.State) != string(after.State) {
		return proof, fmt.Errorf("PostgreSQL fixture changed during raw copy")
	}
	members, bytes, err := validateQualificationPostgresArchive(filename + ".partial")
	if err != nil {
		return proof, err
	}
	if err := os.Rename(filename+".partial", filename); err != nil {
		return proof, err
	}
	proof["archive"], proof["members"], proof["data_bytes"] = filepath.Base(filename), members, bytes
	proof["gzip_crc_and_all_members_read"] = true
	return proof, nil
}

func validateQualificationPostgresArchive(filename string) (int, int64, error) {
	f, err := os.Open(filename)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	z, err := gzip.NewReader(f)
	if err != nil {
		return 0, 0, err
	}
	defer z.Close()
	archive := tar.NewReader(z)
	members, bytes := 0, int64(0)
	version, control := false, false
	for {
		h, err := archive.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return members, bytes, err
		}
		name := path.Clean(h.Name)
		if path.IsAbs(name) || name == ".." || strings.HasPrefix(name, "../") {
			return members, bytes, fmt.Errorf("unsafe PostgreSQL archive member %q", h.Name)
		}
		members++
		n, err := io.Copy(io.Discard, archive)
		bytes += n
		if err != nil {
			return members, bytes, err
		}
		version = version || (name == "PG_VERSION" && n > 0)
		control = control || (name == "global/pg_control" && n > 0)
	}
	// tar EOF can precede the gzip trailer, whose CRC must also be checked.
	if _, err := io.Copy(io.Discard, z); err != nil {
		return members, bytes, err
	}
	if !version || !control {
		return members, bytes, fmt.Errorf("PostgreSQL archive lacks PG_VERSION or global/pg_control")
	}
	return members, bytes, nil
}

func TestQualificationPostgresCaptureRejectsUnsafeEvidence(t *testing.T) {
	for _, state := range []string{`{}`, `{"Running":true,"Paused":false}`, `{"Running":false,"Paused":true}`, `broken`} {
		if err := inspectQualificationPostgresStopped(qualificationPostgresState{State: json.RawMessage(state)}); err == nil {
			t.Fatalf("accepted unavailable/running container state %s", state)
		}
	}
	if err := inspectQualificationPostgresStopped(qualificationPostgresState{State: json.RawMessage(`{"Running":false,"Paused":false}`)}); err != nil {
		t.Fatal(err)
	}
	for _, variant := range []string{"valid", "missing_control", "unsafe_path", "bad_crc"} {
		t.Run(variant, func(t *testing.T) {
			filename := filepath.Join(t.TempDir(), "data.tar.gz")
			f, err := os.Create(filename)
			if err != nil {
				t.Fatal(err)
			}
			z := gzip.NewWriter(f)
			archive := tar.NewWriter(z)
			files := map[string]string{"PG_VERSION": "16\n", "global/pg_control": "control"}
			if variant == "missing_control" {
				delete(files, "global/pg_control")
			}
			if variant == "unsafe_path" {
				files["../escape"] = "outside"
			}
			for name, value := range files {
				if err := archive.WriteHeader(&tar.Header{Name: name, Size: int64(len(value)), Mode: 0600}); err != nil {
					t.Fatal(err)
				}
				if _, err := io.WriteString(archive, value); err != nil {
					t.Fatal(err)
				}
			}
			for _, close := range []func() error{archive.Close, z.Close, f.Close} {
				if err := close(); err != nil {
					t.Fatal(err)
				}
			}
			if variant == "bad_crc" {
				raw, err := os.ReadFile(filename)
				if err != nil {
					t.Fatal(err)
				}
				raw[len(raw)-8] ^= 1
				if err := os.WriteFile(filename, raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			_, _, err = validateQualificationPostgresArchive(filename)
			if (err == nil) != (variant == "valid") {
				t.Fatalf("archive %s: %v", variant, err)
			}
		})
	}
}

// This is a short, separately opted-in capture/restore regression. It cannot
// replace the failed endurance or a representative production restore/PITR gate.
func TestQualificationPostgresUnavailableCaptureAndRestore(t *testing.T) {
	if os.Getenv("EMITLANE_QUALIFICATION_CAPTURE_CHECK") != "1" {
		t.Skip("set EMITLANE_QUALIFICATION_CAPTURE_CHECK=1 for private fixture capture regression")
	}
	t.Setenv("EMITLANE_QUALIFICATION_DURATION", "2m")
	output := os.Getenv("EMITLANE_QUALIFICATION_OUTPUT")
	if output == "" {
		output = t.TempDir()
	}
	output = filepath.Join(output, "postgres-capture-regression")
	if err := os.Mkdir(output, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("EMITLANE_QUALIFICATION_OUTPUT", output)
	e := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if _, err := e.pool.Exec(ctx, `CREATE TABLE public.qualification_capture_marker(id INTEGER PRIMARY KEY); INSERT INTO public.qualification_capture_marker VALUES(41),(42)`); err != nil {
		t.Fatal(err)
	}
	healthy := filepath.Join(output, "available")
	if err := os.Mkdir(healthy, 0700); err != nil {
		t.Fatal(err)
	}
	healthyProof := captureQualificationPostgresFailure(ctx, e.postgres.GetContainerID(), healthy)
	if healthyProof["dump"] != "failure-database.dump" || healthyProof["dump_error"] != nil || healthyProof["postgres_raw"] != nil {
		t.Fatalf("healthy capture: %v", healthyProof)
	}
	data, err := exec.CommandContext(ctx, "docker", "run", "--rm", "--network=none", "--read-only", "-v", healthy+":/evidence:ro", "--entrypoint=pg_restore", "postgres:16-alpine", "--data-only", "--file=-", "/evidence/failure-database.dump").CombinedOutput()
	if err != nil || !bytes.Contains(data, []byte("41\n42\n")) {
		t.Fatalf("logical dump data stream: %v: %s", err, data)
	}
	if err := e.pool.Ping(ctx); err != nil {
		t.Fatalf("healthy capture stopped original fixture: %v", err)
	}
	e.stopPostgres(t)
	t.Cleanup(func() {
		e.startPostgres(t)
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if _, err := e.pool.Exec(cleanup, "DROP TABLE public.qualification_capture_marker"); err != nil {
			t.Errorf("drop capture marker: %v", err)
		}
	})
	proof := captureQualificationPostgresFailure(ctx, e.postgres.GetContainerID(), output)
	if proof["dump_error"] == nil || proof["dump"] != nil || proof["postgres_raw_error"] != nil || proof["postgres_logs_error"] != nil || proof["postgres_inspect_error"] != nil || proof["postgres_proof_error"] != nil {
		t.Fatalf("unexpected unavailable-database capture: %v", proof)
	}
	if _, err := os.Stat(filepath.Join(output, "failure-database.dump")); !os.IsNotExist(err) {
		t.Fatalf("failed logical dump was published: %v", err)
	}
	if _, _, err := validateQualificationPostgresArchive(filepath.Join(output, "failure-postgres-data.tar.gz")); err != nil {
		t.Fatal(err)
	}
	create := exec.CommandContext(ctx, "docker", "create", "--network=none", "--label=emitlane.qualification.capture=true", "--shm-size=512m", "-e", "POSTGRES_DB=emitlane", "-e", "POSTGRES_USER=emitlane", "-e", "POSTGRES_PASSWORD=emitlane", "postgres:16-alpine", "-c", "fsync=on", "-c", "synchronous_commit=on", "-c", "full_page_writes=on")
	raw, err := create.Output()
	if err != nil {
		t.Fatal(err)
	}
	id := strings.TrimSpace(string(raw))
	if !regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(id) {
		t.Fatalf("invalid owned restore fixture ID %q", id)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		if out, err := exec.CommandContext(cleanup, "docker", "rm", "--force", "--volumes", id).CombinedOutput(); err != nil {
			t.Errorf("remove owned restore fixture: %v: %s", err, out)
		}
	})
	f, err := os.Open(filepath.Join(output, "failure-postgres-data.tar.gz"))
	if err != nil {
		t.Fatal(err)
	}
	z, err := gzip.NewReader(f)
	if err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	copy := exec.CommandContext(ctx, "docker", "cp", "-", id+":/var/lib/postgresql/data")
	copy.Stdin = z
	out, err := copy.CombinedOutput()
	_ = z.Close()
	_ = f.Close()
	if err != nil {
		t.Fatalf("restore into isolated owned fixture: %v: %s", err, out)
	}
	if out, err := exec.CommandContext(ctx, "docker", "start", id).CombinedOutput(); err != nil {
		t.Fatalf("start isolated restore fixture: %v: %s", err, out)
	}
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		query, stop := context.WithTimeout(ctx, 2*time.Second)
		out, err = exec.CommandContext(query, "docker", "exec", id, "psql", "-U", "emitlane", "-d", "emitlane", "-At", "-c", `SELECT COUNT(*)::text || ':' || SUM(id)::text FROM public.qualification_capture_marker`).CombinedOutput()
		stop()
		if err == nil {
			if strings.TrimSpace(string(out)) != "2:83" {
				t.Fatalf("restored marker differs: %s", out)
			}
			t.Log("POSTGRES_CAPTURE_PROOF logical_dump_failed raw_stopped_archive_valid restored_records=2 isolated_network=none")
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("isolated restore fixture not ready within60s: %v: %s", err, out)
}
