package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fatal(errors.New("usage: emitlane-soak start|run|relay|status|logs|stop|report"))
	}
	var err error
	switch os.Args[1] {
	case "start":
		err = startCommand(os.Args[2:])
	case "run":
		err = runCommand(os.Args[2:])
	case "relay":
		err = relayCommand(os.Args[2:])
	case "status":
		err = statusCommand(os.Args[2:])
	case "logs":
		err = logsCommand(os.Args[2:])
	case "stop":
		err = stopCommand(os.Args[2:])
	case "report":
		err = reportCommand(os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		var coded *exitError
		if errors.As(err, &coded) {
			fmt.Fprintln(os.Stderr, "emitlane-soak:", coded.err)
			os.Exit(coded.code)
		}
		fatal(err)
	}
}

func fatal(err error) { fmt.Fprintln(os.Stderr, "emitlane-soak:", err); os.Exit(1) }

type exitError struct {
	code int
	err  error
}

func (e *exitError) Error() string { return e.err.Error() }
func (e *exitError) Unwrap() error { return e.err }

func rootFlag(_ string, args []string) (string, []string) {
	root := soakRoot
	for i := 0; i < len(args); i++ {
		if args[i] == "--root" && i+1 < len(args) {
			root = args[i+1]
			args = append(args[:i], args[i+2:]...)
			break
		}
		if strings.HasPrefix(args[i], "--root=") {
			root = strings.TrimPrefix(args[i], "--root=")
			args = append(args[:i], args[i+1:]...)
			break
		}
	}
	return root, args
}

func startCommand(args []string) error {
	root, args := rootFlag("start", args)
	fs := flag.NewFlagSet("start", flag.ContinueOnError)
	profileName := fs.String("profile", "quick", "quick, standard, or release")
	duration := fs.Duration("duration", 0, "override running duration")
	recovery := fs.Duration("recovery-timeout", 0, "override recovery timeout")
	seedFlag := fs.Uint64("seed", 0, "reproducible random seed")
	relays := fs.Int("relays", 0, "override relay count")
	streams := fs.Int("streams", 0, "override ordered stream count")
	rate := fs.Int("rate", 0, "override target events per second")
	allowDirty := fs.Bool("allow-dirty", false, "allow a dirty release-profile run (not valid release evidence)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := profile(strings.ToLower(*profileName))
	if err != nil {
		return err
	}
	if *duration > 0 {
		cfg.Duration = *duration
	}
	if *recovery > 0 {
		cfg.RecoveryTimeout = *recovery
	}
	if *relays > 0 {
		cfg.Relays = *relays
	}
	if *streams > 0 {
		cfg.OrderedStreams = *streams
	}
	if *rate > 0 {
		cfg.EventsPerSecond = *rate
	}
	if *seedFlag == 0 {
		cfg.Seed, err = newSeed()
		if err != nil {
			return err
		}
	} else {
		cfg.Seed = *seedFlag
	}
	cfg.AllowDirty = *allowDirty
	if err := cfg.validate(); err != nil {
		return err
	}
	provenance, err := gitProvenance(".")
	if err != nil {
		return fmt.Errorf("inspect Git provenance: %w", err)
	}
	cfg.Source = provenance
	if err := validateReleaseProvenance(cfg); err != nil {
		return err
	}
	if runDir, currentErr := currentRun(root); currentErr == nil {
		if pid, pidErr := readPID(runDir); pidErr == nil && processMatches(pid, runDir) {
			return fmt.Errorf("soak already running (PID %d, %s)", pid, runDir)
		}
	}
	cfg.RunID = newRunID(time.Now(), cfg.Seed)
	runDir, err := filepath.Abs(filepath.Join(root, cfg.RunID))
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(runDir), 0o755); err != nil {
		return err
	}
	if err := os.Mkdir(runDir, 0o755); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(runDir, "config.json"), cfg); err != nil {
		return err
	}
	state := State{RunID: cfg.RunID, State: "running", Phase: "initializing", UpdatedAt: time.Now().UTC()}
	if err := writeJSON(filepath.Join(runDir, "state.json"), state); err != nil {
		return err
	}
	if err := writeCurrent(root, cfg.RunID); err != nil {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	logFile, err := os.OpenFile(filepath.Join(runDir, "soak.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer logFile.Close()
	cmd := exec.Command(executable, "run", "--run-dir", runDir)
	cmd.Stdout, cmd.Stderr, cmd.Stdin = logFile, logFile, nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(runDir, "pid"), []byte(strconv.Itoa(cmd.Process.Pid)+"\n"), 0o644); err != nil {
		_ = cmd.Process.Kill()
		return err
	}
	fmt.Printf("EmitLane soak started\nRun ID: %s\nPID: %d\nProfile: %s\nOutput: %s\n", cfg.RunID, cmd.Process.Pid, cfg.Profile, runDir)
	return nil
}

func runCommand(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	runDir := fs.String("run-dir", "", "isolated run directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *runDir == "" {
		return errors.New("--run-dir is required")
	}
	var cfg Config
	if err := readJSON(filepath.Join(*runDir, "config.json"), &cfg); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runSoak(ctx, *runDir, cfg)
}

func statusCommand(args []string) error {
	root, args := rootFlag("status", args)
	if len(args) != 0 {
		return errors.New("status accepts only --root")
	}
	runDir, err := currentRun(root)
	if err != nil {
		return fmt.Errorf("no current soak: %w", err)
	}
	var progress Progress
	if err := readJSON(filepath.Join(runDir, "progress.json"), &progress); err != nil {
		var state State
		if stateErr := readJSON(filepath.Join(runDir, "state.json"), &state); stateErr != nil {
			return err
		}
		progress = Progress{RunID: state.RunID, State: state.State, Phase: state.Phase, UpdatedAt: state.UpdatedAt}
	}
	pid, pidErr := readPID(runDir)
	active := progress.State == "running" && (progress.Phase == "initializing" || progress.Phase == "warmup" || progress.Phase == "running" || progress.Phase == "recovering" || progress.Phase == "verifying")
	if active && (pidErr != nil || !processMatches(pid, runDir)) {
		progress.State, progress.Phase = "crashed", "crashed"
		active = false
	}
	fmt.Printf("Run ID:          %s\nState:           %s\nPhase:           %s\nElapsed:         %s\nProfile:         %s\n\n", progress.RunID, progress.State, progress.Phase, durationText(progress.Elapsed), progress.Profile)
	fmt.Printf("Committed:       %d\nObserved unique: %d\nKafka records:   %d\nDuplicates:      %d\n", progress.CommittedEvents, progress.ObservedUnique, progress.BrokerRecords, progress.DuplicateRecords)
	if active {
		fmt.Printf("Not observed yet: %d\n", progress.NotObservedYet)
	}
	fmt.Printf("\nStreams:         %d\nRelays:          %d\nRelay restarts:  %d\nKafka outages:   %d\nPause cycles:    %d\n\nOrdering regressions: %d\n", progress.OrderedStreams, progress.Relays, progress.RelayRestarts, progress.KafkaOutages, progress.PauseCycles, progress.OrderingRegressions)
	return nil
}

func logsCommand(args []string) error {
	root, args := rootFlag("logs", args)
	if len(args) != 0 {
		return errors.New("logs accepts only --root")
	}
	runDir, err := currentRun(root)
	if err != nil {
		return err
	}
	return followFile(filepath.Join(runDir, "soak.log"), os.Stdout)
}

func stopCommand(args []string) error {
	root, args := rootFlag("stop", args)
	if len(args) != 0 {
		return errors.New("stop accepts only --root")
	}
	runDir, err := currentRun(root)
	if err != nil {
		return err
	}
	pid, err := readPID(runDir)
	if err != nil {
		return err
	}
	if !processMatches(pid, runDir) {
		return fmt.Errorf("recorded PID %d is stale; no signal sent", pid)
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	if err := process.Signal(syscall.SIGTERM); err != nil {
		return err
	}
	fmt.Printf("Stopping EmitLane soak %s (PID %d) gracefully...\n", filepath.Base(runDir), pid)
	deadline := time.Now().Add(2 * time.Minute)
	for processAlive(pid) && time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
	}
	if processAlive(pid) {
		return errors.New("soak did not exit within 2 minutes; it was not force-killed")
	}
	fmt.Println("EmitLane soak stopped; artifacts were preserved in", runDir)
	return nil
}

func reportCommand(args []string) error {
	root, args := rootFlag("report", args)
	if len(args) != 0 {
		return errors.New("report accepts only --root")
	}
	runDir, err := currentRun(root)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(filepath.Join(runDir, "report.md"))
	if err != nil {
		return fmt.Errorf("report is not ready: %w", err)
	}
	_, err = os.Stdout.Write(data)
	return err
}
