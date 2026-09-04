package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func readJSON(path string, value any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, value)
}

func currentRun(root string) (string, error) {
	data, err := os.ReadFile(filepath.Join(root, "current"))
	if err != nil {
		return "", err
	}
	runID := strings.TrimSpace(string(data))
	if runID == "" || strings.ContainsAny(runID, `/\\`) {
		return "", errors.New("invalid current run id")
	}
	return filepath.Join(root, runID), nil
}

func writeCurrent(root, runID string) error {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(root, "current"), []byte(runID+"\n"), 0o644)
}

func readPID(runDir string) (int, error) {
	data, err := os.ReadFile(filepath.Join(runDir, "pid"))
	if err != nil {
		return 0, err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 1 {
		return 0, errors.New("invalid soak PID")
	}
	return pid, nil
}

func processAlive(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = process.Signal(syscall.Signal(0))
	return err == nil || errors.Is(err, os.ErrPermission)
}

func processMatches(pid int, runDir string) bool {
	if !processAlive(pid) {
		return false
	}
	out, err := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "command=").Output()
	if err != nil {
		return false
	}
	command := string(out)
	return strings.Contains(command, "emitlane-soak") && strings.Contains(command, runDir)
}

func durationText(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	seconds := int64(d.Seconds())
	return fmt.Sprintf("%02d:%02d:%02d", seconds/3600, (seconds/60)%60, seconds%60)
}

func followFile(path string, out io.Writer) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := io.Copy(out, file); err != nil {
		return err
	}
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		if _, err := io.Copy(out, file); err != nil {
			return err
		}
	}
	return nil
}
