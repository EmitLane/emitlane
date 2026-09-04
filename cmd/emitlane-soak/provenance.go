package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
)

func gitProvenance(repo string) (GitProvenance, error) {
	commit, err := gitOutput(repo, "rev-parse", "HEAD")
	if err != nil {
		return GitProvenance{}, err
	}
	branch, err := gitOutput(repo, "branch", "--show-current")
	if err != nil {
		return GitProvenance{}, err
	}
	if branch == "" {
		branch = "HEAD"
	}
	status, err := gitBytes(repo, "status", "--porcelain=v1", "-z", "--untracked-files=all")
	if err != nil {
		return GitProvenance{}, err
	}
	provenance := GitProvenance{Commit: commit, Branch: branch, Dirty: len(status) != 0}
	if provenance.Dirty {
		provenance.DiffSHA256, err = gitTreeDigest(repo)
		if err != nil {
			return GitProvenance{}, err
		}
	}
	return provenance, nil
}

func validateReleaseProvenance(cfg Config) error {
	if cfg.Profile == "release" && cfg.Source.Dirty && !cfg.AllowDirty {
		return errors.New("release profile requires a clean Git working tree; --allow-dirty is developer-only and cannot produce valid release evidence")
	}
	return nil
}

func gitTreeDigest(repo string) (string, error) {
	diff, err := gitBytes(repo, "diff", "--binary", "HEAD", "--")
	if err != nil {
		return "", err
	}
	untracked, err := gitBytes(repo, "ls-files", "--others", "--exclude-standard", "-z")
	if err != nil {
		return "", err
	}
	paths := bytes.Split(bytes.TrimSuffix(untracked, []byte{0}), []byte{0})
	sort.Slice(paths, func(i, j int) bool { return bytes.Compare(paths[i], paths[j]) < 0 })
	hash := sha256.New()
	_, _ = hash.Write([]byte("tracked-diff\x00"))
	_, _ = hash.Write(diff)
	for _, rawPath := range paths {
		if len(rawPath) == 0 {
			continue
		}
		path := string(rawPath)
		contents, readErr := os.ReadFile(repo + string(os.PathSeparator) + path)
		if readErr != nil {
			return "", fmt.Errorf("read untracked file %q: %w", path, readErr)
		}
		_, _ = hash.Write([]byte("untracked\x00" + path + "\x00"))
		_, _ = hash.Write(contents)
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func gitOutput(repo string, args ...string) (string, error) {
	out, err := gitBytes(repo, args...)
	return strings.TrimSpace(string(out)), err
}

func gitBytes(repo string, args ...string) ([]byte, error) {
	commandArgs := append([]string{"-C", repo}, args...)
	out, err := exec.Command("git", commandArgs...).Output()
	if err != nil {
		return nil, fmt.Errorf("git %s: %w", strings.Join(args, " "), err)
	}
	return out, nil
}
