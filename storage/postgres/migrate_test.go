package postgres

import (
	"errors"
	"testing"
)

// A new numbered SQL file must be embedded in the binary, in both directions,
// before CurrentSchemaVersion advertises that migration to startup and doctor.
func TestEmbeddedMigrationsMatchCurrentSchema(t *testing.T) {
	for _, suffix := range []string{".up.sql", ".down.sql"} {
		entries, err := migrationFiles(suffix)
		if err != nil {
			t.Fatal(err)
		}
		seen := make(map[int]bool)
		for _, entry := range entries {
			version, err := versionOf(entry.Name())
			if err != nil {
				t.Fatal(err)
			}
			if version < 1 || version > CurrentSchemaVersion() || seen[version] {
				t.Fatalf("unexpected or duplicate embedded migration %s", entry.Name())
			}
			seen[version] = true
		}
		for version := 1; version <= CurrentSchemaVersion(); version++ {
			if !seen[version] {
				t.Errorf("schema %d advertises missing embedded migration %d%s", CurrentSchemaVersion(), version, suffix)
			}
		}
	}
}

func TestValidateMigrationHistory(t *testing.T) {
	tests := []struct {
		name     string
		versions []int
		want     int
		invalid  bool
	}{
		{name: "empty"},
		{name: "initial", versions: []int{1}, want: 1},
		{name: "older", versions: []int{3, 1, 2}, want: 3},
		{name: "previous", versions: []int{4, 2, 1, 3}, want: 4},
		{name: "current", versions: []int{5, 4, 2, 1, 3}, want: 5},
		{name: "future", versions: []int{1, 2, 3, 4, 5, 6}, want: 6, invalid: true},
		{name: "missing middle", versions: []int{1, 3, 4}, want: 4, invalid: true},
		{name: "missing initial", versions: []int{2, 3, 4}, want: 4, invalid: true},
		{name: "zero", versions: []int{0, 1, 2, 3, 4}, want: 4, invalid: true},
		{name: "negative", versions: []int{-1}, invalid: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			versions := make(map[int]bool)
			for _, v := range tt.versions {
				versions[v] = true
			}
			got, err := validateVersions(versions)
			if got != tt.want || errors.Is(err, ErrSchemaIncompatible) != tt.invalid {
				t.Fatalf("validateVersions() = %d, %v; want %d, invalid=%v", got, err, tt.want, tt.invalid)
			}
		})
	}
}
