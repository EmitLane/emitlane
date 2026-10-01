package postgres

import (
	"errors"
	"testing"
)

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
		{name: "current", versions: []int{4, 2, 1, 3}, want: 4},
		{name: "future", versions: []int{1, 2, 3, 4, 5}, want: 5, invalid: true},
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
