package migrations_test

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/emitlane/emitlane/migrations"
)

// These hashes anchor migrations shipped through v0.7.0. New schema changes
// belong in new numbered files; do not regenerate this baseline to edit old SQL.
func TestReleasedMigrationBytes(t *testing.T) {
	released := map[string]string{
		"000001_init.down.sql":            "6ae065b1ee90e2f74d19132f174544fcd5c4a3c5e20924b2ad0e647020a11753",
		"000001_init.up.sql":              "4aad9a177b13c7b7041c95425b9868d9c95ec78dfe4642b6479ac53d10c100d8",
		"000002_operability.down.sql":     "b909aa7ab3250e42b8d47a95cf0d4ab45126987832d13da5a442a445a7ea3c5f",
		"000002_operability.up.sql":       "1dfaf0e703372f0d092ba97ccc9e5295633760faae201fdbf83ad575cefd7525",
		"000003_ordering.down.sql":        "c759c4a74f7d9643fd0bd8608e6567ed3a12f2752f0c31fb9e8cf3e7c8cbc40c",
		"000003_ordering.up.sql":          "cd8fc9ba61a955140c91e92585889f2f70330871003f980e27e362f1c13daed4",
		"000004_inbox_lifecycle.down.sql": "73ba4126d4cbc3b34a8aabfd399012c6b4f9ef416ec5ce3c46b5900ba58d6a51",
		"000004_inbox_lifecycle.up.sql":   "c1390ebb4fbb67c4f41a8c4f43447a33395f42a7bd1ee550158174e5d9dc663c",
	}
	for name, want := range released {
		t.Run(name, func(t *testing.T) {
			data, err := migrations.SQL.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			if got := fmt.Sprintf("%x", sha256.Sum256(data)); got != want {
				t.Fatalf("released migration changed: got SHA256 %s, want %s; add a new migration", got, want)
			}
		})
	}
}
