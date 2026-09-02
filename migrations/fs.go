package migrations

import "embed"

// SQL contains the versioned EmitLane schema migrations.
//
//go:embed 000001_init.up.sql 000001_init.down.sql 000002_operability.up.sql 000002_operability.down.sql
var SQL embed.FS
