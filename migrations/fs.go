package migrations

import "embed"

// SQL contains the versioned EmitLane schema migrations.
//
//go:embed 000001_init.up.sql 000001_init.down.sql 000002_operability.up.sql 000002_operability.down.sql 000003_ordering.up.sql 000003_ordering.down.sql 000004_inbox_lifecycle.up.sql 000004_inbox_lifecycle.down.sql 000005_unordered_claim_index.up.sql 000005_unordered_claim_index.down.sql
var SQL embed.FS
