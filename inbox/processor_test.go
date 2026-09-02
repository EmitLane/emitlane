package inbox

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestProcessRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	fn := func(context.Context, pgx.Tx) error { t.Fatal("must not run"); return nil }
	if err := Process(context.Background(), nil, "", "not-a-uuid", fn); err == nil {
		t.Fatal("expected error")
	}
	if err := Process(context.Background(), nil, "billing", "not-a-uuid", fn); err == nil {
		t.Fatal("expected error")
	}
}
