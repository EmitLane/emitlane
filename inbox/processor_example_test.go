package inbox_test

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/emitlane/emitlane/inbox"
)

func ExampleProcess() {
	processPayment := func(ctx context.Context, tx pgx.Tx, eventID, orderID string) error {
		return inbox.Process(ctx, tx, "payments", eventID,
			func(ctx context.Context, tx pgx.Tx) error {
				_, err := tx.Exec(ctx, `INSERT INTO payments (order_id) VALUES ($1)`, orderID)
				return err
			},
		)
	}

	_ = processPayment // Commit this transaction before committing the Kafka offset.
}
