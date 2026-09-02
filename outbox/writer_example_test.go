package outbox_test

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/emitlane/emitlane/outbox"
)

func ExampleWriter_Enqueue() {
	enqueueOrder := func(ctx context.Context, tx pgx.Tx, orderID string) error {
		payload, err := outbox.JSON(struct {
			OrderID string `json:"order_id"`
		}{OrderID: orderID})
		if err != nil {
			return err
		}

		writer := outbox.NewWriter()
		_, err = writer.Enqueue(ctx, tx, outbox.Event{
			Destination: "orders.events",
			Type:        "order.created",
			Key:         []byte(orderID),
			Payload:     payload,
		})
		return err
	}

	_ = enqueueOrder // Call inside the same transaction as the business write.
}
