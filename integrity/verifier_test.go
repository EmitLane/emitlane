package integrity

import (
	"testing"

	internalordering "github.com/emitlane/emitlane/internal/ordering"
)

func TestVerifierUsesV3PartitionMapping(t *testing.T) {
	t.Parallel()
	vectors := []struct {
		destination string
		key         string
		want        int16
	}{
		{destination: "orders.events", key: "order:123", want: 61},
		{destination: "orders.events", key: "order:124", want: 24},
		{destination: "payments.events", key: "order:123", want: 11},
	}
	for _, vector := range vectors {
		if got := internalordering.Partition(vector.destination, vector.key); got != vector.want {
			t.Fatalf("partition(%q,%q)=%d want %d", vector.destination, vector.key, got, vector.want)
		}
	}
}
