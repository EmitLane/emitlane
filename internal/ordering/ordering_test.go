package ordering

import "testing"

func TestPartitionVectors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		destination string
		key         string
		want        int16
	}{
		{destination: "orders.events", key: "order:123", want: 61},
		{destination: "orders.events", key: "order:124", want: 24},
		{destination: "payments.events", key: "order:123", want: 11},
		{destination: "a", key: "bc", want: 35},
		{destination: "ab", key: "c", want: 39},
	}
	for _, tt := range tests {
		if got := Partition(tt.destination, tt.key); got != tt.want {
			t.Errorf("Partition(%q, %q) = %d, want %d", tt.destination, tt.key, got, tt.want)
		}
	}
}

func TestPartitionUsesDestinationAndSeparator(t *testing.T) {
	t.Parallel()
	if Partition("a", "bc") == Partition("ab", "c") {
		t.Fatal("separator must distinguish ambiguous concatenations")
	}
	if Partition("orders.a", "same") == Partition("orders.b", "same") {
		t.Fatal("destination must participate in partition mapping")
	}
}

func TestDesiredOwnerIsStableAndMembershipSensitive(t *testing.T) {
	t.Parallel()
	members := []string{"relay-c", "relay-a", "relay-b"}
	for partition := int16(0); partition < PartitionCount; partition++ {
		first := DesiredOwner(partition, members)
		second := DesiredOwner(partition, []string{"relay-b", "relay-c", "relay-a"})
		if first == "" || first != second {
			t.Fatalf("partition %d owner is not deterministic: %q != %q", partition, first, second)
		}
	}
	if got := DesiredOwner(0, nil); got != "" {
		t.Fatalf("empty membership owner = %q", got)
	}
}
