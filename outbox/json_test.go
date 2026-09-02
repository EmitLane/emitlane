package outbox

import (
	"fmt"
	"testing"
)

func TestJSON(t *testing.T) {
	t.Parallel()
	payload, err := JSON(map[string]string{"order_id": "1"})
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != `{"order_id":"1"}` {
		t.Fatalf("got %s", payload)
	}
}

func TestJSONError(t *testing.T) {
	t.Parallel()
	_, err := JSON(make(chan int))
	if err == nil {
		t.Fatal("expected error")
	}
}

func ExampleJSON() {
	type OrderCreated struct {
		OrderID string `json:"order_id"`
	}
	payload, err := JSON(OrderCreated{OrderID: "ord_1"})
	if err != nil {
		panic(err)
	}
	fmt.Println(string(payload))
	// Output: {"order_id":"ord_1"}
}

func BenchmarkJSON(b *testing.B) {
	v := map[string]string{"order_id": "ord_1", "status": "created"}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := JSON(v); err != nil {
			b.Fatal(err)
		}
	}
}
