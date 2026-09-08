package consumer

import (
	"testing"

	"github.com/google/uuid"
)

func TestResolveEventID(t *testing.T) {
	t.Parallel()
	id := uuid.New()
	tests := []struct {
		name    string
		headers []Header
		want    uuid.UUID
		wantErr bool
	}{
		{name: "one", headers: []Header{{Key: EventIDHeader, Value: []byte(id.String())}}, want: id},
		{name: "equal duplicates", headers: []Header{{Key: EventIDHeader, Value: []byte(id.String())}, {Key: EventIDHeader, Value: []byte(id.String())}}, want: id},
		{name: "missing", wantErr: true},
		{name: "invalid", headers: []Header{{Key: EventIDHeader, Value: []byte("invalid")}}, wantErr: true},
		{name: "conflicting", headers: []Header{{Key: EventIDHeader, Value: []byte(id.String())}, {Key: EventIDHeader, Value: []byte(uuid.New().String())}}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := ResolveEventID(Message{Headers: test.headers})
			if (err != nil) != test.wantErr || got != test.want {
				t.Fatalf("ResolveEventID() = %s, %v; want %s error=%t", got, err, test.want, test.wantErr)
			}
		})
	}
}
