package postgres

import (
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestSanitizeErrorBoundsAndUTF8(t *testing.T) {
	t.Parallel()
	input := strings.Repeat("é", maxErrorBytes) + "\x00" + string([]byte{0xff})
	got := sanitizeError(input)
	if len(got) > maxErrorBytes {
		t.Fatalf("sanitized error is %d bytes, max %d", len(got), maxErrorBytes)
	}
	if !utf8.ValidString(got) {
		t.Fatal("sanitized error is not valid UTF-8")
	}
	if strings.ContainsRune(got, '\x00') {
		t.Fatal("sanitized error contains a PostgreSQL-incompatible NUL")
	}
}

func TestMapHeadersRejectsNonStringValues(t *testing.T) {
	t.Parallel()
	if _, err := mapHeaders([]byte(`{"source":42}`)); err == nil {
		t.Fatal("expected non-string header decode error")
	}
	headers, err := mapHeaders([]byte(`{"source":"orders"}`))
	if err != nil {
		t.Fatal(err)
	}
	if headers["source"] != "orders" {
		t.Fatalf("headers %v", headers)
	}
}

func TestIntervalMSRoundsPositiveDurationsUp(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   time.Duration
		want int64
	}{
		{name: "negative", in: -time.Nanosecond, want: 0},
		{name: "zero", in: 0, want: 0},
		{name: "sub-millisecond", in: time.Nanosecond, want: 1},
		{name: "exact millisecond", in: time.Millisecond, want: 1},
		{name: "partial millisecond", in: 1500 * time.Microsecond, want: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := intervalMS(tt.in); got != tt.want {
				t.Fatalf("intervalMS(%s) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}
