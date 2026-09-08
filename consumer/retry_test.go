package consumer

import (
	"errors"
	"testing"
	"time"

	"github.com/emitlane/emitlane/inbox"
)

func TestRetryDelay(t *testing.T) {
	t.Parallel()
	base := time.Second
	maximum := 5 * time.Second
	tests := []struct {
		attempt int
		jitter  float64
		sample  float64
		want    time.Duration
	}{
		{attempt: 1, want: time.Second},
		{attempt: 2, want: 2 * time.Second},
		{attempt: 4, want: 5 * time.Second},
		{attempt: 3, jitter: 1, sample: 0.25, want: time.Second},
	}
	for _, test := range tests {
		if got := retryDelay(test.attempt, base, maximum, test.jitter, test.sample); got != test.want {
			t.Fatalf("attempt %d delay=%s, want %s", test.attempt, got, test.want)
		}
	}
}

func TestPermanentClassification(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("bad record")
	err := inbox.Permanent(sentinel)
	if !inbox.IsPermanent(err) || !errors.Is(err, sentinel) {
		t.Fatalf("permanent error chain = %v", err)
	}
	if inbox.Permanent(nil) != nil || inbox.IsPermanent(sentinel) {
		t.Fatal("unexpected permanent classification")
	}
}
