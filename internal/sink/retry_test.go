package sink

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWithRetrySuccess(t *testing.T) {
	calls := 0
	err := WithRetry(context.Background(), 3, 10, func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}
}

func TestWithRetryEventualSuccess(t *testing.T) {
	calls := 0
	err := WithRetry(context.Background(), 3, 10, func() error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 3 {
		t.Errorf("expected 3 calls, got %d", calls)
	}
}

func TestWithRetryAllFail(t *testing.T) {
	calls := 0
	err := WithRetry(context.Background(), 3, 10, func() error {
		calls++
		return errors.New("permanent")
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if calls != 3 {
		t.Errorf("expected 3 calls, got %d", calls)
	}
}

func TestWithRetryContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	calls := 0
	err := WithRetry(ctx, 5, 10, func() error {
		calls++
		return errors.New("fail")
	})
	// Should bail out quickly due to cancelled context.
	if err == nil {
		t.Fatal("expected error")
	}
	if calls > 2 {
		t.Errorf("expected early exit, got %d calls", calls)
	}
}

func TestWithRetryBackoffTiming(t *testing.T) {
	start := time.Now()
	calls := 0
	WithRetry(context.Background(), 3, 50, func() error {
		calls++
		return errors.New("fail")
	})
	elapsed := time.Since(start)
	// With baseMS=50: attempt 1 = 100ms, attempt 2 = 200ms → ~300ms total.
	if elapsed < 200*time.Millisecond {
		t.Errorf("expected backoff to take at least 200ms, took %v", elapsed)
	}
}
