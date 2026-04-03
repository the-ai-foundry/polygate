package sink

import (
	"context"
	"fmt"
	"time"
)

// WithRetry executes op up to maxRetries times with exponential backoff.
// baseMS is the base backoff in milliseconds (doubles each attempt).
func WithRetry(ctx context.Context, maxRetries int, baseMS int, op func() error) error {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(baseMS<<uint(attempt)) * time.Millisecond
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if err := op(); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return fmt.Errorf("failed after %d attempts: %w", maxRetries, lastErr)
}
