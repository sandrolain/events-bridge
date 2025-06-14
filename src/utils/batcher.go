package utils

import (
	"time"
)

func Batcher[T any](in <-chan T, maxBatch int, maxWait time.Duration) <-chan []T {
	out := make(chan []T)
	go func() {
		defer close(out)
		batch := make([]T, 0, maxBatch)
		timer := time.NewTimer(maxWait)
		defer timer.Stop()
		resetTimer := func() {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(maxWait)
		}
		flush := func() {
			if len(batch) > 0 {
				out <- batch
				batch = make([]T, 0, maxBatch)
			}
		}
		for {
			select {
			case v, ok := <-in:
				if !ok {
					flush()
					return
				}
				batch = append(batch, v)
				if len(batch) >= maxBatch {
					flush()
					resetTimer()
				}
			case <-timer.C:
				flush()
				resetTimer()
			}
		}
	}()
	return out
}
