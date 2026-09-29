package netdrill

import (
	"sort"
	"sync"
	"time"
)

// Clock is the time source used by the simulated link. Production code can
// pass a wall-clock implementation; tests and deterministic replays use
// SimClock, which only advances when Run/Advance is driven.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
	AfterFunc(d time.Duration, fn func())
}

// SimClock is a manually driven, goroutine-safe virtual clock. Timers only
// fire when the clock is advanced, which makes replay timing deterministic.
type SimClock struct {
	mu      sync.Mutex
	now     time.Time
	seq     uint64
	timers  []simTimer
	running bool
}

type simTimer struct {
	at  time.Time
	seq uint64
	ch  chan time.Time
	fn  func()
}

// NewSimClock returns a virtual clock anchored at the Unix epoch so that
// recorded timestamps are easy to diff across runs.
func NewSimClock() *SimClock {
	return &SimClock{now: time.Unix(0, 0)}
}

// Now returns the current virtual time.
func (c *SimClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// After schedules a one-shot timer. Non-positive durations fire on the next
// Advance/Run step rather than immediately, keeping delivery ordering inside
// the engine deterministic.
func (c *SimClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	c.AfterFunc(d, func() { ch <- c.Now() })
	return ch
}

// AfterFunc schedules fn to run synchronously when the clock reaches the
// deadline. Callbacks fire in deadline/registration order, which makes
// simulated arrivals deterministic.
func (c *SimClock) AfterFunc(d time.Duration, fn func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if d < 0 {
		d = 0
	}
	c.seq++
	t := simTimer{at: c.now.Add(d), seq: c.seq, fn: fn}
	c.timers = append(c.timers, t)
}

// Advance moves virtual time forward by d and fires every timer whose deadline
// has been reached. Timers at the same instant fire in scheduling order.
func (c *SimClock) Advance(d time.Duration) {
	c.mu.Lock()
	if d < 0 {
		d = 0
	}
	c.now = c.now.Add(d)
	due := c.takeDue(c.now)
	c.mu.Unlock()
	c.fire(due)
}

// AdvanceNext jumps directly to the earliest pending timer (firing all timers
// due at that instant) and returns true. It returns false when nothing is
// pending, leaving the clock unchanged.
func (c *SimClock) AdvanceNext() bool {
	c.mu.Lock()
	if len(c.timers) == 0 {
		c.mu.Unlock()
		return false
	}
	sort.Slice(c.timers, func(i, j int) bool {
		if c.timers[i].at.Equal(c.timers[j].at) {
			return c.timers[i].seq < c.timers[j].seq
		}
		return c.timers[i].at.Before(c.timers[j].at)
	})
	next := c.timers[0].at
	if next.After(c.now) {
		c.now = next
	}
	due := c.takeDue(c.now)
	c.mu.Unlock()
	c.fire(due)
	return true
}

// Run advances the clock until no timers remain (or until f returns false via
// RunUntil), firing due timers at their scheduled instants. It returns the
// virtual time reached.
func (c *SimClock) Run() time.Time {
	for {
		c.mu.Lock()
		if len(c.timers) == 0 {
			now := c.now
			c.running = false
			c.mu.Unlock()
			return now
		}
		sort.Slice(c.timers, func(i, j int) bool {
			if c.timers[i].at.Equal(c.timers[j].at) {
				return c.timers[i].seq < c.timers[j].seq
			}
			return c.timers[i].at.Before(c.timers[j].at)
		})
		next := c.timers[0].at
		if next.After(c.now) {
			c.now = next
		}
		due := c.takeDue(c.now)
		c.mu.Unlock()
		c.fire(due)
	}
}

// Pending reports whether timers are still scheduled.
func (c *SimClock) Pending() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers) > 0
}

func (c *SimClock) takeDue(now time.Time) []simTimer {
	var due []simTimer
	remaining := c.timers[:0]
	for _, t := range c.timers {
		if !t.at.After(now) {
			due = append(due, t)
		} else {
			remaining = append(remaining, t)
		}
	}
	c.timers = remaining
	sort.Slice(due, func(i, j int) bool {
		if due[i].at.Equal(due[j].at) {
			return due[i].seq < due[j].seq
		}
		return due[i].at.Before(due[j].at)
	})
	return due
}

func (c *SimClock) fire(due []simTimer) {
	for _, t := range due {
		if t.fn != nil {
			t.fn()
		} else {
			t.ch <- t.at
		}
	}
}

// WallClock adapts the real system clock.
type WallClock struct{}

// Now returns the current wall-clock time.
func (WallClock) Now() time.Time { return time.Now() }

// After wraps time.After.
func (WallClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// AfterFunc wraps time.AfterFunc.
func (WallClock) AfterFunc(d time.Duration, fn func()) { time.AfterFunc(d, fn) }
