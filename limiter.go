package tempo

import "sync"

// exclusionGroupCap is the capacity of an exclusion group: at most one task
// whose registration names the group runs at a time. A future
// WithConcurrencyGroup(name, n) would carry its own per-group capacity here — a
// value change, not a structural one.
const exclusionGroupCap = 1

// limiter bounds how many tasks run concurrently per key. A claim reserves a set
// of keyed slots atomically (all-or-nothing) and hands back a release func that
// frees exactly those slots. All state is in-process; there is no cross-process
// coordination.
//
// Two key spaces that never collide: task names (capacity from
// WithMaxParallelism; 0 means unlimited) and exclusion groups (capacity
// exclusionGroupCap).
type limiter struct {
	mu     sync.Mutex
	names  map[string]int
	groups map[string]int
}

func newLimiter() *limiter {
	return &limiter{
		names:  make(map[string]int),
		groups: make(map[string]int),
	}
}

// tryAcquire reserves a slot for name (capacity nameLimit; 0 means unlimited)
// and, when group is non-empty, a slot in group (capacity exclusionGroupCap),
// atomically. On success it returns a release func and true. If either slot is
// at capacity it reserves nothing and returns (nil, false), so a partial
// reservation can never leak. The returned release is idempotent.
func (l *limiter) tryAcquire(name string, nameLimit int, group string) (func(), bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if nameLimit > 0 && l.names[name] >= nameLimit {
		return nil, false
	}
	if group != "" && l.groups[group] >= exclusionGroupCap {
		return nil, false
	}

	l.names[name]++
	if group != "" {
		l.groups[group]++
	}

	var once sync.Once
	release := func() {
		once.Do(func() {
			l.mu.Lock()
			defer l.mu.Unlock()
			l.names[name]--
			if l.names[name] <= 0 {
				delete(l.names, name)
			}
			if group != "" {
				l.groups[group]--
				if l.groups[group] <= 0 {
					delete(l.groups, group)
				}
			}
		})
	}
	return release, true
}
