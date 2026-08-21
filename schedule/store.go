package schedule

import (
	"context"
	"slices"
	"strings"
	"sync"

	"github.com/google/uuid"
)

// Store persists schedules. It is deliberately dumb: no cron knowledge, no
// concurrency contract beyond being safe for the Scheduler's serialized calls,
// no shutdown. This mirrors tempo.TaskStatePersistence in the core package.
//
// There is no ListEnabled: the Scheduler filters in memory from List, since it
// needs the full set anyway to serve a UI.
type Store interface {
	// List returns every schedule, including disabled ones, ordered by
	// CreatedAt (ties broken by id) so callers get a stable order.
	List(ctx context.Context) ([]Schedule, error)
	// Get returns the schedule for id, or ErrScheduleNotFound.
	Get(ctx context.Context, id uuid.UUID) (Schedule, error)
	// Save upserts by id.
	Save(ctx context.Context, s Schedule) error
	// Delete removes the schedule, or returns ErrScheduleNotFound.
	Delete(ctx context.Context, id uuid.UUID) error
}

// MemStore is an in-memory Store, used when schedules need not survive a
// restart and by tests. It is safe for concurrent use.
//
// It clones Schedule.Params on the way in and out, so a caller mutating the
// params of a stored or returned Schedule cannot reach into the store. A store
// that serializes — dbschedule — gets that for free; MemStore has to do it by
// hand to stay interchangeable with one.
type MemStore struct {
	mu   sync.Mutex
	data map[uuid.UUID]Schedule
}

// cloneSchedule copies the one field of a Schedule that is not copied by
// assignment.
func cloneSchedule(s Schedule) Schedule {
	s.Params = slices.Clone(s.Params)
	return s
}

// NewMemStore returns an empty in-memory store.
func NewMemStore() *MemStore {
	return &MemStore{data: make(map[uuid.UUID]Schedule)}
}

func (m *MemStore) List(_ context.Context) ([]Schedule, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Schedule, 0, len(m.data))
	for _, s := range m.data {
		out = append(out, cloneSchedule(s))
	}
	slices.SortFunc(out, func(a, b Schedule) int {
		if c := a.CreatedAt.Compare(b.CreatedAt); c != 0 {
			return c
		}
		return strings.Compare(a.ID.String(), b.ID.String())
	})
	return out, nil
}

func (m *MemStore) Get(_ context.Context, id uuid.UUID) (Schedule, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.data[id]
	if !ok {
		return Schedule{}, ErrScheduleNotFound
	}
	return cloneSchedule(s), nil
}

func (m *MemStore) Save(_ context.Context, s Schedule) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[s.ID] = cloneSchedule(s)
	return nil
}

func (m *MemStore) Delete(_ context.Context, id uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.data[id]; !ok {
		return ErrScheduleNotFound
	}
	delete(m.data, id)
	return nil
}
