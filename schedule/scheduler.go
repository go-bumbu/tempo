package schedule

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/reugn/go-quartz/quartz"
)

// Cfg configures a Scheduler.
type Cfg struct {
	// Store persists schedules; must not be nil. Use NewMemStore for schedules
	// that need not survive a restart.
	Store Store
	// Enqueuer receives a task when a schedule fires; must not be nil.
	// *tempo.QueueRunner satisfies it directly.
	Enqueuer Enqueuer
	// Logger receives fire and skip messages. Nil uses slog.Default().
	Logger *slog.Logger
	// Location is the timezone cron expressions are evaluated in, server-wide.
	// Nil uses time.Local.
	Location *time.Location
}

// Scheduler fires tempo tasks on a cron timetable. It owns the write path: a
// Create, Update, SetEnabled or Delete both persists the change and updates the
// running jobs, so the store and the scheduler cannot drift apart.
type Scheduler struct {
	qs    quartz.Scheduler
	store Store
	enq   Enqueuer
	log   *slog.Logger

	// newTrigger builds the trigger for a cron expression. It is a field so
	// tests can substitute a fast quartz.SimpleTrigger rather than wait for
	// cron's one-second minimum granularity.
	newTrigger func(expr string) (quartz.Trigger, error)

	// mu serializes store writes with quartz job changes, so a failed write is
	// never observed half-applied.
	mu      sync.Mutex
	started atomic.Bool

	stopOnce sync.Once
	stopChan chan struct{}
}

// New creates a Scheduler. Call Start to load the stored schedules and begin
// firing.
func New(cfg Cfg) (*Scheduler, error) {
	if cfg.Store == nil {
		return nil, errors.New("schedule: store must not be nil")
	}
	if cfg.Enqueuer == nil {
		return nil, errors.New("schedule: enqueuer must not be nil")
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	loc := cfg.Location
	if loc == nil {
		loc = time.Local
	}
	qs, err := quartz.NewStdScheduler()
	if err != nil {
		return nil, fmt.Errorf("schedule: create quartz scheduler: %w", err)
	}
	return &Scheduler{
		qs:    qs,
		store: cfg.Store,
		enq:   cfg.Enqueuer,
		log:   log,
		newTrigger: func(expr string) (quartz.Trigger, error) {
			return quartz.NewCronTriggerWithLoc(expr, loc)
		},
		stopChan: make(chan struct{}),
	}, nil
}

// Start begins firing and loads the stored schedules. It returns an error when
// the store cannot be read: a scheduler silently running with no schedules is
// worse than one that refuses to boot. Individual rows whose cron no longer
// parses are skipped with a warning. Calling Start again is a no-op.
func (s *Scheduler) Start(ctx context.Context) error {
	if !s.started.CompareAndSwap(false, true) {
		return nil
	}
	s.qs.Start(ctx)
	return s.Reload(ctx)
}

// ShutDown stops firing and waits for in-flight fires to return. It returns
// ErrUnsafeStop when ctx expires first.
func (s *Scheduler) ShutDown(ctx context.Context) error {
	var err error
	s.stopOnce.Do(func() {
		s.qs.Stop()
		done := make(chan struct{})
		go func() {
			s.qs.Wait(context.Background())
			close(done)
		}()
		select {
		case <-done:
		case <-ctx.Done():
			err = ErrUnsafeStop
		}
		close(s.stopChan)
	})
	return err
}

// Wait blocks until the Scheduler has shut down.
func (s *Scheduler) Wait() {
	<-s.stopChan
}

// Reload rebuilds the running jobs from the store. Use it after something other
// than this Scheduler wrote to the store — a backup restore, manual SQL. Rows
// whose cron no longer parses are skipped with a warning rather than failing the
// whole reload, since the store may have been written by an older version.
func (s *Scheduler) Reload(ctx context.Context) error {
	list, err := s.store.List(ctx)
	if err != nil {
		return fmt.Errorf("schedule: list schedules: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.qs.Clear(); err != nil {
		return fmt.Errorf("schedule: clear jobs: %w", err)
	}
	for _, sch := range list {
		if !sch.Enabled {
			continue
		}
		if err := s.scheduleLocked(sch); err != nil {
			s.log.Warn("skipping schedule",
				slog.String("component", "tempo/schedule"),
				slog.String("scheduleId", sch.ID.String()),
				slog.String("task", sch.TaskName),
				slog.String("cron", sch.Cron),
				slog.String("error", err.Error()))
			continue
		}
	}
	return nil
}

// List returns every schedule, enabled or not, in store order.
func (s *Scheduler) List(ctx context.Context) ([]ScheduleInfo, error) {
	list, err := s.store.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("schedule: list schedules: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]ScheduleInfo, 0, len(list))
	for _, sch := range list {
		out = append(out, s.infoLocked(sch))
	}
	return out, nil
}

// Get returns one schedule, or ErrScheduleNotFound.
func (s *Scheduler) Get(ctx context.Context, id uuid.UUID) (ScheduleInfo, error) {
	sch, err := s.store.Get(ctx, id)
	if err != nil {
		return ScheduleInfo{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.infoLocked(sch), nil
}

// requireStarted guards the write methods. It must be called before taking mu.
func (s *Scheduler) requireStarted() error {
	if !s.started.Load() {
		return ErrNotStarted
	}
	return nil
}

// jobKey is the quartz key for a schedule: its uuid, so an edit touches exactly
// one job.
func jobKey(id uuid.UUID) *quartz.JobKey {
	return quartz.NewJobKey(id.String())
}

// scheduleLocked registers sch with quartz. Caller must hold mu.
func (s *Scheduler) scheduleLocked(sch Schedule) error {
	trigger, err := s.newTrigger(NormalizeCron(sch.Cron))
	if err != nil {
		return fmt.Errorf("schedule: invalid cron expression %q: %w", sch.Cron, err)
	}
	job := &enqueueJob{
		schedID:  sch.ID,
		taskName: sch.TaskName,
		params:   sch.Params,
		enq:      s.enq,
		log:      s.log,
	}
	if err := s.qs.ScheduleJob(quartz.NewJobDetail(job, jobKey(sch.ID)), trigger); err != nil {
		return fmt.Errorf("schedule: register job %s: %w", sch.ID, err)
	}
	return nil
}

// applyLocked makes the running jobs match sch: deregistered when disabled,
// freshly registered when enabled. Caller must hold mu.
func (s *Scheduler) applyLocked(sch Schedule) error {
	// Deregister any existing job, treating "not there" as success.
	if err := s.qs.DeleteJob(jobKey(sch.ID)); err != nil && !errors.Is(err, quartz.ErrJobNotFound) {
		return fmt.Errorf("schedule: deregister job %s: %w", sch.ID, err)
	}
	if !sch.Enabled {
		return nil
	}
	return s.scheduleLocked(sch)
}

// infoLocked decorates sch with its live next fire time. Caller must hold mu.
func (s *Scheduler) infoLocked(sch Schedule) ScheduleInfo {
	info := ScheduleInfo{Schedule: sch}
	if !sch.Enabled {
		return info
	}
	sj, err := s.qs.GetScheduledJob(jobKey(sch.ID))
	if err != nil {
		return info
	}
	if next := sj.NextRunTime(); next > 0 {
		info.NextFireAt = time.Unix(0, next)
	}
	return info
}
