package schedule

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
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

	// life bounds the quartz scheduler's execution loop. It is rooted at
	// context.Background() rather than at Start's argument on purpose: go-quartz
	// stops firing as soon as the context it was started with is done, and a
	// caller's per-call ctx must not be able to kill the Scheduler behind its
	// back. Only ShutDown ends it.
	life       context.Context
	lifeCancel context.CancelFunc

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
	life, lifeCancel := context.WithCancel(context.Background())
	return &Scheduler{
		qs:    qs,
		store: cfg.Store,
		enq:   cfg.Enqueuer,
		log:   log,
		newTrigger: func(expr string) (quartz.Trigger, error) {
			return quartz.NewCronTriggerWithLoc(expr, loc)
		},
		life:       life,
		lifeCancel: lifeCancel,
		stopChan:   make(chan struct{}),
	}, nil
}

// Start begins firing and loads the stored schedules.
//
// ctx bounds the initial read of the store only; it does not bound the
// Scheduler's lifetime. Only ShutDown stops firing. That makes the usual wiring
// pattern safe:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
//	defer cancel()
//	if err := sched.Start(ctx); err != nil { ... }
//
// The deferred cancel bounds the store read and nothing else — the Scheduler
// keeps firing after Start returns.
//
// Start returns an error when the store cannot be read: a scheduler silently
// running with no schedules is worse than one that refuses to boot. A failed
// Start leaves the Scheduler stopped rather than half-up, so a retry genuinely
// retries. Individual rows whose cron no longer parses are skipped with a
// warning. Calling Start again after it has succeeded is a no-op.
func (s *Scheduler) Start(ctx context.Context) error {
	if !s.started.CompareAndSwap(false, true) {
		return nil
	}
	s.qs.Start(s.life)
	if err := s.Reload(ctx); err != nil {
		// Unwind: leaving quartz running with zero jobs and started=true would
		// let every write method through and make a retried Start a silent no-op.
		s.qs.Stop()
		s.started.Store(false)
		return err
	}
	return nil
}

// ShutDown stops firing and waits for in-flight fires to return. It returns
// ErrUnsafeStop when ctx expires first.
func (s *Scheduler) ShutDown(ctx context.Context) error {
	var err error
	s.stopOnce.Do(func() {
		s.qs.Stop()
		s.lifeCancel()
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

// deleteJobLocked deregisters a job, treating "not there" as success. Caller
// must hold mu.
func (s *Scheduler) deleteJobLocked(id uuid.UUID) error {
	if err := s.qs.DeleteJob(jobKey(id)); err != nil && !errors.Is(err, quartz.ErrJobNotFound) {
		return fmt.Errorf("schedule: deregister job %s: %w", id, err)
	}
	return nil
}

// applyLocked makes the running jobs match sch: deregistered when disabled,
// freshly registered when enabled. Caller must hold mu.
func (s *Scheduler) applyLocked(sch Schedule) error {
	if err := s.deleteJobLocked(sch.ID); err != nil {
		return err
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
		at := time.Unix(0, next)
		info.NextFireAt = &at
	}
	return info
}

// Create persists a new schedule and registers it when enabled. The cron
// expression is validated first, so an invalid one never reaches the store, and
// the normalized 6-field Quartz form is what gets stored — not the string
// submitted, so a 5-field "0 2 * * *" comes back as "0 0 2 * * *". An empty ID
// is generated; a supplied one is honoured, which lets a restore keep its ids.
// If registration fails after the save, the save is undone.
func (s *Scheduler) Create(ctx context.Context, sch Schedule) (ScheduleInfo, error) {
	if err := s.requireStarted(); err != nil {
		return ScheduleInfo{}, err
	}
	if sch.TaskName == "" {
		return ScheduleInfo{}, errors.New("schedule: task name is required")
	}
	if err := ValidateCron(sch.Cron); err != nil {
		return ScheduleInfo{}, err
	}
	if sch.ID == uuid.Nil {
		sch.ID = uuid.New()
	}
	sch.Cron = NormalizeCron(sch.Cron)
	// Take our own copy of the params: the value below is both persisted and read
	// by the quartz worker on every fire, so aliasing the caller's slice would
	// race any later mutation of it.
	sch.Params = slices.Clone(sch.Params)
	now := time.Now()
	sch.CreatedAt, sch.UpdatedAt = now, now

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.store.Get(ctx, sch.ID); err == nil {
		return ScheduleInfo{}, ErrScheduleExists
	} else if !errors.Is(err, ErrScheduleNotFound) {
		return ScheduleInfo{}, fmt.Errorf("schedule: check existence of %s: %w", sch.ID, err)
	}
	if err := s.store.Save(ctx, sch); err != nil {
		return ScheduleInfo{}, fmt.Errorf("schedule: save %s: %w", sch.ID, err)
	}
	if err := s.applyLocked(sch); err != nil {
		if delErr := s.store.Delete(ctx, sch.ID); delErr != nil {
			s.log.Error("could not undo a failed create",
				slog.String("component", "tempo/schedule"),
				slog.String("scheduleId", sch.ID.String()),
				slog.String("error", delErr.Error()))
		}
		return ScheduleInfo{}, err
	}
	return s.infoLocked(sch), nil
}

// Update replaces the schedule identified by sch.ID and re-registers it. Like
// Create, it stores the normalized 6-field Quartz cron rather than the string
// submitted. CreatedAt is preserved. If registration fails after the save, the
// previous record is restored.
func (s *Scheduler) Update(ctx context.Context, sch Schedule) (ScheduleInfo, error) {
	if err := s.requireStarted(); err != nil {
		return ScheduleInfo{}, err
	}
	if sch.ID == uuid.Nil {
		return ScheduleInfo{}, ErrScheduleNotFound
	}
	if sch.TaskName == "" {
		return ScheduleInfo{}, errors.New("schedule: task name is required")
	}
	if err := ValidateCron(sch.Cron); err != nil {
		return ScheduleInfo{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	prev, err := s.store.Get(ctx, sch.ID)
	if err != nil {
		return ScheduleInfo{}, err
	}
	sch.Cron = NormalizeCron(sch.Cron)
	// Take our own copy, for the same reason as in Create.
	sch.Params = slices.Clone(sch.Params)
	sch.CreatedAt = prev.CreatedAt
	sch.UpdatedAt = time.Now()
	if err := s.store.Save(ctx, sch); err != nil {
		return ScheduleInfo{}, fmt.Errorf("schedule: save %s: %w", sch.ID, err)
	}
	if err := s.applyLocked(sch); err != nil {
		s.rollbackLocked(ctx, prev)
		return ScheduleInfo{}, err
	}
	return s.infoLocked(sch), nil
}

// SetEnabled turns a schedule on or off, registering or deregistering its job.
// Setting the value it already has changes nothing.
func (s *Scheduler) SetEnabled(ctx context.Context, id uuid.UUID, on bool) (ScheduleInfo, error) {
	if err := s.requireStarted(); err != nil {
		return ScheduleInfo{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	prev, err := s.store.Get(ctx, id)
	if err != nil {
		return ScheduleInfo{}, err
	}
	if prev.Enabled == on {
		return s.infoLocked(prev), nil
	}
	sch := prev
	sch.Enabled = on
	sch.UpdatedAt = time.Now()
	if err := s.store.Save(ctx, sch); err != nil {
		return ScheduleInfo{}, fmt.Errorf("schedule: save %s: %w", id, err)
	}
	if err := s.applyLocked(sch); err != nil {
		s.rollbackLocked(ctx, prev)
		return ScheduleInfo{}, err
	}
	return s.infoLocked(sch), nil
}

// Delete removes a schedule and deregisters its job.
func (s *Scheduler) Delete(ctx context.Context, id uuid.UUID) error {
	if err := s.requireStarted(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	prev, err := s.store.Get(ctx, id)
	if err != nil {
		return err
	}
	if err := s.deleteJobLocked(id); err != nil {
		return err
	}
	if err := s.store.Delete(ctx, id); err != nil {
		// Put the job back, so the store and the running jobs stay in step.
		if applyErr := s.applyLocked(prev); applyErr != nil {
			s.log.Error("could not restore the job after a failed delete",
				slog.String("component", "tempo/schedule"),
				slog.String("scheduleId", id.String()),
				slog.String("error", applyErr.Error()))
		}
		return fmt.Errorf("schedule: delete %s: %w", id, err)
	}
	return nil
}

// Trigger enqueues the schedule's task immediately, with its stored params,
// regardless of whether the schedule is enabled. It returns the new task id.
//
// Unlike the write methods, Trigger deliberately carries no started guard: it
// touches only the store and the enqueuer, never the quartz scheduler, so it is
// usable before Start and after ShutDown. "Run this now" is a valid request from
// an admin UI whether or not the timetable itself is running.
func (s *Scheduler) Trigger(ctx context.Context, id uuid.UUID) (uuid.UUID, error) {
	sch, err := s.store.Get(ctx, id)
	if err != nil {
		return uuid.Nil, err
	}
	taskID, err := s.enq.AddRaw(sch.TaskName, slices.Clone([]byte(sch.Params)))
	if err != nil {
		return uuid.Nil, fmt.Errorf("schedule: enqueue task %q: %w", sch.TaskName, err)
	}
	return taskID, nil
}

// rollbackLocked restores prev after a failed write, keeping the store and the
// running jobs in step. Caller must hold mu.
func (s *Scheduler) rollbackLocked(ctx context.Context, prev Schedule) {
	if err := s.store.Save(ctx, prev); err != nil {
		s.log.Error("could not undo a failed write",
			slog.String("component", "tempo/schedule"),
			slog.String("scheduleId", prev.ID.String()),
			slog.String("error", err.Error()))
	}
	if err := s.applyLocked(prev); err != nil {
		s.log.Error("could not restore the previous job",
			slog.String("component", "tempo/schedule"),
			slog.String("scheduleId", prev.ID.String()),
			slog.String("error", err.Error()))
	}
}
