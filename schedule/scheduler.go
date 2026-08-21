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
		info.NextFireAt = time.Unix(0, next)
	}
	return info
}

// Create persists a new schedule and registers it when enabled. The cron
// expression is validated first, so an invalid one never reaches the store. An
// empty ID is generated; a supplied one is honoured, which lets a restore keep
// its ids. If registration fails after the save, the save is undone.
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

// Update replaces the schedule identified by sch.ID and re-registers it.
// CreatedAt is preserved. If registration fails after the save, the previous
// record is restored.
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
