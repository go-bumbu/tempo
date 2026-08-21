// Package dbqueue persists tempo task state to a SQL database using gorm. It is
// a tempo.RecoverablePersistence implementation, kept out of the core tempo
// package so that gorm is not a dependency of anyone who does not need it.
//
// Because it implements List, a runner configured with a dbqueue store recovers
// its tasks after a restart: waiting tasks run, and tasks left Running by a
// crash are reconciled to Failed by the core (see tempo's recoverTasks). The
// default in-memory persistence has no List and so recovers nothing.
package dbqueue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-bumbu/tempo"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// row is the persisted form of a task.
//
// The id is stored as its string form so the table works the same on every
// supported database. Status is stored as its integer code; the tempo constants
// must therefore never be renumbered. There is no gorm.DeletedAt — CleanHistory
// hard-deletes trimmed history, which avoids the Unscoped() workarounds a soft
// delete forces on every upsert. QueuedAt/StartedAt/EndedAt are owned by the
// queue, so gorm's automatic timestamp tracking is off.
type row struct {
	ID        string `gorm:"primaryKey;type:varchar(36)"`
	Name      string `gorm:"not null;index"`
	Status    int    `gorm:"not null"`
	QueuedAt  time.Time
	StartedAt time.Time
	EndedAt   time.Time
	Params    []byte
}

// TableName is the table tasks are stored in.
func (row) TableName() string { return "tempo_tasks" }

// Store persists task state with gorm.
type Store struct {
	db *gorm.DB
}

// Verify Store satisfies the core persistence interfaces.
var (
	_ tempo.TaskStatePersistence   = (*Store)(nil)
	_ tempo.RecoverablePersistence = (*Store)(nil)
)

// New returns a Store, running AutoMigrate for the task table. It is safe to
// call more than once on the same database.
func New(db *gorm.DB) (*Store, error) {
	if db == nil {
		return nil, errors.New("dbqueue: db must not be nil")
	}
	if err := db.AutoMigrate(&row{}); err != nil {
		return nil, fmt.Errorf("dbqueue: migrate: %w", err)
	}
	return &Store{db: db}, nil
}

// SaveTask upserts a task by id.
func (s *Store) SaveTask(ctx context.Context, task tempo.TaskInfo) error {
	r := toRow(task)
	err := s.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		UpdateAll: true,
	}).Create(&r).Error
	if err != nil {
		return fmt.Errorf("dbqueue: save %s: %w", task.ID, err)
	}
	return nil
}

// RemoveTasks hard-deletes the given tasks by id. An empty slice is a no-op.
func (s *Store) RemoveTasks(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	strs := make([]string, len(ids))
	for i, id := range ids {
		strs[i] = id.String()
	}
	if err := s.db.WithContext(ctx).Where("id IN ?", strs).Delete(&row{}).Error; err != nil {
		return fmt.Errorf("dbqueue: remove tasks: %w", err)
	}
	return nil
}

// List returns every persisted task, oldest-queued first (ties broken by id) so
// that recovery preserves the queue's FIFO claim order across a restart.
func (s *Store) List(ctx context.Context) ([]tempo.TaskInfo, error) {
	var rows []row
	if err := s.db.WithContext(ctx).Order("queued_at, id").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("dbqueue: list: %w", err)
	}
	out := make([]tempo.TaskInfo, 0, len(rows))
	for _, r := range rows {
		info, err := fromRow(r)
		if err != nil {
			return nil, err
		}
		out = append(out, info)
	}
	return out, nil
}

// utc converts an instant to UTC, leaving the zero time zero. Databases that
// keep a timestamp as text — sqlite — would otherwise record the caller's
// offset, so "ORDER BY queued_at" would compare wall clocks lexicographically
// instead of instants (a row queued at 20:00+10:00 would sort after one queued
// later at 11:00Z).
func utc(t time.Time) time.Time {
	if t.IsZero() {
		return t
	}
	return t.UTC()
}

func toRow(task tempo.TaskInfo) row {
	return row{
		ID:        task.ID.String(),
		Name:      task.Name,
		Status:    int(task.Status),
		QueuedAt:  utc(task.QueuedAt),
		StartedAt: utc(task.StartedAt),
		EndedAt:   utc(task.EndedAt),
		Params:    task.Params,
	}
}

func fromRow(r row) (tempo.TaskInfo, error) {
	id, err := uuid.Parse(r.ID)
	if err != nil {
		return tempo.TaskInfo{}, fmt.Errorf("dbqueue: parse id %q: %w", r.ID, err)
	}
	var params []byte
	if len(r.Params) > 0 {
		params = r.Params
	}
	return tempo.TaskInfo{
		ID:        id,
		Name:      r.Name,
		Status:    tempo.TaskStatus(r.Status),
		QueuedAt:  r.QueuedAt,
		StartedAt: r.StartedAt,
		EndedAt:   r.EndedAt,
		Params:    params,
	}, nil
}
