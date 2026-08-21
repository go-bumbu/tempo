// Package dbschedule stores tempo schedules in a SQL database using gorm. It is
// a schedule.Store implementation, kept out of the schedule package so that
// gorm is not a dependency of anyone who does not need it.
package dbschedule

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-bumbu/tempo/schedule"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// row is the persisted form of a schedule.
//
// The id is stored as its string form so the table works the same on every
// supported database. There is no unique index on task_name: several schedules
// may drive the same task with different cadences or parameters. There is no
// gorm.DeletedAt either — a deleted schedule is gone, which avoids the
// Unscoped() workarounds a soft delete forces on every upsert. CreatedAt and
// UpdatedAt are set by the Scheduler, so gorm's automatic tracking is off.
type row struct {
	ID        string `gorm:"primaryKey;type:varchar(36)"`
	TaskName  string `gorm:"not null;index"`
	Cron      string `gorm:"not null"`
	Params    []byte
	Enabled   bool
	CreatedAt time.Time `gorm:"autoCreateTime:false"`
	UpdatedAt time.Time `gorm:"autoUpdateTime:false"`
}

// TableName is the table schedules are stored in.
func (row) TableName() string { return "tempo_schedules" }

// Store persists schedules with gorm.
type Store struct {
	db *gorm.DB
}

// Verify Store satisfies the schedule.Store interface.
var _ schedule.Store = (*Store)(nil)

// New returns a Store, running AutoMigrate for the schedule table. It is safe
// to call more than once on the same database.
func New(db *gorm.DB) (*Store, error) {
	if db == nil {
		return nil, errors.New("dbschedule: db must not be nil")
	}
	if err := db.AutoMigrate(&row{}); err != nil {
		return nil, fmt.Errorf("dbschedule: migrate: %w", err)
	}
	return &Store{db: db}, nil
}

// List returns every schedule, ordered by creation time then id.
func (s *Store) List(ctx context.Context) ([]schedule.Schedule, error) {
	var rows []row
	if err := s.db.WithContext(ctx).Order("created_at, id").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("dbschedule: list: %w", err)
	}
	out := make([]schedule.Schedule, 0, len(rows))
	for _, r := range rows {
		sch, err := fromRow(r)
		if err != nil {
			return nil, err
		}
		out = append(out, sch)
	}
	return out, nil
}

// Get returns one schedule, or schedule.ErrScheduleNotFound.
func (s *Store) Get(ctx context.Context, id uuid.UUID) (schedule.Schedule, error) {
	var r row
	err := s.db.WithContext(ctx).Where("id = ?", id.String()).First(&r).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return schedule.Schedule{}, schedule.ErrScheduleNotFound
		}
		return schedule.Schedule{}, fmt.Errorf("dbschedule: get %s: %w", id, err)
	}
	return fromRow(r)
}

// Save upserts by id.
func (s *Store) Save(ctx context.Context, sch schedule.Schedule) error {
	r := toRow(sch)
	err := s.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		UpdateAll: true,
	}).Create(&r).Error
	if err != nil {
		return fmt.Errorf("dbschedule: save %s: %w", sch.ID, err)
	}
	return nil
}

// Delete removes the schedule, or returns schedule.ErrScheduleNotFound.
func (s *Store) Delete(ctx context.Context, id uuid.UUID) error {
	res := s.db.WithContext(ctx).Where("id = ?", id.String()).Delete(&row{})
	if res.Error != nil {
		return fmt.Errorf("dbschedule: delete %s: %w", id, res.Error)
	}
	if res.RowsAffected == 0 {
		return schedule.ErrScheduleNotFound
	}
	return nil
}

func toRow(sch schedule.Schedule) row {
	return row{
		ID:        sch.ID.String(),
		TaskName:  sch.TaskName,
		Cron:      sch.Cron,
		Params:    []byte(sch.Params),
		Enabled:   sch.Enabled,
		CreatedAt: sch.CreatedAt,
		UpdatedAt: sch.UpdatedAt,
	}
}

func fromRow(r row) (schedule.Schedule, error) {
	id, err := uuid.Parse(r.ID)
	if err != nil {
		return schedule.Schedule{}, fmt.Errorf("dbschedule: parse id %q: %w", r.ID, err)
	}
	var params json.RawMessage
	if len(r.Params) > 0 {
		params = json.RawMessage(r.Params)
	}
	return schedule.Schedule{
		ID:        id,
		TaskName:  r.TaskName,
		Cron:      r.Cron,
		Params:    params,
		Enabled:   r.Enabled,
		CreatedAt: r.CreatedAt,
		UpdatedAt: r.UpdatedAt,
	}, nil
}
