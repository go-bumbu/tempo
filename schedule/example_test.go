package schedule_test

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/go-bumbu/tempo"
	"github.com/go-bumbu/tempo/schedule"
	"github.com/google/uuid"
)

type scanParams struct {
	Full bool `json:"full"`
}

// Example shows a runner and a scheduler wired together: one registered task,
// two schedules with different parameters.
func Example() {
	runner, err := tempo.NewQueueRunner(tempo.RunnerCfg{
		Parallelism: 2,
		QueueSize:   10,
		Persistence: tempo.NewMemPersistence(),
	})
	if err != nil {
		panic(err)
	}
	tempo.Register(runner, "scan", func(_ context.Context, p scanParams) error {
		fmt.Printf("scanning, full=%v\n", p.Full)
		return nil
	})
	runner.StartBg()

	// *tempo.QueueRunner satisfies schedule.Enqueuer as-is.
	sched, err := schedule.New(schedule.Cfg{
		Store:    schedule.NewMemStore(),
		Enqueuer: runner,
		Logger:   slog.New(slog.DiscardHandler),
	})
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	if err := sched.Start(ctx); err != nil {
		panic(err)
	}

	// A quick nightly scan and a full weekly one: one task, two cadences.
	nightly, err := sched.Create(ctx, schedule.Schedule{
		TaskName: "scan",
		Cron:     "0 2 * * *", // 5-field Unix cron
		Params:   []byte(`{"full":false}`),
		Enabled:  true,
	})
	if err != nil {
		panic(err)
	}
	weekly, err := sched.Create(ctx, schedule.Schedule{
		TaskName: "scan",
		Cron:     "0 3 * * 0", // Unix Sunday (0) translates to Quartz Sunday (1)
		Params:   []byte(`{"full":true}`),
		Enabled:  true,
	})
	if err != nil {
		panic(err)
	}

	// Read each one back by id. Printing the list in order would depend on the
	// two CreatedAt timestamps differing, which the clock does not guarantee.
	list, err := sched.List(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%d schedules for one task\n", len(list))
	for _, id := range []uuid.UUID{nightly.ID, weekly.ID} {
		s, err := sched.Get(ctx, id)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%s cron=%q params=%s enabled=%v\n", s.TaskName, s.Cron, s.Params, s.Enabled)
	}

	// Editing at runtime persists and reschedules in one call.
	if _, err := sched.SetEnabled(ctx, nightly.ID, false); err != nil {
		panic(err)
	}
	off, err := sched.Get(ctx, nightly.ID)
	if err != nil {
		panic(err)
	}
	fmt.Printf("nightly enabled=%v nextFireAt set=%v\n", off.Enabled, !off.NextFireAt.IsZero())

	if err := sched.ShutDown(ctx); err != nil {
		panic(err)
	}
	if err := runner.ShutDown(ctx); err != nil {
		panic(err)
	}

	// Output:
	// 2 schedules for one task
	// scan cron="0 0 2 * * *" params={"full":false} enabled=true
	// scan cron="0 0 3 * * 1" params={"full":true} enabled=true
	// nightly enabled=false nextFireAt set=false
}
