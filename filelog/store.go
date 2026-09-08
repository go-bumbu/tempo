// Package filelog is a disk-backed tempo.TaskLogSink: it writes each task's log
// lines to its own JSON-Lines file, reads them back, and reaps them.
//
// It satisfies tempo.TaskLogReader and tempo.TaskLogCleaner, so a runner keeps
// the files trimmed with task history and sweeps orphans at startup. Because the
// files outlive the process, pair filelog with a tempo.RecoverablePersistence
// (e.g. dbqueue) if you want logs to survive restarts; with the default
// in-memory persistence the startup sweep deletes every prior file, as nothing
// is left to correlate them with.
//
// A Dir must be owned by exactly one runner: on startup a runner's orphan
// sweep (RetainOnly) deletes every log file in Dir for a task not in that
// runner's own recovered set, so pointing two runners — or sharing one Store
// — at the same Dir will delete each other's live task logs.
package filelog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-bumbu/tempo"
	"github.com/google/uuid"
)

const (
	numStripes = 64
	ext        = ".jsonl"
)

// Config configures a Store.
type Config struct {
	Dir      string      // required; created with MkdirAll if absent
	DirPerm  os.FileMode // 0 -> 0o755
	FilePerm os.FileMode // 0 -> 0o644
}

// Store is a disk-backed task log sink.
type Store struct {
	dir      string
	filePerm os.FileMode
	locks    [numStripes]sync.Mutex
}

var (
	_ tempo.TaskLogSink    = (*Store)(nil)
	_ tempo.TaskLogReader  = (*Store)(nil)
	_ tempo.TaskLogCleaner = (*Store)(nil)
)

// New creates a Store, creating Dir if needed.
func New(cfg Config) (*Store, error) {
	if cfg.Dir == "" {
		return nil, errors.New("filelog: Dir must not be empty")
	}
	dirPerm := cfg.DirPerm
	if dirPerm == 0 {
		dirPerm = 0o755
	}
	filePerm := cfg.FilePerm
	if filePerm == 0 {
		filePerm = 0o644
	}
	if err := os.MkdirAll(cfg.Dir, dirPerm); err != nil {
		return nil, fmt.Errorf("filelog: create dir: %w", err)
	}
	return &Store{dir: cfg.Dir, filePerm: filePerm}, nil
}

type line struct {
	At    time.Time `json:"at"`
	Level string    `json:"level"`
	Msg   string    `json:"msg"`
}

func (s *Store) lockFor(id uuid.UUID) *sync.Mutex {
	h := fnv.New32a()
	_, _ = h.Write(id[:])
	return &s.locks[h.Sum32()%numStripes]
}

func (s *Store) path(id uuid.UUID) string {
	return filepath.Join(s.dir, id.String()+ext)
}

// Append writes one JSON line to the task's file.
func (s *Store) Append(_ context.Context, taskID uuid.UUID, level, msg string) error {
	data, err := json.Marshal(line{At: time.Now(), Level: level, Msg: msg})
	if err != nil {
		return err
	}
	data = append(data, '\n')
	mu := s.lockFor(taskID)
	mu.Lock()
	defer mu.Unlock()
	f, err := os.OpenFile(s.path(taskID), os.O_APPEND|os.O_CREATE|os.O_WRONLY, s.filePerm)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	_, err = f.Write(data)
	return err
}

// Logs returns the task's log entries in order; nil for an unknown id.
func (s *Store) Logs(_ context.Context, taskID uuid.UUID) ([]tempo.LogEntry, error) {
	mu := s.lockFor(taskID)
	mu.Lock()
	defer mu.Unlock()
	data, err := os.ReadFile(s.path(taskID))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var out []tempo.LogEntry
	segs := bytes.Split(data, []byte("\n"))
	for i, raw := range segs {
		if len(raw) == 0 {
			continue
		}
		var l line
		if err := json.Unmarshal(raw, &l); err != nil {
			// A crash or kill (or ENOSPC) mid-Append can leave a partial final
			// line with no trailing newline. Tolerate a malformed *final*
			// segment by dropping it and returning the intact prefix; a bad
			// line anywhere earlier is genuine mid-file corruption and stays an
			// error. A complete line is always followed by "\n", so a non-empty
			// last segment is precisely a torn write.
			if i == len(segs)-1 {
				break
			}
			return nil, fmt.Errorf("filelog: parse %s: %w", taskID, err)
		}
		out = append(out, tempo.LogEntry{Level: l.Level, Message: l.Msg, At: l.At})
	}
	return out, nil
}

// RemoveTasks deletes the given tasks' log files, ignoring missing ones.
func (s *Store) RemoveTasks(_ context.Context, ids []uuid.UUID) error {
	for _, id := range ids {
		if err := s.removeOne(id); err != nil {
			return err
		}
	}
	return nil
}

// RetainOnly deletes every recognized log file whose id is not in keep.
func (s *Store) RetainOnly(_ context.Context, keep []uuid.UUID) error {
	keepSet := make(map[uuid.UUID]struct{}, len(keep))
	for _, id := range keep {
		keepSet[id] = struct{}{}
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ext {
			continue
		}
		id, err := uuid.Parse(e.Name()[:len(e.Name())-len(ext)])
		if err != nil {
			continue // not one of ours
		}
		if _, ok := keepSet[id]; ok {
			continue
		}
		if err := s.removeOne(id); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) removeOne(id uuid.UUID) error {
	mu := s.lockFor(id)
	mu.Lock()
	defer mu.Unlock()
	if err := os.Remove(s.path(id)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
