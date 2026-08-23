package tempo_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-bumbu/tempo"
	"github.com/google/uuid"
)

// Example_streamTaskLogsOverHTTP shows how a caller streams a running task's log
// lines to an admin UI in real time. A long-running "scan" task logs its
// progress through the *slog.Logger tempo hands it; a custom tempo.TaskLogSink
// tees every line to (a) an in-memory replay buffer and (b) a per-task pub/sub
// hub. An HTTP handler then serves each connecting client the backlog followed
// by the live tail ("replay + follow"), like `tail -f`.
//
// The transport here is Server-Sent Events (SSE) so the example runs with only
// the standard library. A WebSocket is a drop-in replacement: the hub and the
// streaming sink are unchanged — only the per-line write in logStreamHandler
// becomes conn.Write(ctx, websocket.MessageText, data).
//
// The correctness property called out in tempo.TaskLogSink's docs is that
// Append runs synchronously on the worker goroutine, so it must never block or a
// stalled browser would stall the scan. The hub therefore publishes
// non-blockingly and drops lines for a client that cannot keep up.
func Example_streamTaskLogsOverHTTP() {
	hub := newLogHub()
	sink := newStreamingSink(hub)

	runner, err := tempo.NewQueueRunner(tempo.RunnerCfg{
		Parallelism: 1,
		QueueSize:   10,
		Persistence: tempo.NewMemPersistence(),
		LogSink:     sink,
		LogLevel:    slog.LevelInfo,
	})
	if err != nil {
		panic(err)
	}

	type ScanParams struct {
		Files []string `json:"files"`
	}
	const Scan = "scan"
	// "task started" / "task finished" are logged by the runner automatically;
	// the handler emits one line per file in between.
	tempo.Register(runner, Scan, func(_ context.Context, log *slog.Logger, _ tempo.Progress, p ScanParams) error {
		for _, f := range p.Files {
			log.Info("scanning file", "path", f)
			time.Sleep(15 * time.Millisecond) // simulate real work between lines
		}
		return nil
	})
	runner.StartBg()

	// The admin UI's SSE endpoint: GET /logs?task=<id>.
	srv := httptest.NewServer(logStreamHandler(sink, hub))
	defer srv.Close()

	// Schedule the scan, as the admin UI would.
	id, _, err := tempo.Enqueue(runner, Scan, ScanParams{Files: []string{"a/b/c", "d/e/f", "g/h/i"}})
	if err != nil {
		panic(err)
	}

	// A UI client connects and follows the stream until the task finishes. It
	// dedups by sequence number, so it makes no difference how many lines were
	// already in the backlog when it connected versus streamed live afterwards.
	lines := followStream(srv.URL + "/logs?task=" + id.String())

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = runner.ShutDown(shutdownCtx)

	for _, l := range lines {
		fmt.Printf("[%s] %s\n", l.Level, l.Msg)
	}

	// Output:
	// [INFO] task started
	// [INFO] scanning file path=a/b/c
	// [INFO] scanning file path=d/e/f
	// [INFO] scanning file path=g/h/i
	// [INFO] task finished
}

// LogLine is one streamed log line. Seq is a per-task, monotonically increasing
// sequence number that lets a late-joining client stitch the replayed backlog to
// the live tail with no gaps or duplicates.
type LogLine struct {
	Seq   int       `json:"seq"`
	Level string    `json:"level"`
	Msg   string    `json:"msg"`
	At    time.Time `json:"at"`
}

// logHub is a per-task publish/subscribe fan-out. publish never blocks the
// caller (the task worker): a subscriber that cannot keep up loses lines rather
// than back-pressuring the task.
type logHub struct {
	mu     sync.Mutex
	nextID int
	subs   map[uuid.UUID]map[int]chan LogLine
}

func newLogHub() *logHub {
	return &logHub{subs: make(map[uuid.UUID]map[int]chan LogLine)}
}

// subscribe registers a listener for one task and returns its channel plus an
// unsubscribe func the caller must invoke when done.
func (h *logHub) subscribe(id uuid.UUID) (<-chan LogLine, func()) {
	ch := make(chan LogLine, 256) // buffer absorbs bursts without dropping
	h.mu.Lock()
	subID := h.nextID
	h.nextID++
	if h.subs[id] == nil {
		h.subs[id] = make(map[int]chan LogLine)
	}
	h.subs[id][subID] = ch
	h.mu.Unlock()

	return ch, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if m := h.subs[id]; m != nil {
			if c, ok := m[subID]; ok {
				delete(m, subID)
				close(c)
			}
			if len(m) == 0 {
				delete(h.subs, id)
			}
		}
	}
}

// publish fans a line out to every current subscriber of id. It holds the lock
// only for non-blocking sends, so it is safe to call from Append.
func (h *logHub) publish(id uuid.UUID, line LogLine) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, ch := range h.subs[id] {
		select {
		case ch <- line:
		default: // slow client: drop this line rather than stall the task
		}
	}
}

// streamingSink is a tempo.TaskLogSink that both retains lines for replay and
// broadcasts them live. Append tees each line to an in-memory backlog and to the
// hub. It is safe for concurrent use, and Append never blocks (the hub drops for
// slow clients), honoring TaskLogSink's contract.
type streamingSink struct {
	hub *logHub
	mu  sync.Mutex
	seq map[uuid.UUID]int
	buf map[uuid.UUID][]LogLine
}

func newStreamingSink(hub *logHub) *streamingSink {
	return &streamingSink{hub: hub, seq: make(map[uuid.UUID]int), buf: make(map[uuid.UUID][]LogLine)}
}

// Append implements tempo.TaskLogSink.
func (s *streamingSink) Append(_ context.Context, taskID uuid.UUID, level, msg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.seq[taskID]
	s.seq[taskID] = n + 1
	line := LogLine{Seq: n, Level: level, Msg: msg, At: time.Now()}
	s.buf[taskID] = append(s.buf[taskID], line)
	// Publish under the lock so live lines reach the hub in Seq order; publish is
	// non-blocking, so the critical section stays short.
	s.hub.publish(taskID, line)
	return nil
}

// backlog returns a Seq-tagged copy of everything logged for taskID so far.
func (s *streamingSink) backlog(taskID uuid.UUID) []LogLine {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]LogLine(nil), s.buf[taskID]...)
}

// Logs implements tempo.TaskLogReader, so the same sink can also back a plain
// "fetch all logs" REST endpoint or the runner's history view.
func (s *streamingSink) Logs(_ context.Context, taskID uuid.UUID) ([]tempo.LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]tempo.LogEntry, 0, len(s.buf[taskID]))
	for _, l := range s.buf[taskID] {
		out = append(out, tempo.LogEntry{Level: l.Level, Message: l.Msg, At: l.At})
	}
	return out, nil
}

var (
	_ tempo.TaskLogSink   = (*streamingSink)(nil)
	_ tempo.TaskLogReader = (*streamingSink)(nil)
)

// logStreamHandler serves a task's log lines as Server-Sent Events: first the
// backlog (replay), then the live tail (follow), like `tail -f`. It subscribes
// before reading the backlog and dedups by Seq, so no line is missed or sent
// twice across the replay/live boundary.
func logStreamHandler(sink *streamingSink, hub *logHub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(r.URL.Query().Get("task"))
		if err != nil {
			http.Error(w, "bad task id", http.StatusBadRequest)
			return
		}
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")

		// Subscribe first, so any line logged from here on is captured live even
		// while we are still sending the backlog below.
		live, unsub := hub.subscribe(id)
		defer unsub()

		// Replay: everything logged before we subscribed.
		lastSeq := -1
		for _, l := range sink.backlog(id) {
			writeSSE(w, l)
			lastSeq = l.Seq
		}
		flusher.Flush()

		// Follow: stream live lines, skipping any the backlog already covered.
		for {
			select {
			case l, ok := <-live:
				if !ok {
					return
				}
				if l.Seq <= lastSeq {
					continue // already sent during replay
				}
				writeSSE(w, l)
				lastSeq = l.Seq
				flusher.Flush()
			case <-r.Context().Done():
				return
			}
		}
	}
}

func writeSSE(w io.Writer, l LogLine) {
	data, _ := json.Marshal(l)
	_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
}

// followStream connects to the SSE endpoint and collects log lines until it sees
// the task's terminal line, deduping by Seq. It returns the lines sorted by Seq,
// so the result is deterministic regardless of the replay/live split. A short
// request timeout keeps a hang from blocking the test forever.
func followStream(url string) []LogLine {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer func() { _ = resp.Body.Close() }()

	seen := make(map[int]LogLine)
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		data, ok := strings.CutPrefix(sc.Text(), "data: ")
		if !ok {
			continue // blank separator line between events
		}
		var l LogLine
		if err := json.Unmarshal([]byte(data), &l); err != nil {
			panic(err)
		}
		seen[l.Seq] = l
		if l.Msg == "task finished" {
			break // terminal line the runner logs when the task completes
		}
	}

	out := make([]LogLine, 0, len(seen))
	for _, l := range seen {
		out = append(out, l)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}
