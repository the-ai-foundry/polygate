package sink

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/polygate/polygate/internal/metrics"
	"github.com/polygate/polygate/internal/model"
	"github.com/polygate/polygate/internal/schema"
)

type QuestDBSink struct {
	addr       string
	conn       net.Conn
	mu         sync.Mutex
	bulkSize   int
	maxRetries int
	retryBase  int
	timeout    time.Duration
	tsCol      map[string]string // table → designated timestamp column
}

func NewQuestDBSink(addr string, timeoutSecs, bulkSize, maxRetries, retryBase int) *QuestDBSink {
	return &QuestDBSink{
		addr:       addr,
		bulkSize:   bulkSize,
		maxRetries: maxRetries,
		retryBase:  retryBase,
		timeout:    time.Duration(timeoutSecs) * time.Second,
		tsCol:      make(map[string]string),
	}
}

func (s *QuestDBSink) Name() string { return "questdb" }

func (s *QuestDBSink) CreateTable(ctx context.Context, ts *schema.TableSchema) error {
	// QuestDB auto-creates tables from ILP data.
	// Store the designated timestamp column for later use in ILP formatting.
	if ts.DesignatedTimestamp != "" {
		s.mu.Lock()
		s.tsCol[ts.Table] = ts.DesignatedTimestamp
		s.mu.Unlock()
	}
	slog.Debug("questdb table registered", "table", ts.Table, "designated_ts", ts.DesignatedTimestamp)
	return nil
}

func (s *QuestDBSink) WriteBatch(ctx context.Context, table string, records []model.Record) error {
	if len(records) == 0 {
		return nil
	}

	start := time.Now()
	for i := 0; i < len(records); i += s.bulkSize {
		end := i + s.bulkSize
		if end > len(records) {
			end = len(records)
		}
		chunk := records[i:end]

		err := WithRetry(ctx, s.maxRetries, s.retryBase, func() error {
			return s.writeILP(ctx, table, chunk)
		})
		if err != nil {
			metrics.SinkErrorsTotal.WithLabelValues("questdb").Inc()
			return fmt.Errorf("questdb write to %q: %w", table, err)
		}
	}

	dur := time.Since(start)
	metrics.SinkWriteDuration.WithLabelValues("questdb", table).Observe(dur.Seconds())
	metrics.SinkWriteRowsTotal.WithLabelValues("questdb", table).Add(float64(len(records)))
	return nil
}

func (s *QuestDBSink) writeILP(ctx context.Context, table string, records []model.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureConn(); err != nil {
		return err
	}

	tsColName := s.tsCol[table]

	var buf strings.Builder
	for _, rec := range records {
		// ILP format: table field1=val1,field2=val2 timestamp_ns\n
		buf.WriteString(table)
		buf.WriteByte(' ')

		first := true
		var tsNanos int64
		for k, v := range rec {
			if k == tsColName {
				// Extract timestamp for the ILP timestamp field.
				if t, ok := v.(time.Time); ok {
					tsNanos = t.UnixNano()
				}
				continue
			}
			if !first {
				buf.WriteByte(',')
			}
			first = false
			buf.WriteString(k)
			buf.WriteByte('=')
			writeILPValue(&buf, v)
		}

		if tsNanos > 0 {
			buf.WriteByte(' ')
			fmt.Fprintf(&buf, "%d", tsNanos)
		}
		buf.WriteByte('\n')
	}

	if err := s.conn.SetWriteDeadline(time.Now().Add(s.timeout)); err != nil {
		return err
	}
	_, err := s.conn.Write([]byte(buf.String()))
	if err != nil {
		s.conn.Close()
		s.conn = nil
		return err
	}
	return nil
}

func (s *QuestDBSink) ensureConn() error {
	if s.conn != nil {
		return nil
	}
	conn, err := net.DialTimeout("tcp", s.addr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("connecting to questdb at %s: %w", s.addr, err)
	}
	s.conn = conn
	return nil
}

func writeILPValue(buf *strings.Builder, v any) {
	switch val := v.(type) {
	case string:
		buf.WriteByte('"')
		buf.WriteString(strings.ReplaceAll(val, `"`, `\"`))
		buf.WriteByte('"')
	case float64:
		fmt.Fprintf(buf, "%g", val)
	case int64:
		fmt.Fprintf(buf, "%di", val)
	case int:
		fmt.Fprintf(buf, "%di", val)
	case bool:
		if val {
			buf.WriteByte('t')
		} else {
			buf.WriteByte('f')
		}
	default:
		fmt.Fprintf(buf, "%v", val)
	}
}

func (s *QuestDBSink) Ping(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureConn(); err != nil {
		return err
	}
	return nil
}

func (s *QuestDBSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}
