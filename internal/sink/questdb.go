package sink

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/polygate/polygate/internal/metrics"
	"github.com/polygate/polygate/internal/model"
	"github.com/polygate/polygate/internal/schema"
)

type QuestDBSink struct {
	addr       string // ILP TCP address (e.g. localhost:9009)
	restURL    string // REST API URL (e.g. http://localhost:9000)
	conn       net.Conn
	mu         sync.Mutex
	httpClient *http.Client
	bulkSize   int
	maxRetries int
	retryBase  int
	timeout    time.Duration
	tsCol      map[string]string // table → designated timestamp column
}

// NewQuestDBSink creates a QuestDB sink.
// addr is the ILP TCP address (e.g. localhost:9009).
// restURL is the QuestDB REST API URL (e.g. http://localhost:9000) for DDL.
func NewQuestDBSink(addr, restURL string, timeoutSecs, bulkSize, maxRetries, retryBase int) *QuestDBSink {
	return &QuestDBSink{
		addr:       addr,
		restURL:    strings.TrimRight(restURL, "/"),
		httpClient: &http.Client{Timeout: time.Duration(timeoutSecs) * time.Second},
		bulkSize:   bulkSize,
		maxRetries: maxRetries,
		retryBase:  retryBase,
		timeout:    time.Duration(timeoutSecs) * time.Second,
		tsCol:      make(map[string]string),
	}
}

func (s *QuestDBSink) Name() string { return "questdb" }

func (s *QuestDBSink) CreateTable(ctx context.Context, ts *schema.TableSchema) error {
	if ts.DesignatedTimestamp != "" {
		s.mu.Lock()
		s.tsCol[ts.Table] = ts.DesignatedTimestamp
		s.mu.Unlock()
	}

	// Build CREATE TABLE DDL.
	cols := make([]string, 0, len(ts.Columns))
	for c := range ts.Columns {
		cols = append(cols, c)
	}
	sort.Strings(cols)

	var parts []string
	for _, col := range cols {
		nativeType := ts.ColumnTypeFor(col, "questdb")
		parts = append(parts, fmt.Sprintf("  %s %s", col, nativeType))
	}

	ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n%s\n)", ts.Table, strings.Join(parts, ",\n"))

	if ts.DesignatedTimestamp != "" {
		ddl += fmt.Sprintf(" timestamp(%s)", ts.DesignatedTimestamp)
	}

	if err := s.execSQL(ctx, ddl); err != nil {
		return fmt.Errorf("questdb create table %q: %w", ts.Table, err)
	}
	slog.Debug("questdb table created", "table", ts.Table)

	// Create indexes on indexed columns.
	// Symbol columns are auto-indexed by QuestDB, but we add explicit indexes
	// for any columns listed in the schema's indexes.
	for _, idx := range ts.Indexes {
		for _, col := range idx.Columns {
			indexSQL := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s ADD INDEX", ts.Table, col)
			if err := s.execSQL(ctx, indexSQL); err != nil {
				// Index may already exist or column type may not support it — log and continue.
				slog.Warn("questdb index creation failed (may already exist)", "table", ts.Table, "column", col, "error", err)
			} else {
				slog.Debug("questdb index created", "table", ts.Table, "column", col)
			}
		}
	}

	return nil
}

func (s *QuestDBSink) execSQL(ctx context.Context, sql string) error {
	reqURL := fmt.Sprintf("%s/exec?query=%s", s.restURL, url.QueryEscape(sql))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return err
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("questdb returned %d: %s", resp.StatusCode, string(body))
	}
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
