package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/polygate/polygate/internal/metrics"
	"github.com/polygate/polygate/internal/query"
)

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	engineName := r.URL.Query().Get("engine")
	if engineName == "" {
		engineName = "postgres"
	}

	q := r.URL.Query().Get("q")
	if q == "" {
		http.Error(w, "query parameter 'q' is required", http.StatusBadRequest)
		return
	}

	stream := r.URL.Query().Get("stream") == "true"

	if stream {
		s.handleQueryStream(w, r, engineName, q)
		return
	}

	engine, err := s.Router.Route(engineName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	start := time.Now()
	result, err := engine.Execute(r.Context(), q)
	dur := time.Since(start)

	metrics.QueryDuration.WithLabelValues(engineName).Observe(dur.Seconds())

	if err != nil {
		http.Error(w, "query error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"ok":            true,
		"result":        result,
		"query_time_ms": dur.Milliseconds(),
	})
}

func (s *Server) handleQueryStream(w http.ResponseWriter, r *http.Request, engineName, q string) {
	streamEngine, err := s.Router.RouteStream(engineName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	pageSize := 1000
	if ps := r.URL.Query().Get("page_size"); ps != "" {
		if n, err := strconv.Atoi(ps); err == nil && n > 0 && n <= 10000 {
			pageSize = n
		}
	}

	// Set SSE headers.
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ctx := r.Context()

	// For QuestDB, pass table name through context for CSV schema lookup.
	if table := r.URL.Query().Get("table"); table != "" {
		ctx = query.WithStreamTable(ctx, table)
	}

	out := make(chan query.StreamResult, 4)

	errCh := make(chan error, 1)
	go func() {
		errCh <- streamEngine.ExecuteStream(ctx, q, pageSize, out)
	}()

	for result := range out {
		data, _ := json.Marshal(result)
		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
	}

	// Check for errors from the stream goroutine.
	if err := <-errCh; err != nil {
		errData, _ := json.Marshal(map[string]string{"error": err.Error()})
		fmt.Fprintf(w, "event: error\ndata: %s\n\n", errData)
		flusher.Flush()
	}

	fmt.Fprintf(w, "event: done\ndata: {}\n\n")
	flusher.Flush()
}
