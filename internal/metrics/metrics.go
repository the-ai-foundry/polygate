package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	IngestRecordsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "polygate_ingest_records_total",
		Help: "Total number of records received via ingest API",
	})

	IngestBatchesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "polygate_ingest_batches_total",
		Help: "Total number of batches flushed to sinks",
	})

	SinkWriteDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "polygate_sink_write_duration_seconds",
		Help:    "Duration of sink write operations",
		Buckets: prometheus.DefBuckets,
	}, []string{"sink", "table"})

	SinkWriteRowsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "polygate_sink_write_rows_total",
		Help: "Total rows written per sink and table",
	}, []string{"sink", "table"})

	SinkErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "polygate_sink_errors_total",
		Help: "Total write errors per sink",
	}, []string{"sink"})

	SinkHealthy = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "polygate_sink_healthy",
		Help: "Whether a sink is healthy (1) or not (0)",
	}, []string{"sink"})

	QueryDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "polygate_query_duration_seconds",
		Help:    "Duration of query execution",
		Buckets: prometheus.DefBuckets,
	}, []string{"engine"})

	BatcherQueueSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "polygate_batcher_queue_size",
		Help: "Current number of records buffered in the batcher",
	})

	SchemaRegistrations = promauto.NewCounter(prometheus.CounterOpts{
		Name: "polygate_schema_registrations_total",
		Help: "Total number of schema registrations",
	})
)
