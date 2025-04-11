package prometheusmetrics

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/prometheus/client_golang/prometheus"
)

// PrometheusMetricsCollector implements the MetricsCollector interface using Prometheus metrics
type PrometheusMetricsCollector struct {
	// Counters
	leaderChangesCounter   prometheus.Counter
	learnerPromoteSucceed  prometheus.Counter
	learnerPromoteFailed   prometheus.Counter
	proposalFailedCounter  prometheus.Counter
	slowReadIndexCounter   prometheus.Counter
	readIndexFailedCounter prometheus.Counter

	// Gauges
	hasLeaderGauge          prometheus.Gauge
	isLeaderGauge           prometheus.Gauge
	isLearnerGauge          prometheus.Gauge
	snapshotApplyGauge      prometheus.Gauge
	proposalCommitedGauge   prometheus.Gauge
	proposalAppliedGauge    prometheus.Gauge
	inflightSnapshotCounter prometheus.Gauge
	proposalPendingCounter  prometheus.Gauge

	// Histograms
	applyHistogram         prometheus.Histogram
	doSnapshotHistogram    prometheus.Histogram
	applySnapshotHistogram prometheus.Histogram

	// Metric naming
	namespace     string
	raftSubsystem string
}

// NewPrometheusMetricsCollector creates a new instance of PrometheusMetricsCollector
func NewPrometheusMetricsCollector() ibabuza.MetricsCollector {
	collector := &PrometheusMetricsCollector{
		namespace:     "babuza",
		raftSubsystem: "raft",
	}

	// Create counters
	collector.leaderChangesCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: collector.namespace,
			Subsystem: collector.raftSubsystem,
			Name:      "leader_changes_total",
			Help:      "Number of leader changes",
		},
	)

	collector.learnerPromoteSucceed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: collector.namespace,
			Subsystem: collector.raftSubsystem,
			Name:      "learner_promote_succeed_total",
			Help:      "Number of successful learner promotions",
		},
	)

	collector.learnerPromoteFailed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: collector.namespace,
			Subsystem: collector.raftSubsystem,
			Name:      "learner_promote_failed_total",
			Help:      "Number of failed learner promotions",
		},
	)

	collector.proposalFailedCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: collector.namespace,
			Subsystem: collector.raftSubsystem,
			Name:      "proposal_failed_total",
			Help:      "Number of failed proposals",
		},
	)

	collector.slowReadIndexCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: collector.namespace,
			Subsystem: collector.raftSubsystem,
			Name:      "read_index_slow_total",
			Help:      "Number of slow read indexes",
		},
	)

	collector.readIndexFailedCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: collector.namespace,
			Subsystem: collector.raftSubsystem,
			Name:      "read_index_failed_total",
			Help:      "Number of failed read indexes",
		},
	)

	// Create gauges
	collector.hasLeaderGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: collector.namespace,
			Subsystem: collector.raftSubsystem,
			Name:      "node_has_leader",
			Help:      "Whether the node has a leader",
		},
	)

	collector.isLeaderGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: collector.namespace,
			Subsystem: collector.raftSubsystem,
			Name:      "node_is_leader",
			Help:      "Whether the node is a leader",
		},
	)

	collector.isLearnerGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: collector.namespace,
			Subsystem: collector.raftSubsystem,
			Name:      "node_is_learner",
			Help:      "Whether the node is a learner",
		},
	)

	collector.snapshotApplyGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: collector.namespace,
			Subsystem: collector.raftSubsystem,
			Name:      "snapshot_apply_in_progress",
			Help:      "Whether a snapshot apply is in progress",
		},
	)

	collector.proposalCommitedGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: collector.namespace,
			Subsystem: collector.raftSubsystem,
			Name:      "proposal_commited",
			Help:      "Number of committed proposals",
		},
	)

	collector.proposalAppliedGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: collector.namespace,
			Subsystem: collector.raftSubsystem,
			Name:      "proposal_applied_index",
			Help:      "Index of the last applied proposal",
		},
	)

	// Create histograms
	collector.applyHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: collector.namespace,
			Subsystem: collector.raftSubsystem,
			Name:      "apply_duration_seconds",
			Help:      "Duration of apply operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
	)

	collector.doSnapshotHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: collector.namespace,
			Subsystem: collector.raftSubsystem,
			Name:      "snapshot_do_duration_seconds",
			Help:      "Duration of snapshot creation in seconds",
			Buckets:   prometheus.DefBuckets,
		},
	)

	collector.applySnapshotHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: collector.namespace,
			Subsystem: collector.raftSubsystem,
			Name:      "snapshot_apply_duration_seconds",
			Help:      "Duration of snapshot apply operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
	)

	// Register all metrics with Prometheus
	prometheus.MustRegister(
		collector.leaderChangesCounter,
		collector.learnerPromoteSucceed,
		collector.learnerPromoteFailed,
		collector.proposalFailedCounter,
		collector.slowReadIndexCounter,
		collector.readIndexFailedCounter,
		collector.hasLeaderGauge,
		collector.isLeaderGauge,
		collector.isLearnerGauge,
		collector.snapshotApplyGauge,
		collector.proposalCommitedGauge,
		collector.proposalAppliedGauge,
		collector.applyHistogram,
		collector.doSnapshotHistogram,
		collector.applySnapshotHistogram,
	)

	// Create and register dynamic counters
	collector.proposalPendingCounter = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: collector.namespace,
			Subsystem: collector.raftSubsystem,
			Name:      "proposal_pending",
			Help:      "Number of pending proposals",
		},
	)
	prometheus.MustRegister(collector.proposalPendingCounter)

	collector.inflightSnapshotCounter = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: collector.namespace,
			Subsystem: collector.raftSubsystem,
			Name:      "snapshot_inflight",
			Help:      "Number of in-flight snapshots",
		},
	)
	prometheus.MustRegister(collector.inflightSnapshotCounter)

	return collector
}

// SetHasLeader sets whether the node has a leader
func (p *PrometheusMetricsCollector) SetHasLeader(hasLeader int64) {
	p.hasLeaderGauge.Set(float64(hasLeader))
}

// SetIsLeader sets whether this node is a leader
func (p *PrometheusMetricsCollector) SetIsLeader(isLeader int64) {
	p.isLeaderGauge.Set(float64(isLeader))
}

// IncrementLeaderChanges increments the counter for leader changes
func (p *PrometheusMetricsCollector) IncrementLeaderChanges() {
	p.leaderChangesCounter.Inc()
}

// IncrementLearnerPromoteSucceed increments the counter for successful learner promotions
func (p *PrometheusMetricsCollector) IncrementLearnerPromoteSucceed() {
	p.learnerPromoteSucceed.Inc()
}

// IncrementLearnerPromoteFailed increments the counter for failed learner promotions
func (p *PrometheusMetricsCollector) IncrementLearnerPromoteFailed() {
	p.learnerPromoteFailed.Inc()
}

// SetIsLearner sets whether this node is a learner
func (p *PrometheusMetricsCollector) SetIsLearner(isLearner int64) {
	p.isLearnerGauge.Set(float64(isLearner))
}

// RecordApplySec records the duration of an apply operation
func (p *PrometheusMetricsCollector) RecordApplySec(duration float64) {
	p.applyHistogram.Observe(duration)
}

// RecordDoSnapshotSec records the duration of a snapshot creation
func (p *PrometheusMetricsCollector) RecordDoSnapshotSec(duration float64) {
	p.doSnapshotHistogram.Observe(duration)
}

// RecordApplySnapshotSec records the duration of a snapshot apply operation
func (p *PrometheusMetricsCollector) RecordApplySnapshotSec(duration float64) {
	p.applySnapshotHistogram.Observe(duration)
}

// SetSnapshotApplyInProgress sets whether a snapshot apply is in progress
func (p *PrometheusMetricsCollector) SetSnapshotApplyInProgress(applying int64) {
	p.snapshotApplyGauge.Set(float64(applying))
}

// IncrementInflightSnapshots increments the counter for in-flight snapshots
func (p *PrometheusMetricsCollector) IncrementInflightSnapshots() {
	p.inflightSnapshotCounter.Inc()
}

// DecrementInflightSnapshots decrements the counter for in-flight snapshots
func (p *PrometheusMetricsCollector) DecrementInflightSnapshots() {
	p.inflightSnapshotCounter.Dec()
}

// SetProposalCommited sets the number of committed proposals
func (p *PrometheusMetricsCollector) SetProposalCommited(commitedEntries uint64) {
	p.proposalCommitedGauge.Set(float64(commitedEntries))
}

// SetProposalAppliedIndex sets the index of the last applied proposal
func (p *PrometheusMetricsCollector) SetProposalAppliedIndex(appliedIndex uint64) {
	p.proposalAppliedGauge.Set(float64(appliedIndex))
}

// IncrementProposalPending increments the counter for pending proposals
func (p *PrometheusMetricsCollector) IncrementProposalPending() {
	p.proposalPendingCounter.Inc()
}

// DecrementProposalPending decrements the counter for pending proposals
func (p *PrometheusMetricsCollector) DecrementProposalPending() {
	p.proposalPendingCounter.Dec()
}

// IncrementProposalFailed increments the counter for failed proposals
func (p *PrometheusMetricsCollector) IncrementProposalFailed() {
	p.proposalFailedCounter.Inc()
}

// IncrementSlowReadIndex increments the counter for slow read indexes
func (p *PrometheusMetricsCollector) IncrementSlowReadIndex() {
	p.slowReadIndexCounter.Inc()
}

// IncrementReadIndexFailed increments the counter for failed read indexes
func (p *PrometheusMetricsCollector) IncrementReadIndexFailed() {
	p.readIndexFailedCounter.Inc()
}
