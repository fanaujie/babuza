package otelmetrics

import (
	"context"
	"sync/atomic"

	"github.com/fanaujie/babuza/ibabuza"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// OtelMetricsCollector implements the MetricsCollector interface using OpenTelemetry metrics
type OtelMetricsCollector struct {
	meter                   metric.Meter
	leaderChangesCounter    metric.Int64Counter
	learnerPromoteSucceed   metric.Int64Counter
	learnerPromoteFailed    metric.Int64Counter
	applyHistogram          metric.Float64Histogram
	doSnapshotHistogram     metric.Float64Histogram
	applySnapshotHistogram  metric.Float64Histogram
	inflightSnapshotCounter metric.Int64UpDownCounter
	proposalPendingCounter  metric.Int64UpDownCounter
	proposalFailedCounter   metric.Int64Counter
	slowReadIndexCounter    metric.Int64Counter
	readIndexFailedCounter  metric.Int64Counter

	// Atomic state values for observable metrics
	hasLeader        atomic.Int64
	isLeader         atomic.Int64
	isLearner        atomic.Int64
	snapshotApply    atomic.Int64
	proposalCommited atomic.Int64
	proposalApplied  atomic.Int64
}

// NewOtelMetricsCollector creates a new instance of OtlpMetricsCollector
func NewOtelMetricsCollector() ibabuza.MetricsCollector {
	collector := &OtelMetricsCollector{
		meter: otel.GetMeterProvider().Meter("babuza"),
	}

	// Create counters
	var err error
	collector.leaderChangesCounter, err = collector.meter.Int64Counter(
		"babuza.leader.changes",
		metric.WithDescription("Number of leader changes"),
	)
	if err != nil {
		panic(err)
	}

	collector.learnerPromoteSucceed, err = collector.meter.Int64Counter(
		"babuza.learner.promote_succeed",
		metric.WithDescription("Number of successful learner promotions"),
	)
	if err != nil {
		panic(err)
	}

	collector.learnerPromoteFailed, err = collector.meter.Int64Counter(
		"babuza.learner.promote_failed",
		metric.WithDescription("Number of failed learner promotions"),
	)
	if err != nil {
		panic(err)
	}

	collector.proposalFailedCounter, err = collector.meter.Int64Counter(
		"babuza.proposal.failed",
		metric.WithDescription("Number of failed proposals"),
	)
	if err != nil {
		panic(err)
	}

	collector.slowReadIndexCounter, err = collector.meter.Int64Counter(
		"babuza.read_index.slow",
		metric.WithDescription("Number of slow read indexes"),
	)
	if err != nil {
		panic(err)
	}

	collector.readIndexFailedCounter, err = collector.meter.Int64Counter(
		"babuza.read_index.failed",
		metric.WithDescription("Number of failed read indexes"),
	)
	if err != nil {
		panic(err)
	}

	// Create histograms
	collector.applyHistogram, err = collector.meter.Float64Histogram(
		"babuza.apply.duration_seconds",
		metric.WithDescription("Duration of apply operations in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		panic(err)
	}

	collector.doSnapshotHistogram, err = collector.meter.Float64Histogram(
		"babuza.snapshot.do_duration_seconds",
		metric.WithDescription("Duration of snapshot creation in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		panic(err)
	}

	collector.applySnapshotHistogram, err = collector.meter.Float64Histogram(
		"babuza.snapshot.apply_duration_seconds",
		metric.WithDescription("Duration of snapshot apply operations in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		panic(err)
	}

	// Create up/down counters
	collector.inflightSnapshotCounter, err = collector.meter.Int64UpDownCounter(
		"babuza.snapshot.inflight",
		metric.WithDescription("Number of in-flight snapshots"),
	)
	if err != nil {
		panic(err)
	}

	collector.proposalPendingCounter, err = collector.meter.Int64UpDownCounter(
		"babuza.proposal.pending",
		metric.WithDescription("Number of pending proposals"),
	)
	if err != nil {
		panic(err)
	}

	// Create observable gauges
	_, err = collector.meter.Int64ObservableGauge(
		"babuza.node.has_leader",
		metric.WithDescription("Whether the node has a leader"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(collector.hasLeader.Load())
			return nil
		}),
	)
	if err != nil {
		panic(err)
	}

	_, err = collector.meter.Int64ObservableGauge(
		"babuza.node.is_leader",
		metric.WithDescription("Whether the node is a leader"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(collector.isLeader.Load())
			return nil
		}),
	)
	if err != nil {
		panic(err)
	}

	_, err = collector.meter.Int64ObservableGauge(
		"babuza.node.is_learner",
		metric.WithDescription("Whether the node is a learner"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(collector.isLearner.Load())
			return nil
		}),
	)
	if err != nil {
		panic(err)
	}

	_, err = collector.meter.Int64ObservableGauge(
		"babuza.snapshot.apply_in_progress",
		metric.WithDescription("Whether a snapshot apply is in progress"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(collector.snapshotApply.Load())
			return nil
		}),
	)
	if err != nil {
		panic(err)
	}

	_, err = collector.meter.Int64ObservableGauge(
		"babuza.proposal.commited",
		metric.WithDescription("Number of committed proposals"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(collector.proposalCommited.Load())
			return nil
		}),
	)
	if err != nil {
		panic(err)
	}

	_, err = collector.meter.Int64ObservableGauge(
		"babuza.proposal.applied_index",
		metric.WithDescription("Index of the last applied proposal"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(collector.proposalApplied.Load())
			return nil
		}),
	)
	if err != nil {
		panic(err)
	}

	return collector
}

// SetHasLeader sets whether the node has a leader
func (o *OtelMetricsCollector) SetHasLeader(hasLeader int64) {
	o.hasLeader.Store(hasLeader)
}

// SetIsLeader sets whether this node is a leader
func (o *OtelMetricsCollector) SetIsLeader(isLeader int64) {
	o.isLeader.Store(isLeader)
}

// IncrementLeaderChanges increments the counter for leader changes
func (o *OtelMetricsCollector) IncrementLeaderChanges() {
	ctx := context.Background()
	o.leaderChangesCounter.Add(ctx, 1)
}

// IncrementLearnerPromoteSucceed increments the counter for successful learner promotions
func (o *OtelMetricsCollector) IncrementLearnerPromoteSucceed() {
	ctx := context.TODO()
	o.learnerPromoteSucceed.Add(ctx, 1)
}

// IncrementLearnerPromoteFailed increments the counter for failed learner promotions
func (o *OtelMetricsCollector) IncrementLearnerPromoteFailed() {
	ctx := context.TODO()
	o.learnerPromoteFailed.Add(ctx, 1)
}

// SetIsLearner sets whether this node is a learner
func (o *OtelMetricsCollector) SetIsLearner(isLearner int64) {
	o.isLearner.Store(isLearner)
}

// RecordApplySec records the duration of an apply operation
func (o *OtelMetricsCollector) RecordApplySec(duration float64) {
	ctx := context.TODO()
	o.applyHistogram.Record(ctx, duration)
}

// RecordDoSnapshotSec records the duration of a snapshot creation
func (o *OtelMetricsCollector) RecordDoSnapshotSec(duration float64) {
	ctx := context.TODO()
	o.doSnapshotHistogram.Record(ctx, duration)
}

// RecordApplySnapshotSec records the duration of a snapshot apply operation
func (o *OtelMetricsCollector) RecordApplySnapshotSec(duration float64) {
	ctx := context.Background()
	o.applySnapshotHistogram.Record(ctx, duration)
}

// SetSnapshotApplyInProgress sets whether a snapshot apply is in progress
func (o *OtelMetricsCollector) SetSnapshotApplyInProgress(applying int64) {
	o.snapshotApply.Store(applying)
}

// IncrementInflightSnapshots increments the counter for in-flight snapshots
func (o *OtelMetricsCollector) IncrementInflightSnapshots() {
	ctx := context.Background()
	o.inflightSnapshotCounter.Add(ctx, 1)
}

// DecrementInflightSnapshots decrements the counter for in-flight snapshots
func (o *OtelMetricsCollector) DecrementInflightSnapshots() {
	ctx := context.Background()
	o.inflightSnapshotCounter.Add(ctx, -1)
}

// SetProposalCommited sets the number of committed proposals
func (o *OtelMetricsCollector) SetProposalCommited(commitedEntries uint64) {
	o.proposalCommited.Store(int64(commitedEntries))
}

// SetProposalAppliedIndex sets the index of the last applied proposal
func (o *OtelMetricsCollector) SetProposalAppliedIndex(appliedIndex uint64) {
	o.proposalApplied.Store(int64(appliedIndex))
}

// IncrementProposalPending increments the counter for pending proposals
func (o *OtelMetricsCollector) IncrementProposalPending() {
	ctx := context.Background()
	o.proposalPendingCounter.Add(ctx, 1)
}

// DecrementProposalPending decrements the counter for pending proposals
func (o *OtelMetricsCollector) DecrementProposalPending() {
	ctx := context.TODO()
	o.proposalPendingCounter.Add(ctx, -1)
}

// IncrementProposalFailed increments the counter for failed proposals
func (o *OtelMetricsCollector) IncrementProposalFailed() {
	ctx := context.TODO()
	o.proposalFailedCounter.Add(ctx, 1)
}

// IncrementSlowReadIndex increments the counter for slow read indexes
func (o *OtelMetricsCollector) IncrementSlowReadIndex() {
	ctx := context.TODO()
	o.slowReadIndexCounter.Add(ctx, 1)
}

// IncrementReadIndexFailed increments the counter for failed read indexes
func (o *OtelMetricsCollector) IncrementReadIndexFailed() {
	ctx := context.TODO()
	o.readIndexFailedCounter.Add(ctx, 1)
}
