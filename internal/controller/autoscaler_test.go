/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	kahunav1alpha1 "github.com/kahunakv/k8s-operator/api/v1alpha1"
	"github.com/kahunakv/k8s-operator/internal/kahuna"
)

var t0 = time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)

func autoscaleSpec() *kahunav1alpha1.AutoscalingSpec {
	return &kahunav1alpha1.AutoscalingSpec{
		MinReplicas:                   3,
		MaxReplicas:                   7,
		TargetWritesPerSecond:         ptr.To(int32(1000)),
		ScaleUpStabilizationSeconds:   ptr.To(int32(60)),
		ScaleDownStabilizationSeconds: ptr.To(int32(300)),
	}
}

func writes(v float64) trafficSignal {
	return trafficSignal{writesPerNode: ptr.To(v), queuePerNode: ptr.To(0.0)}
}

// step runs decisions at the given offsets from t0, feeding each one the previous state, and
// returns the final state.
func step(t *testing.T, spec *kahunav1alpha1.AutoscalingSpec, start int32, sig trafficSignal, offsets ...time.Duration) autoscaleDecision {
	t.Helper()
	var prev *kahunav1alpha1.AutoscalingStatus
	var d autoscaleDecision
	for _, off := range offsets {
		d = decideAutoscale(autoscaleInput{
			spec: spec, prev: prev, initial: start, signal: sig, converged: true, now: t0.Add(off),
		})
		prev = &d.status
	}
	return d
}

func TestAutoscaleAddsOneNodeAfterTheScaleUpWindow(t *testing.T) {
	// Twice the target per node asks for twice the nodes, but only one is added per step.
	d := step(t, autoscaleSpec(), 4, writes(2000), 0)
	if d.status.DesiredReplicas != 4 || d.status.RecommendedReplicas != 7 {
		t.Fatalf("first pass: desired %d recommended %d, want 4 and 7 (capped by max)",
			d.status.DesiredReplicas, d.status.RecommendedReplicas)
	}
	if d.status.AboveTargetSince == nil {
		t.Fatal("the scale-up window did not start")
	}

	d = step(t, autoscaleSpec(), 4, writes(2000), 0, 30*time.Second)
	if d.status.DesiredReplicas != 4 {
		t.Fatalf("scaled up before the window elapsed: desired %d", d.status.DesiredReplicas)
	}

	d = step(t, autoscaleSpec(), 4, writes(2000), 0, 30*time.Second, 60*time.Second)
	if d.status.DesiredReplicas != 5 {
		t.Fatalf("desired %d after the window, want 5 (one node per step)", d.status.DesiredReplicas)
	}
	if d.status.AboveTargetSince != nil || d.status.LastScaleTime == nil {
		t.Fatal("a step must restart the window and record when it happened")
	}
}

func TestAutoscaleNeedsTheWholeScaleDownWindow(t *testing.T) {
	d := step(t, autoscaleSpec(), 5, writes(100), 0, 299*time.Second)
	if d.status.DesiredReplicas != 5 {
		t.Fatalf("scaled down early: desired %d", d.status.DesiredReplicas)
	}
	d = step(t, autoscaleSpec(), 5, writes(100), 0, 300*time.Second)
	if d.status.DesiredReplicas != 4 {
		t.Fatalf("desired %d after the window, want 4", d.status.DesiredReplicas)
	}
}

func TestAutoscaleSpikeInsideTheWindowRestartsIt(t *testing.T) {
	spec := autoscaleSpec()
	d := decideAutoscale(autoscaleInput{spec: spec, initial: 5, signal: writes(100), converged: true, now: t0})
	// Load returns to target: the below-target streak is broken.
	d = decideAutoscale(autoscaleInput{spec: spec, prev: &d.status, initial: 5, signal: writes(1000), converged: true, now: t0.Add(200 * time.Second)})
	if d.status.BelowTargetSince != nil {
		t.Fatal("a reading at target did not reset the scale-down window")
	}
	d = decideAutoscale(autoscaleInput{spec: spec, prev: &d.status, initial: 5, signal: writes(100), converged: true, now: t0.Add(300 * time.Second)})
	if d.status.DesiredReplicas != 5 {
		t.Fatalf("scaled down on a window that was interrupted: desired %d", d.status.DesiredReplicas)
	}
}

func TestAutoscaleDeadBandHoldsSteady(t *testing.T) {
	d := step(t, autoscaleSpec(), 4, writes(1050), 0, time.Hour)
	if d.status.DesiredReplicas != 4 || d.status.RecommendedReplicas != 4 {
		t.Fatalf("a reading inside the tolerance moved the cluster: %+v", d.status)
	}
}

func TestAutoscaleRespectsBounds(t *testing.T) {
	d := step(t, autoscaleSpec(), 3, writes(0), 0, time.Hour)
	if d.status.DesiredReplicas != 3 {
		t.Fatalf("shrank below minReplicas: %d", d.status.DesiredReplicas)
	}

	d = step(t, autoscaleSpec(), 7, writes(50000), 0, time.Hour)
	if d.status.DesiredReplicas != 7 {
		t.Fatalf("grew past maxReplicas: %d", d.status.DesiredReplicas)
	}
	if !d.limited {
		t.Fatal("traffic above maxReplicas must be reported as limited")
	}

	// A bound edited below the current size applies at once.
	spec := autoscaleSpec()
	spec.MaxReplicas = 4
	prev := &kahunav1alpha1.AutoscalingStatus{DesiredReplicas: 6}
	d = decideAutoscale(autoscaleInput{spec: spec, prev: prev, initial: 3, signal: writes(1000), converged: true, now: t0})
	if d.status.DesiredReplicas != 4 {
		t.Fatalf("desired %d after maxReplicas dropped to 4", d.status.DesiredReplicas)
	}
}

func TestAutoscaleHoldsWhileTheRosterConverges(t *testing.T) {
	spec := autoscaleSpec()
	prev := &kahunav1alpha1.AutoscalingStatus{DesiredReplicas: 5, AboveTargetSince: ptr.To(metav1.NewTime(t0))}
	d := decideAutoscale(autoscaleInput{spec: spec, prev: prev, initial: 3, signal: writes(5000), converged: false, now: t0.Add(time.Hour)})
	if d.status.DesiredReplicas != 5 {
		t.Fatalf("stacked a step on one still in flight: desired %d", d.status.DesiredReplicas)
	}
	if d.status.AboveTargetSince != nil {
		t.Fatal("a window must not accumulate while the previous step is still converging")
	}
	if d.limited {
		t.Fatal("a scale in progress is not a limit")
	}
}

func TestAutoscaleNeverShrinksOnAPartialSignal(t *testing.T) {
	spec := autoscaleSpec()
	spec.TargetQueueDepth = ptr.To(int32(10))
	// Writes are known and low; the queue depth could not be read and might be the high one.
	sig := trafficSignal{writesPerNode: ptr.To(10.0)}
	d := step(t, spec, 5, sig, 0, time.Hour)
	if d.status.DesiredReplicas != 5 {
		t.Fatalf("shrank with a signal missing: desired %d", d.status.DesiredReplicas)
	}
}

func TestAutoscaleGrowsOnEitherSignal(t *testing.T) {
	spec := autoscaleSpec()
	spec.TargetQueueDepth = ptr.To(int32(10))
	sig := trafficSignal{writesPerNode: ptr.To(10.0), queuePerNode: ptr.To(40.0)}
	d := step(t, spec, 4, sig, 0, time.Minute)
	if d.status.DesiredReplicas != 5 {
		t.Fatalf("a queue four times its target did not add a node: desired %d", d.status.DesiredReplicas)
	}
}

func TestAutoscaleHoldsAtTheReplicationCeiling(t *testing.T) {
	spec := autoscaleSpec()
	var prev *kahunav1alpha1.AutoscalingStatus
	var d autoscaleDecision
	for _, off := range []time.Duration{0, time.Hour} {
		d = decideAutoscale(autoscaleInput{
			spec: spec, prev: prev, initial: 3, signal: writes(5000), converged: true,
			growthCeiling: 3, ceilingReason: "fully replicated", now: t0.Add(off),
		})
		prev = &d.status
	}
	if d.status.DesiredReplicas != 3 {
		t.Fatalf("grew a fully replicated cluster past the replication cap: desired %d", d.status.DesiredReplicas)
	}
	if !d.limited || d.reason != "fully replicated" {
		t.Fatalf("the ceiling was not reported: limited=%v reason=%q", d.limited, d.reason)
	}
}

func TestAutoscaleWaitsForASignal(t *testing.T) {
	d := step(t, autoscaleSpec(), 4, trafficSignal{}, 0, time.Hour)
	if d.status.DesiredReplicas != 4 || !d.limited {
		t.Fatalf("acted with no signal, or did not say so: %+v limited=%v", d.status, d.limited)
	}
}

func TestSamplerComputesRatesAndSurvivesRestarts(t *testing.T) {
	samples := map[types.UID]podSample{}

	if _, ok := observeSample(samples, "pod", 1000, 0); ok {
		t.Fatal("a first reading has no baseline and must not produce a rate")
	}
	// Too soon: no new rate yet, and the baseline is kept.
	if _, ok := observeSample(samples, "pod", 1500, 5_000); ok {
		t.Fatal("a reading inside the minimum interval produced a rate")
	}
	rate, ok := observeSample(samples, "pod", 3000, 20_000)
	if !ok || rate != 100 {
		t.Fatalf("rate %v ok=%v, want 100 writes/s over 20 s", rate, ok)
	}
	// Reused, not recomputed, inside the interval.
	if r, ok := observeSample(samples, "pod", 3100, 21_000); !ok || r != 100 {
		t.Fatalf("cached rate %v ok=%v, want 100", r, ok)
	}

	// The container restarted under the same pod: the counter went backwards.
	if _, ok := observeSample(samples, "pod", 10, 40_000); ok {
		t.Fatal("a counter reset produced a rate (it would be negative)")
	}
	rate, ok = observeSample(samples, "pod", 1010, 50_000)
	if !ok || rate != 100 {
		t.Fatalf("rate after reset %v ok=%v, want 100", rate, ok)
	}
}

func TestPlacementFindsFullyReplicatedDataPartitions(t *testing.T) {
	p := &kahuna.Placement{Partitions: []kahuna.PartitionPlacement{
		// The system partition always spans every node; it is not a violation.
		{PartitionID: 0, State: kahuna.PartitionStateActive},
		{PartitionID: 1, State: kahuna.PartitionStateActive, Replicas: []kahuna.ReplicaPlacement{{Endpoint: "a"}}},
		{PartitionID: 2, State: kahuna.PartitionStateActive},
		{PartitionID: 3, State: "Removed"},
	}}
	full := p.FullyReplicated()
	if len(full) != 1 || full[0] != 2 {
		t.Fatalf("fully replicated partitions %v, want [2]", full)
	}
}

func TestMetricsSumAcrossTagSets(t *testing.T) {
	m := &kahuna.Metrics{Rows: []kahuna.MetricRow{
		{Metric: metricWritesAdmitted, Kind: kahuna.MetricKindCounter, Tags: "class=ordinary", Total: ptr.To(30.0)},
		{Metric: metricWritesAdmitted, Kind: kahuna.MetricKindCounter, Tags: "class=terminal", Total: ptr.To(12.0)},
		{Metric: metricClientQueueDepth, Kind: kahuna.MetricKindGauge, Tags: "partition_id=1", Last: ptr.To(4.0)},
		{Metric: metricClientQueueDepth, Kind: kahuna.MetricKindGauge, Tags: "partition_id=2", Last: ptr.To(6.0)},
	}}
	if v, ok := m.CounterTotal(metricWritesAdmitted); !ok || v != 42 {
		t.Fatalf("writes %v ok=%v, want 42", v, ok)
	}
	if v, ok := m.GaugeSum(metricClientQueueDepth); !ok || v != 10 {
		t.Fatalf("queue %v ok=%v, want 10", v, ok)
	}
}

func TestReplicationFactorEnablesTheRebalancer(t *testing.T) {
	cluster := testCluster(4)
	if hasFlag(staticArgs(cluster), "--raft-replication-factor") {
		t.Fatal("a cluster without spec.replication must keep the server's full-replication default")
	}

	cluster.Spec.Replication = &kahunav1alpha1.ReplicationSpec{Factor: 3}
	args := staticArgs(cluster)
	if got := argValues(args, "--raft-replication-factor"); len(got) != 1 || got[0] != "3" {
		t.Fatalf("--raft-replication-factor %v, want [3]", got)
	}
	// Without the rebalancer a node added later never receives a replica.
	if !hasFlag(args, "--raft-enable-placement-rebalancer") {
		t.Fatal("the replication factor was set without the placement rebalancer")
	}
	// And with the server's default pacing it receives them too slowly to matter, then stops
	// short of its share: the three settings travel together (see the entrypoint for why).
	for flag, want := range map[string]string{
		"--raft-max-concurrent-replica-transfers": "4",
		"--raft-max-replica-moves-per-pass":       "8",
		"--raft-replica-count-deadband":           "0",
	} {
		if got := argValues(args, flag); len(got) != 1 || got[0] != want {
			t.Errorf("%s = %v, want [%s]", flag, got, want)
		}
	}
}

func TestDesiredReplicasFollowsTheAutoscaler(t *testing.T) {
	cluster := testCluster(5)
	cluster.Spec.Autoscaling = &kahunav1alpha1.AutoscalingSpec{MinReplicas: 3, MaxReplicas: 4}
	if got := desiredReplicas(cluster); got != 4 {
		t.Fatalf("starting size %d, want spec.replicas clamped to maxReplicas (4)", got)
	}
	cluster.Status.Autoscaling = &kahunav1alpha1.AutoscalingStatus{DesiredReplicas: 3}
	if got := desiredReplicas(cluster); got != 3 {
		t.Fatalf("desired %d, want the autoscaler's choice (3)", got)
	}
	cluster.Spec.Autoscaling = nil
	if got := desiredReplicas(cluster); got != 5 {
		t.Fatalf("desired %d after autoscaling was removed, want spec.replicas (5)", got)
	}
}
