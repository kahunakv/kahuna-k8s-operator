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
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	kahunav1alpha1 "github.com/kahunakv/k8s-operator/api/v1alpha1"
	"github.com/kahunakv/k8s-operator/internal/kahuna"
)

const (
	// metricWritesAdmitted counts direct key/value writes. The aggregator that records it is
	// leader-local, so summing it across pods counts each write once.
	metricWritesAdmitted = "kahuna.kv.write.admitted"
	// metricClientQueueDepth is the number of client proposals waiting in each partition executor.
	metricClientQueueDepth = "raft.executor.client_queue_depth"

	// maxReplicationFactor is the most copies of a data partition the autoscaler will create. A
	// fully replicated partition has one copy per node, so while any partition is fully replicated
	// the autoscaler does not grow the cluster past this.
	maxReplicationFactor = 3

	// minSampleInterval keeps rates from being computed over a window so short that one batch
	// boundary dominates. Reconciles triggered by status writes arrive much faster than this.
	minSampleInterval = 10 * time.Second

	// requeueAutoscaling is the resync interval for an autoscaled cluster. It bounds how stale the
	// traffic signal can be when a decision is made.
	requeueAutoscaling = 15 * time.Second

	defaultScaleUpStabilization   = 60 * time.Second
	defaultScaleDownStabilization = 300 * time.Second
	defaultTolerancePercent       = 10

	// placementSettleTimeout bounds how long a step waits for the rebalancer to give every node its
	// share of the data partitions. The wait exists because a node that holds nothing adds no
	// capacity and reads zero traffic, which drags the per-node average down just when it should
	// not. The bound exists because the server's planner stops at its own notion of even — a
	// cluster with fewer placed replicas than it has nodes leaves a node empty for good — and the
	// autoscaler must not hang on that. Moves still in flight are never waited out by this clock.
	placementSettleTimeout = 5 * time.Minute
)

// Replica roles reported by GET /v1/cluster/placement. Anything but a Voter is a move in flight.
const (
	replicaRoleVoter = "Voter"
)

// podSample is the last counter reading taken from one pod.
type podSample struct {
	writesTotal float64
	monotonicMs int64
	// rate is the write rate computed at the last accepted reading. hasRate is false until two
	// readings exist, and again after a counter reset.
	rate    float64
	hasRate bool
}

// trafficSampler remembers counter readings between reconciles so cumulative counters can be
// turned into rates. It is in memory on purpose: losing it on an operator restart costs one
// sampling interval, while persisting it would put a status write on every reconcile.
//
// The zero value is ready to use.
type trafficSampler struct {
	mu sync.Mutex
	// clusters maps a KahunaCluster to its pods' samples, keyed by pod UID so a recreated pod
	// never inherits its predecessor's counters.
	clusters map[types.NamespacedName]map[types.UID]podSample
}

// observeSample records a reading for one pod and returns that pod's current write rate, if known.
func observeSample(samples map[types.UID]podSample, uid types.UID, total float64, monotonicMs int64) (float64, bool) {
	prev, seen := samples[uid]
	switch {
	case !seen, total < prev.writesTotal, monotonicMs <= prev.monotonicMs:
		// First reading, or the counter or the clock went backwards — the container restarted
		// under the same pod. Either way there is no baseline to compute a rate from.
		samples[uid] = podSample{writesTotal: total, monotonicMs: monotonicMs}
		return 0, false
	case time.Duration(monotonicMs-prev.monotonicMs)*time.Millisecond < minSampleInterval:
		// Too soon for a new reading to mean anything; keep the baseline and reuse the last rate.
		return prev.rate, prev.hasRate
	}
	rate := (total - prev.writesTotal) / (float64(monotonicMs-prev.monotonicMs) / 1000)
	samples[uid] = podSample{writesTotal: total, monotonicMs: monotonicMs, rate: rate, hasRate: true}
	return rate, true
}

// trafficSignal is the averaged traffic of a cluster. A nil field means the signal is not known on
// this pass, which the decision treats as "do not act on it".
type trafficSignal struct {
	writesPerNode *float64
	queuePerNode  *float64
}

// measure reads the dashboard metrics of every ready pod and averages them per node.
//
// Every ready pod must answer. Writes concentrate on partition leaders, so a missing pod is not a
// rounding error: it can hide most of the load, and a scale-down decided on that would be wrong.
func (r *KahunaClusterReconciler) measure(ctx context.Context, cluster *kahunav1alpha1.KahunaCluster, pods []corev1.Pod) (trafficSignal, error) {
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}

	ready := make([]*corev1.Pod, 0, len(pods))
	for i := range pods {
		if pods[i].Status.PodIP != "" && pods[i].DeletionTimestamp == nil && podIsReady(&pods[i]) {
			ready = append(ready, &pods[i])
		}
	}
	if len(ready) == 0 {
		return trafficSignal{}, fmt.Errorf("no ready pod to read metrics from")
	}

	// Fetch outside the lock: these are network calls, and the sampler is shared by every cluster.
	readings := make([]*kahuna.Metrics, len(ready))
	for i, pod := range ready {
		m, err := r.Kahuna.Metrics(ctx, podBaseURL(cluster, pod))
		if err != nil {
			return trafficSignal{}, fmt.Errorf("reading metrics from %s: %w", pod.Name, err)
		}
		readings[i] = m
	}

	r.traffic.mu.Lock()
	defer r.traffic.mu.Unlock()
	if r.traffic.clusters == nil {
		r.traffic.clusters = make(map[types.NamespacedName]map[types.UID]podSample)
	}
	samples := r.traffic.clusters[key]
	if samples == nil {
		samples = make(map[types.UID]podSample, len(ready))
		r.traffic.clusters[key] = samples
	}

	var writes, queue float64
	writesKnown := true
	live := make(map[types.UID]bool, len(ready))
	for i, pod := range ready {
		live[pod.UID] = true
		// A metric with no rows yet has simply never been recorded on that node: no write has
		// reached it, or it hosts no partition executor. Zero is the right reading.
		total, _ := readings[i].CounterTotal(metricWritesAdmitted)
		depth, _ := readings[i].GaugeSum(metricClientQueueDepth)
		rate, ok := observeSample(samples, pod.UID, total, readings[i].MonotonicMs)
		if !ok {
			writesKnown = false
		}
		writes += rate
		queue += depth
	}
	// Samples of pods that are gone would otherwise accumulate for the life of the operator.
	for uid := range samples {
		if !live[uid] {
			delete(samples, uid)
		}
	}

	n := float64(len(ready))
	sig := trafficSignal{queuePerNode: ptr.To(queue / n)}
	if writesKnown {
		sig.writesPerNode = ptr.To(writes / n)
	}
	return sig, nil
}

// forget drops a cluster's samples once it no longer autoscales or no longer exists.
func (s *trafficSampler) forget(key types.NamespacedName) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.clusters, key)
}

// autoscaleInput is everything a decision depends on, gathered so the decision itself is a pure
// function that can be tested without a cluster.
type autoscaleInput struct {
	spec    *kahunav1alpha1.AutoscalingSpec
	prev    *kahunav1alpha1.AutoscalingStatus
	initial int32
	signal  trafficSignal
	// converged is true when the roster and the ready pods match the current desired size. No step
	// is taken otherwise: the previous one is still in flight, and its catch-up traffic would
	// distort the signal anyway.
	converged bool
	// growthCeiling is the largest size the cluster may grow to on this pass, independent of the
	// spec bounds. Zero means no extra ceiling.
	growthCeiling int32
	ceilingReason string
	now           time.Time
}

// autoscaleDecision is the new autoscaler state plus why it is not where the traffic wants it.
type autoscaleDecision struct {
	status  kahunav1alpha1.AutoscalingStatus
	limited bool
	reason  string
}

// decideAutoscale picks the desired node count.
//
// The recommendation follows the HorizontalPodAutoscaler's rule — scale the current size by the
// worst signal's ratio to its target, with a dead band around 1 — but a step is at most one node and
// is taken only after the recommendation has held for the whole stabilization window. One node at
// a time is not caution for its own sake: each step is a committed membership change plus a catch-up
// or a drain, and the server admits one at a time anyway.
func decideAutoscale(in autoscaleInput) autoscaleDecision {
	spec := in.spec
	st := kahunav1alpha1.AutoscalingStatus{}
	if in.prev != nil {
		st = *in.prev.DeepCopy()
	}
	if st.DesiredReplicas == 0 {
		st.DesiredReplicas = in.initial
	}
	// A bound edited past the current size is explicit intent and applies at once; the plan still
	// walks the StatefulSet there one node at a time. It is a scale like any other, so it starts
	// the clock that bounds the wait for placement to settle afterwards.
	if clamped := min(max(st.DesiredReplicas, spec.MinReplicas), spec.MaxReplicas); clamped != st.DesiredReplicas {
		st.DesiredReplicas = clamped
		st.LastScaleTime = ptr.To(metav1.NewTime(in.now))
	}
	current := st.DesiredReplicas

	st.WritesPerSecondPerNode = roundedPtr(in.signal.writesPerNode)
	st.QueueDepthPerNode = roundedPtr(in.signal.queuePerNode)

	hold := func(reason string) autoscaleDecision {
		st.RecommendedReplicas = current
		st.AboveTargetSince, st.BelowTargetSince = nil, nil
		return autoscaleDecision{status: st, limited: reason != "", reason: reason}
	}

	if !in.converged {
		// Not "limited": a scale in progress is the autoscaler working, not being blocked.
		return hold("")
	}

	var ratios []float64
	allKnown := true
	if t := spec.TargetWritesPerSecond; t != nil {
		if v := in.signal.writesPerNode; v != nil {
			ratios = append(ratios, *v/float64(*t))
		} else {
			allKnown = false
		}
	}
	if t := spec.TargetQueueDepth; t != nil {
		if v := in.signal.queuePerNode; v != nil {
			ratios = append(ratios, *v/float64(*t))
		} else {
			allKnown = false
		}
	}
	if len(ratios) == 0 {
		return hold("no traffic signal is available yet")
	}

	ratio := ratios[0]
	for _, x := range ratios[1:] {
		ratio = max(ratio, x)
	}

	tolerance := float64(defaultTolerancePercent) / 100
	if spec.TolerancePercent != nil {
		tolerance = float64(*spec.TolerancePercent) / 100
	}

	rec := current
	if math.Abs(ratio-1) > tolerance {
		rec = int32(math.Ceil(float64(current) * ratio))
	}
	// Shrinking on a partial view is how a cluster loses the node it needed: a signal that could
	// not be read may be the one that is high.
	if rec < current && !allKnown {
		rec = current
	}
	rec = min(max(rec, spec.MinReplicas), spec.MaxReplicas)
	st.RecommendedReplicas = rec

	upWindow := secondsOr(spec.ScaleUpStabilizationSeconds, defaultScaleUpStabilization)
	downWindow := secondsOr(spec.ScaleDownStabilizationSeconds, defaultScaleDownStabilization)
	now := metav1.NewTime(in.now)

	switch {
	case rec > current:
		st.BelowTargetSince = nil
		if in.growthCeiling > 0 && current >= in.growthCeiling {
			st.AboveTargetSince = nil
			return autoscaleDecision{status: st, limited: true, reason: in.ceilingReason}
		}
		if st.AboveTargetSince == nil {
			st.AboveTargetSince = &now
		}
		if in.now.Sub(st.AboveTargetSince.Time) >= upWindow {
			st.DesiredReplicas = current + 1
			st.LastScaleTime = &now
			st.AboveTargetSince = nil
		}
	case rec < current:
		st.AboveTargetSince = nil
		if st.BelowTargetSince == nil {
			st.BelowTargetSince = &now
		}
		if in.now.Sub(st.BelowTargetSince.Time) >= downWindow {
			st.DesiredReplicas = current - 1
			st.LastScaleTime = &now
			st.BelowTargetSince = nil
		}
	default:
		st.AboveTargetSince, st.BelowTargetSince = nil, nil
	}

	if rec == spec.MaxReplicas && ratio > 1+tolerance {
		return autoscaleDecision{status: st, limited: true,
			reason: fmt.Sprintf("traffic asks for more than maxReplicas (%d)", spec.MaxReplicas)}
	}
	return autoscaleDecision{status: st}
}

// growthCeiling decides how far the cluster may grow while still honouring the replication cap.
//
// A data partition with no replica set lives on every node, so each node added is another copy of
// it. The server never converts such a partition to a placed one, so the only way to keep it at or
// below maxReplicationFactor copies is to not grow past that many nodes.
func growthCeiling(p *kahuna.Placement) (int32, string) {
	if full := p.FullyReplicated(); len(full) > 0 {
		return maxReplicationFactor, fmt.Sprintf(
			"partitions %v are fully replicated, so growing past %d nodes would exceed %d copies; "+
				"the cluster must be created with more nodes than its replication factor",
			full, maxReplicationFactor, maxReplicationFactor)
	}
	return 0, ""
}

// placementSettled reports whether the server has finished acting on the last membership change:
// no replica is in a transitional role, and no replica sits on a node that is no longer a member.
// Both hold while the server is still moving data, so both are waited for as long as they hold.
// Replicas are counted per current member on the way, for the share check that builds on this.
//
// A nil placement is a server without the feature. It replicates every partition to every node
// and never moves one, so there is nothing to settle.
func placementSettled(cluster *kahunav1alpha1.KahunaCluster, p *kahuna.Placement, current int32) (hosted map[string]int, total int, reason string) {
	hosted = make(map[string]int, current)
	if p == nil {
		return hosted, 0, ""
	}
	members := make(map[string]bool, current)
	for i := range current {
		members[raftEndpoint(cluster, i)] = true
	}
	var inFlight, stranded []int32
	for _, part := range p.Partitions {
		// Partitions without a replica set are on every node; there is nothing to settle.
		if part.State != kahuna.PartitionStateActive || len(part.Replicas) == 0 {
			continue
		}
		for _, rep := range part.Replicas {
			switch {
			case rep.Role != replicaRoleVoter:
				inFlight = append(inFlight, part.PartitionID)
			case !members[rep.Endpoint]:
				stranded = append(stranded, part.PartitionID)
			default:
				hosted[rep.Endpoint]++
				total++
			}
		}
	}
	switch {
	case len(inFlight) > 0:
		return hosted, total, fmt.Sprintf("replica moves in flight on partitions %v", inFlight)
	case len(stranded) > 0:
		return hosted, total, fmt.Sprintf("partitions %v still have replicas on nodes that left", stranded)
	}
	return hosted, total, ""
}

// placementConverged is placementSettled plus the autoscaler's own requirement that every node
// carries a reasonable share of the data partitions. The roster converging says the membership
// change is committed; this says the data has followed it, which is what makes a node added for
// capacity actually carry some. The share is what the rebalancer is for, but its planner stops at
// its own notion of even, so that part is only waited for until settleBy.
func placementConverged(cluster *kahunav1alpha1.KahunaCluster, p *kahuna.Placement, current int32, now time.Time, settleBy *time.Time) (bool, string) {
	hosted, total, reason := placementSettled(cluster, p, current)
	if reason != "" {
		return false, reason
	}
	// With fewer placed replicas than nodes some node has to be empty, so there is no share to
	// wait for; the same goes once the settle clock has run out.
	share := total / int(current)
	if share == 0 || (settleBy != nil && !now.Before(*settleBy)) {
		return true, ""
	}
	// Half the even share is deliberately lax: the planner leaves counts uneven whenever the
	// replicas do not divide by the nodes, and this only has to tell "carrying data" from "empty".
	minShare := max(1, share/2)
	for i := range current {
		if n := hosted[raftEndpoint(cluster, i)]; n < minShare {
			return false, fmt.Sprintf("%s holds %d of the %d data replicas it should carry",
				podName(cluster, i), n, share)
		}
	}
	return true, ""
}

// reconcileAutoscaling updates the autoscaler state in cluster.Status. The caller persists it with
// the rest of status; every other step of the pass then reads the new size through desiredReplicas.
func (r *KahunaClusterReconciler) reconcileAutoscaling(ctx context.Context, cluster *kahunav1alpha1.KahunaCluster, bootstrapped bool, membership *kahuna.Membership, placement *kahuna.Placement, placementErr error, pods []corev1.Pod) {
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}
	spec := cluster.Spec.Autoscaling
	if spec == nil {
		cluster.Status.Autoscaling = nil
		apimeta.RemoveStatusCondition(&cluster.Status.Conditions, kahunav1alpha1.ConditionAutoscalingLimited)
		r.traffic.forget(key)
		return
	}
	log := logf.FromContext(ctx)

	current := desiredReplicas(cluster)
	// Only the nodes the cluster is being sized to count. A pod at or above the target ordinal is on
	// its way out, and its share of the traffic is already moving to the others.
	members := make([]corev1.Pod, 0, len(pods))
	readyCount := int32(0)
	for i := range pods {
		if o := ordinalOf(pods[i].Name); o < 0 || o >= current {
			continue
		}
		members = append(members, pods[i])
		if podIsReady(&pods[i]) && pods[i].DeletionTimestamp == nil {
			readyCount++
		}
	}
	now := time.Now()
	converged := bootstrapped && rosterMatches(cluster, membership, current) && readyCount == current

	// The roster says the step is committed; the placement says whether the data has followed.
	// Until it has, the previous step is still in progress and the next must not stack on it.
	waiting := ""
	if converged {
		switch {
		case placementErr != nil:
			// Not knowing whether the data has settled is a reason to wait, same as for growth.
			// A server without the feature cannot autoscale at all: the factor it needs arrived
			// with placement, and the replication cap below has nothing to read either.
			converged = false
			waiting = fmt.Sprintf("replica placement could not be read: %v", placementErr)
		default:
			var settleBy *time.Time
			if as := cluster.Status.Autoscaling; as != nil && as.LastScaleTime != nil {
				settleBy = ptr.To(as.LastScaleTime.Add(placementSettleTimeout))
			}
			var ok bool
			if ok, waiting = placementConverged(cluster, placement, current, now, settleBy); !ok {
				converged = false
			}
		}
	}

	in := autoscaleInput{
		spec:      spec,
		prev:      cluster.Status.Autoscaling,
		initial:   initialAutoscaledReplicas(cluster),
		converged: converged,
		now:       now,
	}

	var measureErr error
	if converged {
		in.signal, measureErr = r.measure(ctx, cluster, members)
		if measureErr != nil {
			log.V(1).Info("Could not read traffic metrics", "error", measureErr)
		}
		in.growthCeiling, in.ceilingReason = growthCeiling(placement)
	}

	d := decideAutoscale(in)
	switch {
	case measureErr != nil:
		// More specific than the decision's own "no signal" explanation, and the thing to fix.
		d.limited = true
		d.reason = fmt.Sprintf("traffic metrics could not be read: %v", measureErr)
	case waiting != "":
		// Not a limit either: the data is still following the last step.
		d.reason = "waiting for replica placement to settle: " + waiting
	}

	if d.status.DesiredReplicas != current {
		log.Info("Autoscaler changed desired replicas", "from", current, "to", d.status.DesiredReplicas,
			"recommended", d.status.RecommendedReplicas,
			"writesPerNode", ptr.Deref(d.status.WritesPerSecondPerNode, -1),
			"queuePerNode", ptr.Deref(d.status.QueueDepthPerNode, -1))
	}

	cluster.Status.Autoscaling = &d.status
	msg := d.reason
	if msg == "" {
		msg = fmt.Sprintf("desired %d, recommended %d", d.status.DesiredReplicas, d.status.RecommendedReplicas)
	}
	setCondition(&cluster.Status, kahunav1alpha1.ConditionAutoscalingLimited, d.limited,
		"Limited", "WithinLimits", msg)
}

// secondsOr converts an optional seconds field, falling back when it is unset.
func secondsOr(v *int32, fallback time.Duration) time.Duration {
	if v == nil {
		return fallback
	}
	return time.Duration(*v) * time.Second
}

// roundedPtr reports a measurement as a whole number for status, where fractions are noise.
func roundedPtr(v *float64) *int64 {
	if v == nil {
		return nil
	}
	return ptr.To(int64(math.Round(*v)))
}
