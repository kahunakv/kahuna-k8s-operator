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
	"fmt"

	kahunav1alpha1 "github.com/kahunakv/k8s-operator/api/v1alpha1"
	"github.com/kahunakv/k8s-operator/internal/kahuna"
)

// plan is what the operator wants the StatefulSet to look like on this pass.
type plan struct {
	// Replicas is the StatefulSet size to apply now. It is not always spec.replicas: membership
	// changes are committed one node at a time, so a multi-node scale walks there in steps.
	Replicas int32
	// Partition freezes rolling updates at and below this ordinal. 0 lets an update proceed.
	Partition int32
	// Reason explains a step that was deliberately not taken, for the Progressing condition.
	Reason string
}

// rosterMatches reports whether the committed roster is exactly the set of nodes a StatefulSet of
// the given size should have: every ordinal below it a Voter, and nothing else present.
//
// This single predicate gates both scale directions. Scaling up, it means the previous node
// finished catching up and was promoted. Scaling down, it means the removed node's eviction has
// been committed — the roster, not the pod's absence, is what says the cluster has let it go.
func rosterMatches(cluster *kahunav1alpha1.KahunaCluster, m *kahuna.Membership, replicas int32) bool {
	if m == nil {
		return false
	}
	if int32(len(m.Members)) != replicas {
		return false
	}
	for i := range replicas {
		if !m.HasVoter(raftEndpoint(cluster, i)) {
			return false
		}
	}
	return true
}

// buildPlan decides the StatefulSet size and rolling-update partition for this pass.
//
// currentReplicas is what the StatefulSet is set to right now (not what it has running), and
// membership may be nil when no node could be reached.
func buildPlan(cluster *kahunav1alpha1.KahunaCluster, currentReplicas int32, bootstrapped bool, m *kahuna.Membership) plan {
	target := desiredReplicas(cluster)

	// Greenfield: every node must come up at once. Each one blocks until it can form a quorum
	// with its peers, so bringing them up in steps would deadlock — there is no quorum to join.
	if !bootstrapped {
		return plan{Replicas: target, Partition: 0}
	}

	// A converged roster is the precondition for any further membership change, and also the
	// signal that it is safe to let a rolling update continue.
	converged := rosterMatches(cluster, m, currentReplicas)

	p := plan{Replicas: currentReplicas, Partition: 0}

	if !converged {
		// Hold every node in place. Whatever is wrong — a node still catching up, an eviction
		// not yet committed, no node reachable — taking another one down cannot help, and if
		// the cluster is near its quorum floor it would turn a degraded cluster into a dead one.
		p.Partition = currentReplicas
		p.Reason = rosterHoldReason(cluster, m, currentReplicas)
		return p
	}

	switch {
	case currentReplicas < target:
		// One at a time: Kommander admits a Learner, backfills it and promotes it as a single
		// committed transition. Adding several at once would leave the cluster carrying multiple
		// non-voting members while quorum is still computed from the old Voter set.
		p.Replicas = currentReplicas + 1
	case currentReplicas > target:
		// Also one at a time, and never below one node: the cluster refuses to remove its last
		// Voter, so asking for it would just stall.
		if currentReplicas > 1 {
			p.Replicas = currentReplicas - 1
		}
	}
	return p
}

// rosterHoldReason explains why the operator is not taking the next step.
func rosterHoldReason(cluster *kahunav1alpha1.KahunaCluster, m *kahuna.Membership, replicas int32) string {
	if m == nil {
		return "waiting for cluster membership to become readable"
	}
	var missing, extra []string
	for i := range replicas {
		if !m.HasVoter(raftEndpoint(cluster, i)) {
			missing = append(missing, podName(cluster, i))
		}
	}
	expected := make(map[string]bool, replicas)
	for i := range replicas {
		expected[raftEndpoint(cluster, i)] = true
	}
	for _, mem := range m.Members {
		if !expected[mem.Endpoint] {
			extra = append(extra, mem.Endpoint)
		}
	}
	switch {
	case len(missing) > 0 && len(extra) > 0:
		return fmt.Sprintf("waiting for %v to become voters and %v to be evicted", missing, extra)
	case len(missing) > 0:
		return fmt.Sprintf("waiting for %v to become voters", missing)
	case len(extra) > 0:
		// The common scale-down case: the pod is gone but the cluster still counts it. SWIM has
		// to run its suspicion and eviction-grace windows before the removal is committed.
		return fmt.Sprintf("waiting for the cluster to evict %v", extra)
	default:
		return "waiting for the roster to converge"
	}
}
