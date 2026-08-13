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
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	kahunav1alpha1 "github.com/kahunakv/k8s-operator/api/v1alpha1"
	"github.com/kahunakv/k8s-operator/internal/kahuna"
)

// podBaseURL addresses a pod by IP rather than by its DNS name. Pod IPs are reachable from the
// operator without depending on cluster DNS resolving inside the operator's own network
// namespace, which matters when the manager runs outside the cluster during development.
func podBaseURL(cluster *kahunav1alpha1.KahunaCluster, pod *corev1.Pod) string {
	return fmt.Sprintf("http://%s:%d", pod.Status.PodIP, ports(cluster).HTTP)
}

// ordinalOf extracts the StatefulSet ordinal from a pod name, or -1 when the name does not carry
// one. Only ordering uses this, so an unparseable name sorts first rather than failing a reconcile.
func ordinalOf(pod string) int32 {
	idx := strings.LastIndex(pod, "-")
	if idx < 0 {
		return -1
	}
	n, err := strconv.Atoi(pod[idx+1:])
	if err != nil || n < 0 {
		return -1
	}
	return int32(n)
}

// podIsReady reports whether the kubelet considers the pod ready — which, given the readiness
// probe points at /v1/cluster/health, means the node is initialized and holds a serving role.
func podIsReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// readMembership asks the cluster for its committed roster.
//
// Any node can answer, so it tries ready pods first and falls back to merely running ones: during
// bootstrap no pod is ready yet, but they can already answer membership queries, and that early
// view is exactly what tells the operator whether the roster has been seeded.
func (r *KahunaClusterReconciler) readMembership(ctx context.Context, cluster *kahunav1alpha1.KahunaCluster, pods []corev1.Pod) (*kahuna.Membership, error) {
	candidates := make([]corev1.Pod, 0, len(pods))
	for _, p := range pods {
		if p.Status.PodIP != "" && p.DeletionTimestamp == nil {
			candidates = append(candidates, p)
		}
	}
	// Ready pods first, then by ordinal so the choice is deterministic and the logs are stable.
	slices.SortStableFunc(candidates, func(a, b corev1.Pod) int {
		ra, rb := podIsReady(&a), podIsReady(&b)
		if ra != rb {
			if ra {
				return -1
			}
			return 1
		}
		return int(ordinalOf(a.Name) - ordinalOf(b.Name))
	})

	var lastErr error
	for i := range candidates {
		m, err := r.Kahuna.Membership(ctx, podBaseURL(cluster, &candidates[i]))
		if err != nil {
			lastErr = err
			continue
		}
		return m, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no pod was reachable")
	}
	return nil, lastErr
}

// buildMemberStatus joins the committed roster to the pods serving it.
//
// The join is the interesting part: a roster entry with no pod is a member the cluster still
// counts but Kubernetes no longer runs — a node evicted from the StatefulSet whose removal has
// not yet been committed, which is precisely the state scale-down waits out.
func buildMemberStatus(cluster *kahunav1alpha1.KahunaCluster, m *kahuna.Membership, pods []corev1.Pod) []kahunav1alpha1.MemberStatus {
	byName := make(map[string]*corev1.Pod, len(pods))
	for i := range pods {
		byName[pods[i].Name] = &pods[i]
	}

	// Roster endpoints are the per-pod DNS names the operator generated, so they map back to
	// ordinals without parsing the address.
	endpointToPod := make(map[string]string)
	for ordinal := range int32(len(pods)) + desiredReplicas(cluster) {
		endpointToPod[raftEndpoint(cluster, ordinal)] = podName(cluster, ordinal)
	}

	out := make([]kahunav1alpha1.MemberStatus, 0, len(m.Members))
	for _, mem := range m.Members {
		st := kahunav1alpha1.MemberStatus{
			Endpoint:      mem.Endpoint,
			NodeID:        mem.NodeID,
			Role:          mem.Role,
			JoinedVersion: mem.JoinedVersion,
		}
		if name, ok := endpointToPod[mem.Endpoint]; ok {
			st.PodName = name
			if pod, ok := byName[name]; ok {
				st.Ready = podIsReady(pod)
			}
		}
		out = append(out, st)
	}
	slices.SortFunc(out, func(a, b kahunav1alpha1.MemberStatus) int {
		return strings.Compare(a.Endpoint, b.Endpoint)
	})
	return out
}

// countRoles tallies the roster by role. Only Voters count toward quorum; Learners are still
// catching up and do not help a cluster make progress.
func countRoles(members []kahunav1alpha1.MemberStatus) (voters, learners int32) {
	for _, m := range members {
		switch m.Role {
		case kahuna.RoleVoter:
			voters++
		case kahuna.RoleLearner:
			learners++
		}
	}
	return voters, learners
}

// decommission asks the node at the given ordinal to remove itself from the committed roster, and
// reports whether it is safe to take the pod away.
//
// Returning false is not a failure — it means "not yet". The caller holds the cluster at its
// current size and tries again, because deleting a pod that is still a committed member is exactly
// the situation the leave API exists to avoid.
func (r *KahunaClusterReconciler) decommission(ctx context.Context, cluster *kahunav1alpha1.KahunaCluster, pods []corev1.Pod, ordinal int32) (bool, string) {
	log := logf.FromContext(ctx)
	name := podName(cluster, ordinal)

	var pod *corev1.Pod
	for i := range pods {
		if pods[i].Name == name && pods[i].Status.PodIP != "" {
			pod = &pods[i]
			break
		}
	}
	if pod == nil {
		// No pod to ask. It is already gone, so the cluster will evict it through the failure
		// detector — slower, but not wrong, and there is nothing better available.
		log.Info("node has no reachable pod to decommission; falling back to eviction", "pod", name)
		return true, ""
	}

	result, err := r.Kahuna.Leave(ctx, podBaseURL(cluster, pod))
	if err != nil {
		return false, fmt.Sprintf("could not ask %s to leave the cluster: %v", name, err)
	}

	if result.Removed() {
		log.Info("node left the cluster", "pod", name,
			"outcome", result.Outcome, "membershipVersion", result.MembershipVersion)
		return true, ""
	}

	// A permanent refusal will not pass on its own. Surfacing the server's own reason is more
	// useful than restating it: it names the remedy.
	if !result.Retryable {
		return false, fmt.Sprintf("%s refused to leave: %s", name, result.Reason)
	}
	return false, fmt.Sprintf("waiting for %s to leave the cluster: %s", name, result.Reason)
}
