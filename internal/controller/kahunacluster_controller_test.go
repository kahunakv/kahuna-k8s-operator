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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kahunav1alpha1 "github.com/kahunakv/k8s-operator/api/v1alpha1"
	"github.com/kahunakv/k8s-operator/internal/kahuna"
)

// fakeKahuna serves a canned roster so reconciler behaviour can be exercised without a running
// Kahuna cluster. err models "no node was reachable", which the operator must treat as a reason
// to hold rather than as a reason to act.
type fakeKahuna struct {
	membership *kahuna.Membership
	err        error
	// leaveResult is what a decommission request returns; nil means a committed removal.
	leaveResult *kahuna.LeaveResult
	leaveErr    error
	// leaveCalls records which base URLs were asked to leave, so a test can assert the operator
	// asked before removing rather than just removing.
	leaveCalls []string
}

func (f *fakeKahuna) Membership(context.Context, string) (*kahuna.Membership, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.membership, nil
}

func (f *fakeKahuna) Health(context.Context, string) (*kahuna.Health, error) {
	return &kahuna.Health{Ready: true, Initialized: true, LocalRole: kahuna.RoleVoter}, nil
}

func (f *fakeKahuna) Leave(_ context.Context, baseURL string) (*kahuna.LeaveResult, error) {
	f.leaveCalls = append(f.leaveCalls, baseURL)
	if f.leaveErr != nil {
		return nil, f.leaveErr
	}
	if f.leaveResult != nil {
		return f.leaveResult, nil
	}
	return &kahuna.LeaveResult{Left: true, Outcome: kahuna.OutcomeCommitted}, nil
}

// rosterOf builds a roster of Voters for ordinals 0..n-1 of the given cluster.
func rosterOf(cluster *kahunav1alpha1.KahunaCluster, n int32) *kahuna.Membership {
	m := &kahuna.Membership{MembershipVersion: int64(n), Initialized: true, LocalRole: kahuna.RoleVoter}
	for i := int32(0); i < n; i++ {
		m.Members = append(m.Members, kahuna.Member{
			Endpoint: raftEndpoint(cluster, i),
			NodeID:   nodeIDForOrdinal(i),
			Role:     kahuna.RoleVoter,
		})
	}
	return m
}

var _ = Describe("KahunaCluster Controller", func() {
	const namespace = "default"

	var (
		ctx        context.Context
		name       string
		nn         types.NamespacedName
		cluster    *kahunav1alpha1.KahunaCluster
		reconciler *KahunaClusterReconciler
		counter    int
	)

	newCluster := func(replicas int32) *kahunav1alpha1.KahunaCluster {
		return &kahunav1alpha1.KahunaCluster{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: kahunav1alpha1.KahunaClusterSpec{
				Replicas:   ptr.To(replicas),
				Image:      "kahunakv/kahuna:dev",
				Partitions: ptr.To(int32(3)),
				Storage: kahunav1alpha1.StorageSpec{
					Backend: kahunav1alpha1.StorageBackendRocksDB,
					Data:    kahunav1alpha1.VolumeSpec{Size: resource.MustParse("1Gi")},
				},
				TLS: kahunav1alpha1.TLSSpec{
					SecretRef: &corev1.LocalObjectReference{Name: "kahuna-tls"},
				},
			},
		}
	}

	// ensurePods stands in for the StatefulSet controller, which envtest does not run. The
	// reconciler reaches nodes by pod IP, so a roster is only observable when pods carrying one
	// exist.
	ensurePods := func(n int32) {
		for i := int32(0); i < n; i++ {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      podName(cluster, i),
					Namespace: namespace,
					Labels:    selectorLabels(cluster),
				},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: containerName, Image: "kahunakv/kahuna:dev"}}},
			}
			if err := k8sClient.Create(ctx, pod); err != nil {
				Expect(apierrors.IsAlreadyExists(err)).To(BeTrue(), "%v", err)
				continue
			}
			pod.Status.PodIP = fmt.Sprintf("10.244.0.%d", i+1)
			pod.Status.Conditions = []corev1.PodCondition{{
				Type: corev1.PodReady, Status: corev1.ConditionTrue,
				LastTransitionTime: metav1.Now(),
			}}
			Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
		}
	}

	// syncSTSStatus stands in for the StatefulSet controller, which envtest also does not run.
	// Pods count as ready only up to the roster size, mirroring reality: a node just added by a
	// scale is not ready until it has joined.
	syncSTSStatus := func(rosterSize int32) {
		sts := &appsv1.StatefulSet{}
		if err := k8sClient.Get(ctx, nn, sts); err != nil {
			return
		}
		want := *sts.Spec.Replicas
		ready := want
		if rosterSize < ready {
			ready = rosterSize
		}
		sts.Status.Replicas = want
		sts.Status.ReadyReplicas = ready
		sts.Status.CurrentReplicas = want
		Expect(k8sClient.Status().Update(ctx, sts)).To(Succeed())
	}

	// reconcile runs a pass with the given roster view and returns the fake, so a test can assert
	// on what the operator asked the cluster to do.
	reconcileUsing := func(fake *fakeKahuna) *fakeKahuna {
		n := int32(3)
		if fake.membership != nil {
			n = int32(len(fake.membership.Members))
		}
		ensurePods(n)
		syncSTSStatus(n)
		reconciler.Kahuna = fake
		_, rerr := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		Expect(rerr).NotTo(HaveOccurred())
		return fake
	}

	reconcileWith := func(m *kahuna.Membership, err error) {
		reconcileUsing(&fakeKahuna{membership: m, err: err})
	}

	getSTS := func() *appsv1.StatefulSet {
		sts := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, nn, sts)).To(Succeed())
		return sts
	}

	getCluster := func() *kahunav1alpha1.KahunaCluster {
		out := &kahunav1alpha1.KahunaCluster{}
		Expect(k8sClient.Get(ctx, nn, out)).To(Succeed())
		return out
	}

	BeforeEach(func() {
		ctx = context.Background()
		counter++
		name = fmt.Sprintf("kahuna-%d", counter)
		nn = types.NamespacedName{Name: name, Namespace: namespace}
		cluster = newCluster(3)
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		reconciler = &KahunaClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	})

	AfterEach(func() {
		obj := &kahunav1alpha1.KahunaCluster{}
		if err := k8sClient.Get(ctx, nn, obj); err == nil {
			Expect(k8sClient.Delete(ctx, obj)).To(Succeed())
		}
	})

	It("creates the owned objects with a bootstrap-safe StatefulSet", func() {
		reconcileWith(nil, fmt.Errorf("no cluster yet"))

		sts := getSTS()
		// Parallel is required: under OrderedReady, pod-0 would have to become ready before
		// pod-1 is created, but it cannot become ready until its peers exist.
		Expect(sts.Spec.PodManagementPolicy).To(Equal(appsv1.ParallelPodManagement))
		Expect(*sts.Spec.Replicas).To(Equal(int32(3)))
		Expect(sts.Spec.ServiceName).To(Equal(peerServiceName(cluster)))

		peer := &corev1.Service{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: peerServiceName(cluster), Namespace: namespace}, peer)).To(Succeed())
		Expect(peer.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone))
		// Without this, peers cannot resolve each other before they are ready, and readiness
		// requires a quorum — bootstrap would deadlock.
		Expect(peer.Spec.PublishNotReadyAddresses).To(BeTrue())

		client := &corev1.Service{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clientServiceName(cluster), Namespace: namespace}, client)).To(Succeed())
		Expect(client.Spec.PublishNotReadyAddresses).To(BeFalse())

		cm := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: configMapName(cluster), Namespace: namespace}, cm)).To(Succeed())
		Expect(cm.Data).To(HaveKey(entrypointFileName))
		Expect(cm.Data).To(HaveKey(topologyFileName))

		pdb := &policyv1.PodDisruptionBudget{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: pdbName(cluster), Namespace: namespace}, pdb)).To(Succeed())
		Expect(pdb.Spec.MaxUnavailable.IntValue()).To(Equal(1))

		Expect(getCluster().Status.BootstrapSize).To(Equal(int32(3)))
	})

	It("marks the cluster bootstrapped only once the roster carries every node as a voter", func() {
		reconcileWith(nil, fmt.Errorf("unreachable"))
		Expect(getCluster().Status.Bootstrapped).To(BeFalse())

		// A seeded roster that is still short of the full voter set is not bootstrap complete.
		reconcileWith(rosterOf(cluster, 2), nil)
		Expect(getCluster().Status.Bootstrapped).To(BeFalse())

		reconcileWith(rosterOf(cluster, 3), nil)
		out := getCluster()
		Expect(out.Status.Bootstrapped).To(BeTrue())
		Expect(out.Status.Voters).To(Equal(int32(3)))
		Expect(out.Status.Phase).To(Equal(kahunav1alpha1.PhaseRunning))
	})

	It("scales up one node at a time and only from a converged roster", func() {
		reconcileWith(rosterOf(cluster, 3), nil)

		obj := getCluster()
		obj.Spec.Replicas = ptr.To(int32(5))
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())

		// The roster still shows 3, so exactly one node may be added.
		reconcileWith(rosterOf(cluster, 3), nil)
		Expect(*getSTS().Spec.Replicas).To(Equal(int32(4)))

		// Until the new node is a committed Voter, no further step is taken.
		reconcileWith(rosterOf(cluster, 3), nil)
		Expect(*getSTS().Spec.Replicas).To(Equal(int32(4)))

		reconcileWith(rosterOf(cluster, 4), nil)
		Expect(*getSTS().Spec.Replicas).To(Equal(int32(5)))

		fake := reconcileUsing(&fakeKahuna{membership: rosterOf(cluster, 5)})
		Expect(*getSTS().Spec.Replicas).To(Equal(int32(5)))
		Expect(getCluster().Status.Phase).To(Equal(kahunav1alpha1.PhaseRunning))
		Expect(fake.leaveCalls).To(BeEmpty(), "growing the cluster asked a node to leave")
	})

	It("scales down one node at a time, waiting for the cluster to evict each one", func() {
		// Grow to 5 first: 3 cannot shrink anywhere legal (2 is rejected outright, and 1 is a
		// different quorum model the API refuses to transition into).
		reconcileWith(rosterOf(cluster, 3), nil)
		obj := getCluster()
		obj.Spec.Replicas = ptr.To(int32(5))
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())
		reconcileWith(rosterOf(cluster, 3), nil)
		reconcileWith(rosterOf(cluster, 4), nil)
		reconcileWith(rosterOf(cluster, 5), nil)
		Expect(*getSTS().Spec.Replicas).To(Equal(int32(5)))

		obj = getCluster()
		obj.Spec.Replicas = ptr.To(int32(3))
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())

		reconcileWith(rosterOf(cluster, 5), nil)
		Expect(*getSTS().Spec.Replicas).To(Equal(int32(4)))

		// The pod is gone but the roster still counts it: SWIM has not committed the eviction,
		// so removing another node now could cost quorum.
		reconcileWith(rosterOf(cluster, 5), nil)
		Expect(*getSTS().Spec.Replicas).To(Equal(int32(4)))

		reconcileWith(rosterOf(cluster, 4), nil)
		Expect(*getSTS().Spec.Replicas).To(Equal(int32(3)))
	})

	It("asks the departing node to leave before removing its pod", func() {
		reconcileWith(rosterOf(cluster, 3), nil)
		obj := getCluster()
		obj.Spec.Replicas = ptr.To(int32(5))
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())
		reconcileWith(rosterOf(cluster, 3), nil)
		reconcileWith(rosterOf(cluster, 4), nil)
		reconcileWith(rosterOf(cluster, 5), nil)

		obj = getCluster()
		obj.Spec.Replicas = ptr.To(int32(3))
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())

		fake := reconcileUsing(&fakeKahuna{membership: rosterOf(cluster, 5)})

		// The highest ordinal is the one the StatefulSet will delete, so it is the one that has to
		// be out of the roster first.
		Expect(fake.leaveCalls).To(HaveLen(1))
		Expect(fake.leaveCalls[0]).To(ContainSubstring("10.244.0.5"), "asked the wrong node to leave")
		Expect(*getSTS().Spec.Replicas).To(Equal(int32(4)),
			"the pod was not removed after the node left")
	})

	It("keeps the node when it cannot be asked to leave", func() {
		reconcileWith(rosterOf(cluster, 3), nil)
		obj := getCluster()
		obj.Spec.Replicas = ptr.To(int32(5))
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())
		reconcileWith(rosterOf(cluster, 3), nil)
		reconcileWith(rosterOf(cluster, 4), nil)
		reconcileWith(rosterOf(cluster, 5), nil)

		obj = getCluster()
		obj.Spec.Replicas = ptr.To(int32(3))
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())

		// Deleting a pod that is still a committed member is exactly what the leave call exists to
		// avoid, so an unreachable node must stall the scale rather than fall back to it.
		reconcileUsing(&fakeKahuna{
			membership: rosterOf(cluster, 5),
			leaveErr:   fmt.Errorf("connection refused"),
		})
		Expect(*getSTS().Spec.Replicas).To(Equal(int32(5)))

		// A permanent refusal holds too, and must not be retried into a loop of pod deletions.
		reconcileUsing(&fakeKahuna{
			membership: rosterOf(cluster, 5),
			leaveResult: &kahuna.LeaveResult{
				Outcome:   kahuna.OutcomeRefusedInsufficientVoters,
				Retryable: false,
				Reason:    "would leave the cluster without a voter",
			},
		})
		Expect(*getSTS().Spec.Replicas).To(Equal(int32(5)))
	})

	It("treats a node that already left as removable", func() {
		reconcileWith(rosterOf(cluster, 3), nil)
		obj := getCluster()
		obj.Spec.Replicas = ptr.To(int32(5))
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())
		reconcileWith(rosterOf(cluster, 3), nil)
		reconcileWith(rosterOf(cluster, 4), nil)
		reconcileWith(rosterOf(cluster, 5), nil)

		obj = getCluster()
		obj.Spec.Replicas = ptr.To(int32(3))
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())

		// Idempotency: a retried scale step must not stall because the node is already gone.
		reconcileUsing(&fakeKahuna{
			membership:  rosterOf(cluster, 5),
			leaveResult: &kahuna.LeaveResult{Outcome: kahuna.OutcomeNotAMember},
		})
		Expect(*getSTS().Spec.Replicas).To(Equal(int32(4)))
	})

	It("holds every node in place while the roster cannot be read", func() {
		reconcileWith(rosterOf(cluster, 3), nil)

		obj := getCluster()
		obj.Spec.Replicas = ptr.To(int32(5))
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())

		reconcileWith(nil, fmt.Errorf("all nodes unreachable"))

		sts := getSTS()
		Expect(*sts.Spec.Replicas).To(Equal(int32(3)))
		// Freezing the rolling update matters as much as freezing the size: taking another node
		// down while the cluster's state is unknown can turn degraded into dead.
		Expect(*sts.Spec.UpdateStrategy.RollingUpdate.Partition).To(Equal(int32(3)))
	})

	It("keeps the pod template stable across a scale", func() {
		reconcileWith(rosterOf(cluster, 3), nil)
		before := getSTS().Spec.Template.Annotations[configHashAnnotation]

		obj := getCluster()
		obj.Spec.Replicas = ptr.To(int32(5))
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())
		reconcileWith(rosterOf(cluster, 3), nil)

		Expect(getSTS().Spec.Template.Annotations[configHashAnnotation]).To(Equal(before),
			"scaling changed the pod template, which would restart every existing node")
	})

	It("rolls the cluster when the configuration actually changes", func() {
		reconcileWith(rosterOf(cluster, 3), nil)
		before := getSTS().Spec.Template.Annotations[configHashAnnotation]

		obj := getCluster()
		obj.Spec.ExtraArgs = []string{"--max-concurrent-sessions", "1"}
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())
		reconcileWith(rosterOf(cluster, 3), nil)

		Expect(getSTS().Spec.Template.Annotations[configHashAnnotation]).NotTo(Equal(before))
	})

	It("pins the bootstrap size so a later scale does not change who founds the cluster", func() {
		reconcileWith(rosterOf(cluster, 3), nil)

		obj := getCluster()
		obj.Spec.Replicas = ptr.To(int32(5))
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())
		reconcileWith(rosterOf(cluster, 3), nil)

		Expect(getCluster().Status.BootstrapSize).To(Equal(int32(3)))

		cm := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: configMapName(cluster), Namespace: namespace}, cm)).To(Succeed())
		Expect(cm.Data[topologyFileName]).To(ContainSubstring("BOOTSTRAP_SIZE=3"))
		Expect(cm.Data[topologyFileName]).To(ContainSubstring("REPLICAS=5"))
	})
})
