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
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	// placement is the replica placement reported; nil means every data partition has a replica
	// set, which is what a cluster created with more nodes than its factor looks like.
	placement *kahuna.Placement
	// metrics returns the dashboard metrics for a base URL; nil means the endpoint is absent.
	metrics func(baseURL string) *kahuna.Metrics
}

func (f *fakeKahuna) Placement(context.Context, string) (*kahuna.Placement, error) {
	if f.placement != nil {
		return f.placement, nil
	}
	return &kahuna.Placement{ReplicationFactor: 3, Initialized: true, Partitions: []kahuna.PartitionPlacement{
		{PartitionID: 1, State: kahuna.PartitionStateActive, Replicas: []kahuna.ReplicaPlacement{{Endpoint: "a"}, {Endpoint: "b"}, {Endpoint: "c"}}},
	}}, nil
}

func (f *fakeKahuna) Metrics(_ context.Context, baseURL string) (*kahuna.Metrics, error) {
	if f.metrics == nil {
		return nil, fmt.Errorf("metrics query returned HTTP 404")
	}
	return f.metrics(baseURL), nil
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

// staleCache stands in for an informer cache that has not yet seen the controller's own last
// status write: every Get of the KahunaCluster returns the snapshot it was given, while every
// other object is read live.
type staleCache struct {
	client.Client
	snapshot *kahunav1alpha1.KahunaCluster
}

func (c *staleCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if out, ok := obj.(*kahunav1alpha1.KahunaCluster); ok && key.Name == c.snapshot.Name {
		c.snapshot.DeepCopyInto(out)
		return nil
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

// rosterOf builds a roster of Voters for ordinals 0..n-1 of the given cluster.
func rosterOf(cluster *kahunav1alpha1.KahunaCluster, n int32) *kahuna.Membership {
	m := &kahuna.Membership{MembershipVersion: int64(n), Initialized: true, LocalRole: kahuna.RoleVoter}
	for i := range n {
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
					SecretRef: &corev1.LocalObjectReference{Name: testTLSSecret},
				},
			},
		}
	}

	// ensurePods stands in for the StatefulSet controller, which envtest does not run. The
	// reconciler reaches nodes by pod IP, so a roster is only observable when pods carrying one
	// exist.
	ensurePods := func(n int32) {
		for i := range n {
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
		ready := min(rosterSize, want)
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
		obj.Spec.ExtraArgs = []string{flagMaxSessions, "1"}
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())
		reconcileWith(rosterOf(cluster, 3), nil)

		Expect(getSTS().Spec.Template.Annotations[configHashAnnotation]).NotTo(Equal(before))
	})

	Context("with autoscaling", func() {
		// steadyLoad returns a metrics source where every node admits writesPerNode writes per
		// second. Each call advances the node's clock by 15 s, past the sampler's minimum interval.
		steadyLoad := func(writesPerNode float64) func(string) *kahuna.Metrics {
			clocks := map[string]int64{}
			return func(baseURL string) *kahuna.Metrics {
				clocks[baseURL] += 15_000
				total := writesPerNode * float64(clocks[baseURL]) / 1000
				return &kahuna.Metrics{MonotonicMs: clocks[baseURL], Rows: []kahuna.MetricRow{{
					Metric: metricWritesAdmitted, Kind: kahuna.MetricKindCounter, Total: ptr.To(total),
				}}}
			}
		}

		// autoscale turns the test's cluster into an autoscaled one founded at 4 nodes, which is the
		// smallest size at which a replication factor of 3 produces real placement.
		autoscale := func() {
			obj := getCluster()
			obj.Spec.Replicas = ptr.To(int32(4))
			obj.Spec.Replication = &kahunav1alpha1.ReplicationSpec{Factor: 3}
			obj.Spec.Autoscaling = &kahunav1alpha1.AutoscalingSpec{
				MinReplicas:                   3,
				MaxReplicas:                   6,
				TargetWritesPerSecond:         ptr.To(int32(1000)),
				ScaleUpStabilizationSeconds:   ptr.To(int32(0)),
				ScaleDownStabilizationSeconds: ptr.To(int32(0)),
			}
			Expect(k8sClient.Update(ctx, obj)).To(Succeed())
		}

		It("adds a node under load, one at a time", func() {
			autoscale()
			load := steadyLoad(5000)

			reconcileUsing(&fakeKahuna{membership: rosterOf(cluster, 4), metrics: load})
			// The first converged pass has no rate yet: one reading is not a rate.
			reconcileUsing(&fakeKahuna{membership: rosterOf(cluster, 4), metrics: load})
			Expect(*getSTS().Spec.Replicas).To(Equal(int32(4)))

			reconcileUsing(&fakeKahuna{membership: rosterOf(cluster, 4), metrics: load})
			out := getCluster()
			Expect(out.Status.Autoscaling.DesiredReplicas).To(Equal(int32(5)))
			Expect(out.Status.Autoscaling.RecommendedReplicas).To(Equal(int32(6)))
			Expect(*out.Status.Autoscaling.WritesPerSecondPerNode).To(Equal(int64(5000)))
			Expect(*getSTS().Spec.Replicas).To(Equal(int32(5)))

			// The new node has not joined: the autoscaler must not stack a second step on it.
			reconcileUsing(&fakeKahuna{membership: rosterOf(cluster, 4), metrics: load})
			Expect(getCluster().Status.Autoscaling.DesiredReplicas).To(Equal(int32(5)))
			Expect(*getSTS().Spec.Replicas).To(Equal(int32(5)))

			// spec.replicas is only the starting size; the autoscaler owns the count now.
			Expect(*getCluster().Spec.Replicas).To(Equal(int32(4)))
		})

		It("removes a node when idle, through the leave call, down to the minimum", func() {
			autoscale()
			idle := steadyLoad(0)

			reconcileUsing(&fakeKahuna{membership: rosterOf(cluster, 4), metrics: idle})
			reconcileUsing(&fakeKahuna{membership: rosterOf(cluster, 4), metrics: idle})
			fake := reconcileUsing(&fakeKahuna{membership: rosterOf(cluster, 4), metrics: idle})

			Expect(getCluster().Status.Autoscaling.DesiredReplicas).To(Equal(int32(3)))
			Expect(fake.leaveCalls).To(HaveLen(1))
			Expect(fake.leaveCalls[0]).To(ContainSubstring("10.244.0.4"))
			Expect(*getSTS().Spec.Replicas).To(Equal(int32(3)))

			reconcileUsing(&fakeKahuna{membership: rosterOf(cluster, 3), metrics: idle})
			reconcileUsing(&fakeKahuna{membership: rosterOf(cluster, 3), metrics: idle})
			Expect(getCluster().Status.Autoscaling.DesiredReplicas).To(Equal(int32(3)),
				"scaled below minReplicas")
		})

		It("does not regrow the cluster from a cached status the previous pass already moved", func() {
			autoscale()
			idle := steadyLoad(0)

			reconcileUsing(&fakeKahuna{membership: rosterOf(cluster, 4), metrics: idle})
			reconcileUsing(&fakeKahuna{membership: rosterOf(cluster, 4), metrics: idle})
			before := getCluster()
			Expect(before.Status.Autoscaling.DesiredReplicas).To(Equal(int32(4)))

			// This pass decides 4 -> 3, asks kahuna-3 to leave and shrinks the StatefulSet.
			reconcileUsing(&fakeKahuna{membership: rosterOf(cluster, 4), metrics: idle})
			Expect(getCluster().Status.Autoscaling.DesiredReplicas).To(Equal(int32(3)))
			Expect(*getSTS().Spec.Replicas).To(Equal(int32(3)))

			// The next pass runs before the cache has caught up with that status write, while the
			// roster has already converged at 3. Through the cache the cluster still wants 4
			// nodes, and growing to it would recreate the node that was just decommissioned.
			reconciler = &KahunaClusterReconciler{
				Client: &staleCache{Client: k8sClient, snapshot: before},
				Reader: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			// Run the pass by hand: the StatefulSet is checked before the pass's own result,
			// because the regrowth is a write that lands before the status update fails.
			ensurePods(3)
			syncSTSStatus(3)
			fake := &fakeKahuna{membership: rosterOf(cluster, 3), metrics: idle}
			reconciler.Kahuna = fake
			_, rerr := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(*getSTS().Spec.Replicas).To(Equal(int32(3)), "a stale cache regrew the StatefulSet")
			Expect(rerr).NotTo(HaveOccurred())
			Expect(fake.leaveCalls).To(BeEmpty(), "a converged cluster asked a node to leave")
			Expect(getCluster().Status.Autoscaling.DesiredReplicas).To(Equal(int32(3)))
		})

		It("does not grow a fully replicated cluster past the replication cap", func() {
			obj := getCluster()
			obj.Spec.Replication = &kahunav1alpha1.ReplicationSpec{Factor: 3}
			obj.Spec.Autoscaling = &kahunav1alpha1.AutoscalingSpec{
				MinReplicas: 3, MaxReplicas: 6,
				TargetWritesPerSecond:       ptr.To(int32(1000)),
				ScaleUpStabilizationSeconds: ptr.To(int32(0)),
			}
			Expect(k8sClient.Update(ctx, obj)).To(Succeed())

			// Founded at 3 nodes with factor 3: the server left every data partition without a
			// replica set, so each node added would be one more copy of everything.
			full := &kahuna.Placement{ReplicationFactor: 3, Initialized: true, Partitions: []kahuna.PartitionPlacement{
				{PartitionID: 1, State: kahuna.PartitionStateActive}, {PartitionID: 2, State: kahuna.PartitionStateActive},
			}}
			load := steadyLoad(5000)
			for range 4 {
				reconcileUsing(&fakeKahuna{membership: rosterOf(cluster, 3), metrics: load, placement: full})
			}

			out := getCluster()
			Expect(out.Status.Autoscaling.DesiredReplicas).To(Equal(int32(3)))
			Expect(*getSTS().Spec.Replicas).To(Equal(int32(3)))
			limited := apimeta.FindStatusCondition(out.Status.Conditions, kahunav1alpha1.ConditionAutoscalingLimited)
			Expect(limited).NotTo(BeNil())
			Expect(limited.Status).To(Equal(metav1.ConditionTrue))
			Expect(limited.Message).To(ContainSubstring("fully replicated"))
		})

		It("holds and says why when the metrics endpoint is missing", func() {
			autoscale()
			for range 3 {
				reconcileUsing(&fakeKahuna{membership: rosterOf(cluster, 4)})
			}
			out := getCluster()
			Expect(out.Status.Autoscaling.DesiredReplicas).To(Equal(int32(4)))
			limited := apimeta.FindStatusCondition(out.Status.Conditions, kahunav1alpha1.ConditionAutoscalingLimited)
			Expect(limited).NotTo(BeNil())
			Expect(limited.Message).To(ContainSubstring("traffic metrics could not be read"))
		})

		It("rejects autoscaling without a replication factor", func() {
			obj := getCluster()
			obj.Spec.Autoscaling = &kahunav1alpha1.AutoscalingSpec{
				MinReplicas: 3, MaxReplicas: 6, TargetWritesPerSecond: ptr.To(int32(1000)),
			}
			err := k8sClient.Update(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("autoscaling requires spec.replication"))
		})

		It("rejects a factor above 3 and a minimum below 3", func() {
			obj := getCluster()
			obj.Spec.Replication = &kahunav1alpha1.ReplicationSpec{Factor: 5}
			Expect(k8sClient.Update(ctx, obj)).NotTo(Succeed())

			obj = getCluster()
			obj.Spec.Replication = &kahunav1alpha1.ReplicationSpec{Factor: 3}
			obj.Spec.Autoscaling = &kahunav1alpha1.AutoscalingSpec{
				MinReplicas: 1, MaxReplicas: 6, TargetWritesPerSecond: ptr.To(int32(1000)),
			}
			Expect(k8sClient.Update(ctx, obj)).NotTo(Succeed())
		})
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
