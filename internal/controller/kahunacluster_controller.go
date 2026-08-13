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
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	kahunav1alpha1 "github.com/kahunakv/k8s-operator/api/v1alpha1"
	"github.com/kahunakv/k8s-operator/internal/kahuna"
)

// requeueProgressing is how often to re-check a cluster that is mid-bootstrap, mid-scale or
// mid-upgrade. Membership changes take tens of seconds, so polling faster only burns API calls.
const requeueProgressing = 10 * time.Second

// requeueSteady is the resync interval for a converged cluster. The roster can change without
// any Kubernetes object changing — a SWIM eviction, for instance — so status would go stale
// without a periodic re-read.
const requeueSteady = 60 * time.Second

// KahunaClusterReconciler reconciles a KahunaCluster object
type KahunaClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// Kahuna reads the committed Raft roster from the cluster itself. Every membership-changing
	// step is gated on it rather than on Kubernetes state, because the roster is the
	// authoritative record of who is a member: a Running pod need not be one, and a roster entry
	// can outlive the pod that created it.
	Kahuna kahuna.Client
}

// +kubebuilder:rbac:groups=kahuna.kahunakv.io,resources=kahunaclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kahuna.kahunakv.io,resources=kahunaclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=kahuna.kahunakv.io,resources=kahunaclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=cert-manager.io,resources=certificates,verbs=get;list;watch;create;update;patch;delete

// Reconcile drives a KahunaCluster toward its desired state.
func (r *KahunaClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	cluster := &kahunav1alpha1.KahunaCluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// The founding size is pinned before anything else is created, and never revised. A node
	// decides whether to form the cluster or join it by comparing its ordinal against this
	// number, so letting it drift with spec.replicas would make founding nodes look like
	// late arrivals after the first scale.
	bootstrapped := cluster.Status.Bootstrapped
	if cluster.Status.BootstrapSize == 0 {
		cluster.Status.BootstrapSize = desiredReplicas(cluster)
		if err := r.Status().Update(ctx, cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("pinning bootstrap size: %w", err)
		}
	}
	bootstrapSize := cluster.Status.BootstrapSize

	// TLS is provisioned before anything that mounts it. A StatefulSet whose keystore Secret does
	// not exist yet schedules pods that sit stuck on the missing volume with no useful message,
	// so the keystore is made ready first and its absence is reported as a condition.
	if err := r.reconcileKeystorePassword(ctx, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling keystore password: %w", err)
	}
	if err := r.reconcileCertificate(ctx, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling certificate: %w", err)
	}

	if err := r.reconcileServices(ctx, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling services: %w", err)
	}
	if err := r.reconcileConfigMap(ctx, cluster, bootstrapSize); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling config map: %w", err)
	}
	if err := r.reconcilePDB(ctx, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling pod disruption budget: %w", err)
	}

	pods, err := r.listPods(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("listing pods: %w", err)
	}

	// The roster is read before the StatefulSet is touched, because it is what decides whether
	// the next membership step is safe to take. A failed read is not fatal: buildPlan treats an
	// unknown roster as "hold", which is the correct response to not knowing.
	membership, membershipErr := r.readMembership(ctx, cluster, pods)
	if membershipErr != nil {
		membership = nil
		log.V(1).Info("could not read cluster membership", "error", membershipErr)
	}

	sts, err := r.reconcileStatefulSet(ctx, cluster, bootstrapped, membership, pods)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling stateful set: %w", err)
	}

	result, err := r.reconcileStatus(ctx, cluster, sts, pods, membership)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
	}

	log.V(1).Info("reconciled", "phase", cluster.Status.Phase,
		"ready", cluster.Status.ReadyReplicas, "voters", cluster.Status.Voters,
		"desired", desiredReplicas(cluster))
	return result, nil
}

func (r *KahunaClusterReconciler) reconcileServices(ctx context.Context, cluster *kahunav1alpha1.KahunaCluster) error {
	for _, desired := range []*corev1.Service{desiredPeerService(cluster), desiredClientService(cluster)} {
		svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
		spec := desired.Spec
		lbls := desired.Labels
		if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
			svc.Labels = lbls
			// ClusterIP is assigned by the API server and is immutable; carrying the existing
			// value forward keeps the update from being rejected on every reconcile.
			clusterIP := svc.Spec.ClusterIP
			svc.Spec = spec
			if clusterIP != "" {
				svc.Spec.ClusterIP = clusterIP
			}
			return controllerutil.SetControllerReference(cluster, svc, r.Scheme)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (r *KahunaClusterReconciler) reconcileConfigMap(ctx context.Context, cluster *kahunav1alpha1.KahunaCluster, bootstrapSize int32) error {
	desired := desiredConfigMap(cluster, bootstrapSize)
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Labels = desired.Labels
		cm.Data = desired.Data
		return controllerutil.SetControllerReference(cluster, cm, r.Scheme)
	})
	return err
}

func (r *KahunaClusterReconciler) reconcilePDB(ctx context.Context, cluster *kahunav1alpha1.KahunaCluster) error {
	desired := desiredPDB(cluster)
	pdb := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}

	if !wantsPDB(cluster) {
		err := r.Delete(ctx, pdb)
		return client.IgnoreNotFound(err)
	}

	spec := desired.Spec
	lbls := desired.Labels
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		pdb.Labels = lbls
		pdb.Spec = spec
		return controllerutil.SetControllerReference(cluster, pdb, r.Scheme)
	})
	return err
}

// reconcileStatefulSet creates or updates the StatefulSet, applying the size and rolling-update
// partition that buildPlan considers safe right now.
func (r *KahunaClusterReconciler) reconcileStatefulSet(ctx context.Context, cluster *kahunav1alpha1.KahunaCluster, bootstrapped bool, membership *kahuna.Membership, pods []corev1.Pod) (*appsv1.StatefulSet, error) {
	// bootstrapped decides whether a scale step may be taken at all, not what the pods look like.
	existing := &appsv1.StatefulSet{}
	err := r.Get(ctx, types.NamespacedName{Name: stsName(cluster), Namespace: cluster.Namespace}, existing)
	switch {
	case apierrors.IsNotFound(err):
		// Nothing is running yet, so there is nothing a template change could disrupt, and a
		// greenfield cluster needs every node up at once to form its first quorum.
		desired := desiredStatefulSet(cluster, 0)
		if err := controllerutil.SetControllerReference(cluster, desired, r.Scheme); err != nil {
			return nil, err
		}
		if err := r.Create(ctx, desired); err != nil {
			return nil, err
		}
		return desired, nil
	case err != nil:
		return nil, err
	}

	currentReplicas := int32(0)
	if existing.Spec.Replicas != nil {
		currentReplicas = *existing.Spec.Replicas
	}
	p := buildPlan(cluster, currentReplicas, bootstrapped, membership)

	// Shrinking means removing a member, and a member leaves the roster properly by asking. Doing
	// it here rather than deleting the pod and waiting for the cluster to notice keeps a planned
	// removal from being indistinguishable from a crash — and turns the wait from the failure
	// detector's suspicion plus eviction-grace windows into one consensus round.
	if p.Replicas < currentReplicas {
		ordinal := currentReplicas - 1
		left, reason := r.decommission(ctx, cluster, pods, ordinal)
		if !left {
			// The node is still a member. Removing its pod now would drop the cluster into the
			// slow path it was just asked to avoid, so hold at the current size.
			p.Replicas = currentReplicas
			p.Partition = currentReplicas
			p.Reason = reason
		}
	}

	desired := desiredStatefulSet(cluster, p.Partition)
	desired.Spec.Replicas = &p.Replicas

	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, sts, func() error {
		sts.Labels = desired.Labels
		// Selector and volume claim templates are immutable after creation; assigning the whole
		// spec would make every reconcile a rejected update.
		sts.Spec.Replicas = desired.Spec.Replicas
		sts.Spec.Template = desired.Spec.Template
		sts.Spec.UpdateStrategy = desired.Spec.UpdateStrategy
		sts.Spec.PersistentVolumeClaimRetentionPolicy = desired.Spec.PersistentVolumeClaimRetentionPolicy
		return controllerutil.SetControllerReference(cluster, sts, r.Scheme)
	}); err != nil {
		return nil, err
	}
	return sts, nil
}

// reconcileStatus recomputes status from the StatefulSet, the pods behind it, and the committed
// Raft roster.
func (r *KahunaClusterReconciler) reconcileStatus(ctx context.Context, cluster *kahunav1alpha1.KahunaCluster, sts *appsv1.StatefulSet, pods []corev1.Pod, membership *kahuna.Membership) (ctrl.Result, error) {
	desired := desiredReplicas(cluster)

	current := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: stsName(cluster), Namespace: cluster.Namespace}, current); err == nil {
		sts = current
	}

	status := cluster.Status.DeepCopy()
	status.ObservedGeneration = cluster.Generation
	status.Replicas = sts.Status.Replicas
	status.ReadyReplicas = sts.Status.ReadyReplicas
	status.CurrentImage = cluster.Spec.Image
	sel, err := metav1.LabelSelectorAsSelector(selector(cluster))
	if err != nil {
		return ctrl.Result{}, err
	}
	status.LabelSelector = sel.String()
	if ru := sts.Spec.UpdateStrategy.RollingUpdate; ru != nil {
		status.UpdatePartition = ru.Partition
	}

	// A failed roster read is "not known right now", not a failure of the cluster: the previous
	// roster is kept so a transient network blip cannot make a healthy cluster look empty.
	rosterKnown := membership != nil
	if rosterKnown {
		status.MembershipVersion = membership.MembershipVersion
		status.Members = buildMemberStatus(cluster, membership, pods)
		status.Voters, status.Learners = countRoles(status.Members)
	}

	// Bootstrap completes when the roster itself says so. Waiting on pod readiness alone would
	// be a proxy for the thing that actually matters — a committed roster carrying every node as
	// a Voter — and the flag latches, so getting it wrong once is permanent.
	if !status.Bootstrapped && rosterKnown &&
		membership.MembershipVersion > 0 && status.Voters >= desired {
		status.Bootstrapped = true
	}

	rosterConverged := rosterKnown && status.Voters == desired && status.Learners == 0
	progressing := status.ReadyReplicas < desired || !rosterConverged

	// Degraded is reserved for a cluster that is actually in trouble — quorum lost, or its state
	// unreadable. A cluster part-way through a scale is working exactly as intended, and
	// labelling that Degraded would train operators to ignore the word.
	switch {
	case !status.Bootstrapped:
		status.Phase = kahunav1alpha1.PhaseBootstrapping
	case !hasQuorum(status.ReadyReplicas, desired) || !rosterKnown:
		status.Phase = kahunav1alpha1.PhaseDegraded
	case status.Replicas < desired || status.Voters < desired:
		status.Phase = kahunav1alpha1.PhaseScalingUp
	case status.Replicas > desired || status.Voters > desired:
		status.Phase = kahunav1alpha1.PhaseScalingDown
	case progressing:
		// Right size, right roster, but not every node is serving yet: a rolling restart.
		status.Phase = kahunav1alpha1.PhaseUpgrading
	default:
		status.Phase = kahunav1alpha1.PhaseRunning
	}

	keystoreReady, keystoreMsg := r.certificateReady(ctx, cluster)
	setCondition(status, conditionKeystoreReady, keystoreReady,
		"KeystoreAvailable", "KeystorePending", keystoreOrDefault(keystoreMsg))

	setCondition(status, kahunav1alpha1.ConditionProgressing, progressing,
		"Converging", "Converged",
		fmt.Sprintf("%d/%d nodes ready, %d voters", status.ReadyReplicas, desired, status.Voters))
	setCondition(status, kahunav1alpha1.ConditionRosterConverged, rosterConverged,
		"RosterMatchesSpec", "RosterDiffers",
		rosterMessage(rosterKnown, status.Voters, status.Learners, desired))
	// Availability is a quorum question, so it is answered from the roster's Voter count rather
	// than from how many pods happen to be Running.
	setCondition(status, kahunav1alpha1.ConditionAvailable, hasQuorum(status.ReadyReplicas, desired),
		"QuorumReady", "QuorumLost",
		fmt.Sprintf("%d of %d nodes ready", status.ReadyReplicas, desired))

	if !equalStatus(&cluster.Status, status) {
		cluster.Status = *status
		if err := r.Status().Update(ctx, cluster); err != nil {
			return ctrl.Result{}, err
		}
	}

	if progressing || status.Replicas != desired {
		return ctrl.Result{RequeueAfter: requeueProgressing}, nil
	}
	return ctrl.Result{RequeueAfter: requeueSteady}, nil
}

// keystoreOrDefault supplies a message for the ready case, where there is nothing to explain.
func keystoreOrDefault(msg string) string {
	if msg == "" {
		return "PKCS#12 keystore is present"
	}
	return msg
}

// rosterMessage explains the RosterConverged condition.
func rosterMessage(known bool, voters, learners, desired int32) string {
	if !known {
		return "cluster membership could not be read from any node"
	}
	if learners > 0 {
		return fmt.Sprintf("%d voters, %d learners still catching up (want %d voters)", voters, learners, desired)
	}
	return fmt.Sprintf("%d voters (want %d)", voters, desired)
}

// hasQuorum reports whether enough nodes are ready for the cluster to make progress. Raft needs
// a strict majority of the configured members, so 3 nodes tolerate 1 failure and 5 tolerate 2.
func hasQuorum(ready, desired int32) bool {
	if desired <= 0 {
		return false
	}
	return ready >= desired/2+1
}

// setCondition writes a condition, choosing the reason from the observed state.
func setCondition(status *kahunav1alpha1.KahunaClusterStatus, condType string, active bool, trueReason, falseReason, message string) {
	st := metav1.ConditionFalse
	reason := falseReason
	if active {
		st = metav1.ConditionTrue
		reason = trueReason
	}
	apimeta.SetStatusCondition(&status.Conditions,
		metav1.Condition{Type: condType, Status: st, Reason: reason, Message: message})
}

// equalStatus compares two status values while ignoring condition timestamps, so an unchanged
// cluster does not generate a status write — and therefore a fresh reconcile — on every pass.
func equalStatus(a, b *kahunav1alpha1.KahunaClusterStatus) bool {
	x, y := a.DeepCopy(), b.DeepCopy()
	for i := range x.Conditions {
		x.Conditions[i].LastTransitionTime = metav1.Time{}
	}
	for i := range y.Conditions {
		y.Conditions[i].LastTransitionTime = metav1.Time{}
	}
	return apiequality.Semantic.DeepEqual(x, y)
}

// listPods returns the pods belonging to this cluster.
func (r *KahunaClusterReconciler) listPods(ctx context.Context, cluster *kahunav1alpha1.KahunaCluster) ([]corev1.Pod, error) {
	var list corev1.PodList
	if err := r.List(ctx, &list,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabelsSelector{Selector: labels.SelectorFromSet(selectorLabels(cluster))},
	); err != nil {
		return nil, err
	}
	return list.Items, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *KahunaClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kahunav1alpha1.KahunaCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Named("kahunacluster").
		Complete(r)
}
