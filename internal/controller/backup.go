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
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/robfig/cron/v3"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	kahunav1alpha1 "github.com/kahunakv/k8s-operator/api/v1alpha1"
	"github.com/kahunakv/k8s-operator/internal/kahuna"
)

const (
	// backupMACKey is the key inside the generated MAC Secret.
	backupMACKey = "key"
	// backupMACKeyBytes is the size of a generated key: the full block of HMAC-SHA-256.
	backupMACKeyBytes = 32

	// backupPollInterval is how often a cluster with a backup in progress is re-checked, so that the
	// result reaches status soon after the backup ends.
	backupPollInterval = 10 * time.Second
	// defaultBackupTimeout applies when spec.backup.timeout is unset, which the CRD default prevents
	// except for objects written before the field existed.
	defaultBackupTimeout = time.Hour

	// backupRetryInitial and backupRetryMax bound the backoff after a failed backup. A failed backup
	// stays due, so without a backoff a refusal that repeats would be sent on every pass.
	backupRetryInitial = time.Minute
	backupRetryMax     = 30 * time.Minute

	// maxMissedSchedules bounds the walk from an old schedule time to the latest one that is due.
	// One run covers any number of missed ones, as with a CronJob.
	maxMissedSchedules = 100000

	// maxFailureMessage keeps one server error from filling status.
	maxFailureMessage = 1024
)

// backupKind is the kind of backup the scheduler takes.
type backupKind string

const (
	backupNone        backupKind = ""
	backupFull        backupKind = "Full"
	backupIncremental backupKind = "Incremental"
)

// backupMACKeyRef resolves where the manifest-signing key lives.
func backupMACKeyRef(cluster *kahunav1alpha1.KahunaCluster) corev1.SecretKeySelector {
	if ref := cluster.Spec.Backup.MACKeySecretRef; ref != nil {
		return *ref
	}
	return corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: backupMACSecretName(cluster)},
		Key:                  backupMACKey,
	}
}

// reconcileBackupResources creates the backup claim and the generated MAC key before any pod
// mounts them. A pod whose claim or Secret does not exist yet stays stuck with no useful message.
//
// Neither object gets an owner reference, so neither is deleted with the KahunaCluster. The backups
// are what is left when the cluster is gone, and the key is what makes them usable: the server
// refuses to restore or seed from a signed backup without it.
func (r *KahunaClusterReconciler) reconcileBackupResources(ctx context.Context, cluster *kahunav1alpha1.KahunaCluster) error {
	b := cluster.Spec.Backup
	if b == nil {
		return nil
	}

	if b.Volume.Claim != nil {
		desired := desiredBackupClaim(cluster)
		existing := &corev1.PersistentVolumeClaim{}
		err := r.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, existing)
		switch {
		case apierrors.IsNotFound(err):
			if err := r.Create(ctx, desired); err != nil && !apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("creating backup claim: %w", err)
			}
		case err != nil:
			return err
		}
	}

	if b.MACKeySecretRef == nil {
		name := backupMACSecretName(cluster)
		existing := &corev1.Secret{}
		err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, existing)
		switch {
		case apierrors.IsNotFound(err):
			// Created once and never rewritten: a new key would make every earlier backup fail its
			// authentication check.
			key := make([]byte, backupMACKeyBytes)
			if _, err := rand.Read(key); err != nil {
				return fmt.Errorf("generating backup MAC key: %w", err)
			}
			secret := &corev1.Secret{
				ObjectMeta: objectMeta(cluster, name),
				Type:       corev1.SecretTypeOpaque,
				Data:       map[string][]byte{backupMACKey: key},
			}
			if err := r.Create(ctx, secret); err != nil && !apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("creating backup MAC key: %w", err)
			}
		case err != nil:
			return err
		}
	}
	return nil
}

// backupJob is one backup running outside the reconcile loop.
type backupJob struct {
	kind      backupKind
	scheduled time.Time
	parent    string

	done bool
	info *kahuna.BackupInfo
	err  error
}

// backupRunner runs backups in the background, one per cluster at most.
//
// A full backup copies the whole store and can take many minutes. Holding a reconcile open for that
// long would stop this cluster's scaling and rollouts for the whole time, so the reconcile starts
// the backup here and collects the result on a later pass.
//
// The state is in memory only. If the operator restarts during a backup, the result is lost: the
// backup's schedule time has not advanced, so the backup is due again and runs again. A duplicate
// backup costs space until retention removes it; a missed one costs recoverability.
type backupRunner struct {
	mu   sync.Mutex
	jobs map[types.NamespacedName]*backupJob
}

// start runs fn in the background for the cluster key.
func (b *backupRunner) start(key types.NamespacedName, job *backupJob, timeout time.Duration, log logr.Logger,
	fn func(ctx context.Context) (*kahuna.BackupInfo, error)) {
	b.mu.Lock()
	if b.jobs == nil {
		b.jobs = map[types.NamespacedName]*backupJob{}
	}
	b.jobs[key] = job
	b.mu.Unlock()

	go func() {
		// The reconcile's context ends with the reconcile, so the backup gets its own.
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		info, err := fn(ctx)
		if err != nil {
			log.Error(err, "Backup failed", "kind", job.kind)
		}

		b.mu.Lock()
		defer b.mu.Unlock()
		job.info, job.err, job.done = info, err, true
	}()
}

// poll returns a copy of the cluster's job, if it has one. A finished job is removed, so its result
// is applied exactly once.
func (b *backupRunner) poll(key types.NamespacedName) (backupJob, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	job, ok := b.jobs[key]
	if !ok {
		return backupJob{}, false
	}
	if job.done {
		delete(b.jobs, key)
	}
	return *job, true
}

// forget drops the cluster's job. A backup still running finishes, and its result is discarded.
func (b *backupRunner) forget(key types.NamespacedName) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.jobs, key)
}

// backupInput is what the scheduler decides from.
type backupInput struct {
	full        cron.Schedule
	incremental cron.Schedule // nil when there is no incremental schedule
	status      kahunav1alpha1.BackupStatus
	now         time.Time
}

// backupDecision is what to do now, and when to look again.
type backupDecision struct {
	// kind is the backup to start now, or backupNone.
	kind backupKind
	// scheduled is the schedule time the backup covers. It becomes the anchor when it succeeds.
	scheduled time.Time
	// retryAt is set when a backup is due but held by the backoff after a failure.
	retryAt time.Time
	// next is the earliest time something becomes due. Zero when no schedule has a next time.
	next time.Time
}

// decideBackup chooses the backup to take now.
//
// A cluster with no successful full backup takes one at once, whatever the schedule: until it has
// one, it has nothing to restore. After that, a full backup is due at the first schedule time after
// the last one that succeeded, and an incremental likewise on its own schedule. A full backup wins
// when both are due, because it starts a new chain that the incremental would only add to.
func decideBackup(in backupInput) backupDecision {
	st := in.status
	var d backupDecision

	if st.LastFullScheduleTime == nil {
		d.kind, d.scheduled = backupFull, in.now
	} else {
		fullDue, fullNext := latestDue(in.full, st.LastFullScheduleTime.Time, in.now)
		d.next = fullNext
		if !fullDue.IsZero() {
			d.kind, d.scheduled = backupFull, fullDue
		} else if in.incremental != nil && st.LastBackupID != "" {
			base := st.LastFullScheduleTime.Time
			if t := st.LastIncrementalScheduleTime; t != nil && t.After(base) {
				base = t.Time
			}
			incDue, incNext := latestDue(in.incremental, base, in.now)
			d.next = earliest(d.next, incNext)
			if !incDue.IsZero() {
				d.kind, d.scheduled = backupIncremental, incDue
			}
		}
	}

	if d.kind != backupNone && st.ConsecutiveFailures > 0 && st.LastFailureTime != nil {
		if retryAt := st.LastFailureTime.Add(backupRetryDelay(st.ConsecutiveFailures)); retryAt.After(in.now) {
			d.kind, d.scheduled = backupNone, time.Time{}
			d.retryAt, d.next = retryAt, retryAt
		}
	}
	return d
}

// latestDue walks the schedule from base and returns the latest schedule time that is not in the
// future (zero when none is), and the first one that is.
func latestDue(s cron.Schedule, base, now time.Time) (due, next time.Time) {
	t := s.Next(base)
	// A schedule with no match (30 February) has no next time at all.
	if t.IsZero() || t.After(now) {
		return time.Time{}, t
	}
	for range maxMissedSchedules {
		n := s.Next(t)
		if n.IsZero() || n.After(now) {
			return t, n
		}
		t = n
	}
	// Still behind after the bound. The next pass continues the walk from here.
	return t, time.Time{}
}

// earliest returns the earlier of two times, where zero means "no time".
func earliest(a, b time.Time) time.Time {
	switch {
	case a.IsZero():
		return b
	case b.IsZero():
		return a
	case b.Before(a):
		return b
	default:
		return a
	}
}

// backupRetryDelay doubles from backupRetryInitial with each consecutive failure, up to
// backupRetryMax.
func backupRetryDelay(failures int32) time.Duration {
	d := backupRetryInitial
	for i := int32(1); i < failures && d < backupRetryMax; i++ {
		d *= 2
	}
	return min(d, backupRetryMax)
}

// parseSchedule parses a cron schedule, with descriptors and CRON_TZ= allowed. Times are computed
// in UTC unless the schedule names a zone.
func parseSchedule(s string) (cron.Schedule, error) {
	return cron.ParseStandard(s)
}

// backupBlocked returns why a backup must not start now, or "" when it may.
//
// A backup needs every node to run the backup flags, because the coordinated backup goes to the
// meta-partition leader, which can be any node. It also must not race a membership change: a
// backup taken while the roster moves can fail on a leadership change or capture a topology the
// next incremental refuses.
func backupBlocked(cluster *kahunav1alpha1.KahunaCluster, membership *kahuna.Membership, pods []corev1.Pod) string {
	if !cluster.Status.Bootstrapped {
		return "the cluster has not finished bootstrap"
	}
	desired := desiredReplicas(cluster)
	if !rosterMatches(cluster, membership, desired) {
		return "the roster has not converged on the desired nodes"
	}
	hash := configHash(entrypointScript(cluster))
	seen := int32(0)
	for i := range pods {
		pod := &pods[i]
		if o := ordinalOf(pod.Name); o < 0 || o >= desired {
			continue
		}
		seen++
		switch {
		case pod.DeletionTimestamp != nil || !podIsReady(pod):
			return fmt.Sprintf("%s is not ready", pod.Name)
		case pod.Annotations[configHashAnnotation] != hash:
			return fmt.Sprintf("%s does not run the current configuration yet", pod.Name)
		}
	}
	if seen < desired {
		return fmt.Sprintf("%d of %d pods exist", seen, desired)
	}
	return ""
}

// backupTarget is one node a backup request can go to.
type backupTarget struct {
	pod string
	url string
}

// runBackup sends the backup to each node in turn until one takes it.
//
// Only the meta-partition leader takes a coordinated backup, and the operator has no cheap way to
// ask which node that is: the leader query is behind the Raft node authentication. Every other node
// refuses at once with NotBackupCoordinator, so asking each node in turn costs one fast refusal per
// node that is not the leader.
func runBackup(ctx context.Context, c kahuna.Client, kind backupKind, parent string, targets []backupTarget) (*kahuna.BackupInfo, error) {
	var refusals []string
	for _, t := range targets {
		var info *kahuna.BackupInfo
		var err error
		if kind == backupFull {
			info, err = c.TakeCoordinatedBackup(ctx, t.url)
		} else {
			info, err = c.TakeIncrementalBackup(ctx, t.url, parent)
		}
		if err == nil {
			return info, nil
		}
		if ctx.Err() != nil {
			return nil, fmt.Errorf("%s did not finish the backup before the timeout: %w", t.pod, err)
		}
		var be *kahuna.BackupError
		if errors.As(err, &be) && !be.TryAnotherNode() {
			return nil, fmt.Errorf("%s: %w", t.pod, err)
		}
		// A refusal another node may not repeat, or a node that could not be reached.
		refusals = append(refusals, fmt.Sprintf("%s: %v", t.pod, err))
	}
	if len(refusals) == 0 {
		return nil, errors.New("no ready node to send the backup to")
	}
	return nil, fmt.Errorf("no node took the backup: %s", strings.Join(refusals, "; "))
}

// applyBackupResult records a finished backup in status.
func applyBackupResult(st *kahunav1alpha1.BackupStatus, job backupJob, now time.Time, log logr.Logger) {
	at := metav1.NewTime(now)
	if job.err == nil {
		typ := job.info.ActualKind
		if typ == "" {
			typ = job.info.Type
		}
		st.LastSuccessfulTime = &at
		st.LastBackupID = job.info.BackupID
		st.LastBackupType = typ
		st.ConsecutiveFailures = 0
		st.LastFailureTime = nil
		st.LastFailureMessage = ""
		scheduled := metav1.NewTime(job.scheduled)
		if job.kind == backupFull {
			st.LastFullScheduleTime = &scheduled
			// An incremental due right after a full one would hold almost nothing, so the
			// incremental schedule restarts from the full backup.
			st.LastIncrementalScheduleTime = &at
		} else {
			st.LastIncrementalScheduleTime = &scheduled
		}
		log.Info("Took backup", "kind", job.kind, "type", typ, "backupID", job.info.BackupID,
			"substitutionReason", job.info.SubstitutionReason)
		return
	}

	var be *kahuna.BackupError
	if job.kind == backupIncremental && errors.As(job.err, &be) && be.Outcome == kahuna.BackupOutcomeParentMissing {
		// The parent is gone from the catalog: retention removed it, or someone deleted it. No
		// incremental can continue that chain, so a new one starts with a full backup now. This is
		// not counted as a failure, because the full backup is the correct next step.
		st.LastBackupID = ""
		st.LastFullScheduleTime = nil
		log.Info("Parent backup is gone from the catalog, starting a new chain", "parentBackupID", job.parent)
		return
	}

	msg := job.err.Error()
	if len(msg) > maxFailureMessage {
		msg = msg[:maxFailureMessage]
	}
	st.ConsecutiveFailures++
	st.LastFailureTime = &at
	st.LastFailureMessage = fmt.Sprintf("%s backup: %s", job.kind, msg)
}

// reconcileBackup collects a finished backup, and starts the next one when it is due. It edits
// cluster.Status, which the caller persists. It returns how soon the cluster should be looked at
// again for backups, or zero when the normal resync is soon enough.
func (r *KahunaClusterReconciler) reconcileBackup(ctx context.Context, cluster *kahunav1alpha1.KahunaCluster, membership *kahuna.Membership, pods []corev1.Pod) time.Duration {
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}
	spec := cluster.Spec.Backup
	if spec == nil {
		cluster.Status.Backup = nil
		apimeta.RemoveStatusCondition(&cluster.Status.Conditions, kahunav1alpha1.ConditionBackupFailing)
		r.backups.forget(key)
		return 0
	}
	log := logf.FromContext(ctx).WithValues("backupDir", backupPath(cluster))
	now := time.Now()

	st := &kahunav1alpha1.BackupStatus{}
	if cluster.Status.Backup != nil {
		st = cluster.Status.Backup.DeepCopy()
	}
	defer func() { cluster.Status.Backup = st }()

	job, hasJob := r.backups.poll(key)
	if hasJob && job.done {
		applyBackupResult(st, job, now, log)
		hasJob = false
	}

	full, err := parseSchedule(spec.Schedule)
	if err != nil {
		setBackupCondition(cluster, true, "InvalidSchedule", fmt.Sprintf("backup.schedule %q: %v", spec.Schedule, err))
		return 0
	}
	var incremental cron.Schedule
	if spec.IncrementalSchedule != "" {
		if incremental, err = parseSchedule(spec.IncrementalSchedule); err != nil {
			setBackupCondition(cluster, true, "InvalidSchedule",
				fmt.Sprintf("backup.incrementalSchedule %q: %v", spec.IncrementalSchedule, err))
			return 0
		}
	}

	failing := st.ConsecutiveFailures > 0
	failure := ""
	if failing {
		failure = fmt.Sprintf("%d consecutive failures, latest: %s. ", st.ConsecutiveFailures, st.LastFailureMessage)
	}
	failedOr := func(reason string) string {
		if failing {
			return "BackupFailed"
		}
		return reason
	}

	if hasJob {
		setBackupCondition(cluster, failing, failedOr("BackupRunning"),
			fmt.Sprintf("%s%s backup scheduled for %s is running", failure, job.kind, job.scheduled.UTC().Format(time.RFC3339)))
		return backupPollInterval
	}

	d := decideBackup(backupInput{full: full, incremental: incremental, status: *st, now: now})
	if d.kind == backupNone {
		msg := "no backup is scheduled"
		switch {
		case !d.retryAt.IsZero():
			msg = "retrying at " + d.retryAt.UTC().Format(time.RFC3339)
		case !d.next.IsZero():
			msg = "next backup at " + d.next.UTC().Format(time.RFC3339)
		}
		setBackupCondition(cluster, failing, failedOr("BackupScheduled"), failure+msg)
		return untilDue(d.next, now)
	}

	if spec.Suspend != nil && *spec.Suspend {
		setBackupCondition(cluster, failing, failedOr("BackupSuspended"), failure+"backups are suspended")
		return 0
	}
	if reason := backupBlocked(cluster, membership, pods); reason != "" {
		setBackupCondition(cluster, failing, failedOr("BackupWaiting"),
			fmt.Sprintf("%s%s backup is due, waiting because %s", failure, d.kind, reason))
		return 0
	}

	desired := desiredReplicas(cluster)
	targets := make([]backupTarget, 0, len(pods))
	for i := range pods {
		if o := ordinalOf(pods[i].Name); o >= 0 && o < desired && pods[i].Status.PodIP != "" {
			targets = append(targets, backupTarget{pod: pods[i].Name, url: podBaseURL(cluster, &pods[i])})
		}
	}

	timeout := defaultBackupTimeout
	if spec.Timeout != nil && spec.Timeout.Duration > 0 {
		timeout = spec.Timeout.Duration
	}
	next := &backupJob{kind: d.kind, scheduled: d.scheduled, parent: st.LastBackupID}
	parent, kind, client := next.parent, next.kind, r.Kahuna
	log.Info("Starting backup", "kind", kind, "scheduled", d.scheduled, "parentBackupID", parent)
	r.backups.start(key, next, timeout, log, func(ctx context.Context) (*kahuna.BackupInfo, error) {
		return runBackup(ctx, client, kind, parent, targets)
	})

	setBackupCondition(cluster, failing, failedOr("BackupRunning"),
		fmt.Sprintf("%s%s backup scheduled for %s is running", failure, d.kind, d.scheduled.UTC().Format(time.RFC3339)))
	return backupPollInterval
}

// untilDue converts the next due time into a requeue delay. Zero means none is needed.
func untilDue(next, now time.Time) time.Duration {
	if next.IsZero() {
		return 0
	}
	return max(next.Sub(now), time.Second)
}

// setBackupCondition writes the BackupFailing condition.
func setBackupCondition(cluster *kahunav1alpha1.KahunaCluster, failing bool, reason, message string) {
	st := metav1.ConditionFalse
	if failing {
		st = metav1.ConditionTrue
	}
	apimeta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type: kahunav1alpha1.ConditionBackupFailing, Status: st, Reason: reason, Message: message,
	})
}
