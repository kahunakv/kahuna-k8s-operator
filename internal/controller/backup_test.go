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
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/robfig/cron/v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	kahunav1alpha1 "github.com/kahunakv/k8s-operator/api/v1alpha1"
	"github.com/kahunakv/k8s-operator/internal/kahuna"
)

// testBackupSchedule is a full-backup schedule far enough apart that one test never sees two runs.
const testBackupSchedule = "@daily"

// Images for the tests: a release before composed backups, and two tags without a release version.
const (
	testOldImage    = "kahunakv/kahuna:1.12.4"
	testDevImage    = "kahunakv/kahuna:dev"
	testLatestImage = "kahunakv/kahuna:latest"
)

func mustSchedule(t *testing.T, s string) cron.Schedule {
	t.Helper()
	sched, err := parseSchedule(s)
	if err != nil {
		t.Fatalf("parsing %q: %v", s, err)
	}
	return sched
}

func at(d time.Duration) *metav1.Time { return ptr.To(metav1.NewTime(t0.Add(d))) }

// A cluster with no full backup has nothing to restore, so it takes one at once rather than at the
// next schedule time, which can be a day away.
func TestFirstFullBackupRunsAtOnce(t *testing.T) {
	d := decideBackup(backupInput{full: mustSchedule(t, "0 2 * * *"), now: t0})
	if d.kind != backupFull || !d.scheduled.Equal(t0) {
		t.Fatalf("got %+v, want a full backup scheduled now", d)
	}
}

func TestFullBackupIsDueAtTheNextScheduleTime(t *testing.T) {
	// t0 is 12:00; the last full backup covered 02:00 today.
	st := kahunav1alpha1.BackupStatus{LastFullScheduleTime: at(-10 * time.Hour), LastBackupID: "b1"}
	full := mustSchedule(t, "0 2 * * *")

	d := decideBackup(backupInput{full: full, status: st, now: t0})
	if d.kind != backupNone {
		t.Fatalf("got %s before the next schedule time", d.kind)
	}
	if want := t0.Add(14 * time.Hour); !d.next.Equal(want) {
		t.Fatalf("next = %v, want %v", d.next, want)
	}

	d = decideBackup(backupInput{full: full, status: st, now: t0.Add(14 * time.Hour)})
	if d.kind != backupFull || !d.scheduled.Equal(t0.Add(14*time.Hour)) {
		t.Fatalf("got %+v, want the 02:00 full backup", d)
	}
}

// Like a CronJob, missed runs collapse into one, and that run covers the latest schedule time, so
// the cadence continues from now rather than replaying every missed time.
func TestMissedFullBackupsCollapseIntoOne(t *testing.T) {
	st := kahunav1alpha1.BackupStatus{LastFullScheduleTime: at(-72 * time.Hour), LastBackupID: "b1"}
	d := decideBackup(backupInput{full: mustSchedule(t, "@hourly"), status: st, now: t0.Add(30 * time.Minute)})
	if d.kind != backupFull || !d.scheduled.Equal(t0) {
		t.Fatalf("got %+v, want one full backup for %v", d, t0)
	}
}

func TestIncrementalBackupFollowsItsOwnSchedule(t *testing.T) {
	st := kahunav1alpha1.BackupStatus{
		LastFullScheduleTime:        at(-10 * time.Hour),
		LastIncrementalScheduleTime: at(0),
		LastBackupID:                "b1",
	}
	in := backupInput{full: mustSchedule(t, "0 2 * * *"), incremental: mustSchedule(t, "*/15 * * * *"), status: st}

	in.now = t0.Add(5 * time.Minute)
	if d := decideBackup(in); d.kind != backupNone || !d.next.Equal(t0.Add(15*time.Minute)) {
		t.Fatalf("got %+v, want nothing until 12:15", d)
	}
	in.now = t0.Add(16 * time.Minute)
	if d := decideBackup(in); d.kind != backupIncremental || !d.scheduled.Equal(t0.Add(15*time.Minute)) {
		t.Fatalf("got %+v, want the 12:15 incremental", d)
	}
}

// An incremental needs a parent. Without one, only the full schedule applies.
func TestIncrementalBackupNeedsAParent(t *testing.T) {
	st := kahunav1alpha1.BackupStatus{LastFullScheduleTime: at(-10 * time.Hour)}
	d := decideBackup(backupInput{
		full: mustSchedule(t, "0 2 * * *"), incremental: mustSchedule(t, "*/15 * * * *"), status: st, now: t0,
	})
	if d.kind != backupNone {
		t.Fatalf("got %s with no parent backup", d.kind)
	}
}

// A full backup starts a new chain, so it wins when both are due.
func TestFullBackupWinsWhenBothAreDue(t *testing.T) {
	st := kahunav1alpha1.BackupStatus{
		LastFullScheduleTime:        at(-24 * time.Hour),
		LastIncrementalScheduleTime: at(-time.Hour),
		LastBackupID:                "b1",
	}
	d := decideBackup(backupInput{
		full: mustSchedule(t, "0 12 * * *"), incremental: mustSchedule(t, "@hourly"), status: st, now: t0,
	})
	if d.kind != backupFull {
		t.Fatalf("got %s, want the full backup", d.kind)
	}
}

// A failed backup stays due. Without a backoff, a refusal that repeats would be sent on every pass.
func TestFailedBackupIsRetriedAfterABackoff(t *testing.T) {
	st := kahunav1alpha1.BackupStatus{ConsecutiveFailures: 3, LastFailureTime: at(0)}
	full := mustSchedule(t, "0 2 * * *")

	d := decideBackup(backupInput{full: full, status: st, now: t0.Add(time.Minute)})
	if d.kind != backupNone || !d.retryAt.Equal(t0.Add(4*time.Minute)) {
		t.Fatalf("got %+v, want a retry at +4m after three failures", d)
	}
	if d := decideBackup(backupInput{full: full, status: st, now: t0.Add(4 * time.Minute)}); d.kind != backupFull {
		t.Fatalf("got %+v, want the retry once the backoff has passed", d)
	}
}

func TestBackupRetryDelayIsCapped(t *testing.T) {
	for failures, want := range map[int32]time.Duration{1: time.Minute, 2: 2 * time.Minute, 6: 30 * time.Minute, 40: 30 * time.Minute} {
		if got := backupRetryDelay(failures); got != want {
			t.Errorf("backupRetryDelay(%d) = %v, want %v", failures, got, want)
		}
	}
}

// A schedule that never matches must not make a backup look due.
func TestScheduleWithNoMatchIsNeverDue(t *testing.T) {
	st := kahunav1alpha1.BackupStatus{LastFullScheduleTime: at(-time.Hour), LastBackupID: "b1"}
	if d := decideBackup(backupInput{full: mustSchedule(t, "0 0 30 2 *"), status: st, now: t0}); d.kind != backupNone {
		t.Fatalf("got %+v from a schedule with no match", d)
	}
}

// scriptedBackups answers each node from a script, and records the order the nodes were asked in.
type scriptedBackups struct {
	fakeKahuna
	answers map[string]error
	asked   []string
}

func (s *scriptedBackups) TakeCoordinatedBackup(_ context.Context, url string) (*kahuna.BackupInfo, error) {
	s.asked = append(s.asked, url)
	if err := s.answers[url]; err != nil {
		return nil, err
	}
	return &kahuna.BackupInfo{BackupID: "b-" + url, Type: kahuna.BackupTypeFull}, nil
}

func targets(urls ...string) []backupTarget {
	out := make([]backupTarget, 0, len(urls))
	for _, u := range urls {
		out = append(out, backupTarget{pod: u, url: u})
	}
	return out
}

// Only the meta-partition leader takes a coordinated backup, and the others refuse at once.
func TestCoordinatedBackupFindsTheLeader(t *testing.T) {
	notLeader := &kahuna.BackupError{StatusCode: 503, Outcome: kahuna.BackupOutcomeNotBackupCoordinator}
	c := &scriptedBackups{answers: map[string]error{"a": notLeader, "b": errors.New("connection refused")}}

	info, err := runBackup(context.Background(), c, backupFull, "", targets("a", "b", "c"))
	if err != nil {
		t.Fatalf("runBackup: %v", err)
	}
	if info.BackupID != "b-c" || !slices.Equal(c.asked, []string{"a", "b", "c"}) {
		t.Fatalf("got %s after asking %v, want the backup from c", info.BackupID, c.asked)
	}
}

// A refusal that every node would repeat ends the attempt at once.
func TestBackupStopsOnARefusalEveryNodeWouldRepeat(t *testing.T) {
	coverage := &kahuna.BackupError{StatusCode: 422, Outcome: "RestrictedCoverage", Detail: "partition 4 missing"}
	c := &scriptedBackups{answers: map[string]error{"a": coverage}}

	_, err := runBackup(context.Background(), c, backupFull, "", targets("a", "b"))
	if err == nil || !strings.Contains(err.Error(), "RestrictedCoverage") || len(c.asked) != 1 {
		t.Fatalf("got %v after asking %v, want the refusal from a only", err, c.asked)
	}
}

func TestBackupReportsEveryNodeWhenNoneTakesIt(t *testing.T) {
	notLeader := &kahuna.BackupError{StatusCode: 503, Outcome: kahuna.BackupOutcomeNotBackupCoordinator}
	c := &scriptedBackups{answers: map[string]error{"a": notLeader, "b": notLeader}}

	_, err := runBackup(context.Background(), c, backupFull, "", targets("a", "b"))
	if err == nil || !strings.Contains(err.Error(), "a: ") || !strings.Contains(err.Error(), "b: ") {
		t.Fatalf("got %v, want both refusals named", err)
	}
}

// Right after bootstrap no node leads the meta partition: every node refuses, or the one that just
// took the lead cannot verify its first cut. That is reported as unclaimed, not as a plain failure.
func TestBackupRefusedOnlyForWantOfALeaderIsUnclaimed(t *testing.T) {
	notLeader := &kahuna.BackupError{StatusCode: 503, Outcome: kahuna.BackupOutcomeNotBackupCoordinator}
	cut := &kahuna.BackupError{StatusCode: 503, Outcome: kahuna.BackupOutcomeCutUnverified}
	c := &scriptedBackups{answers: map[string]error{"a": notLeader, "b": cut, "c": notLeader}}

	_, err := runBackup(context.Background(), c, backupFull, "", targets("a", "b", "c"))
	var unclaimed *unclaimedBackupError
	if !errors.As(err, &unclaimed) || !strings.Contains(err.Error(), "b: ") {
		t.Fatalf("got %v, want an unclaimed backup that names every refusal", err)
	}
}

// Under a replication factor the coordinator refuses when one node's share of the partitions does
// not arrive, and the other nodes refuse because they do not coordinate. The next attempt gives the
// missing share to another replica, so this clears on retry like a leader election.
func TestBackupWithAMissingShareIsUnclaimed(t *testing.T) {
	notLeader := &kahuna.BackupError{StatusCode: 503, Outcome: kahuna.BackupOutcomeNotBackupCoordinator}
	share := &kahuna.BackupError{StatusCode: 503, Outcome: kahuna.BackupOutcomePartitionCaptureFailed,
		Detail: "partition(s) 4, 5 on node kahuna-3: the node could not be reached"}
	c := &scriptedBackups{answers: map[string]error{"a": notLeader, "b": share, "c": notLeader}}

	_, err := runBackup(context.Background(), c, backupFull, "", targets("a", "b", "c"))
	var unclaimed *unclaimedBackupError
	if !errors.As(err, &unclaimed) || !strings.Contains(err.Error(), "partition(s) 4, 5") || len(c.asked) != 3 {
		t.Fatalf("got %v after asking %v, want an unclaimed backup that names the missing share", err, c.asked)
	}
}

// A node that could not be reached, or one without the backup flags, is not a leader election. The
// result is a plain failure.
func TestBackupRefusedForAnotherCauseIsNotUnclaimed(t *testing.T) {
	notLeader := &kahuna.BackupError{StatusCode: 503, Outcome: kahuna.BackupOutcomeNotBackupCoordinator}
	notConfigured := &kahuna.BackupError{StatusCode: 503, Outcome: kahuna.BackupOutcomeNotConfigured}
	for name, other := range map[string]error{"unreachable": errors.New("connection refused"), "not configured": notConfigured} {
		c := &scriptedBackups{answers: map[string]error{"a": notLeader, "b": other}}
		_, err := runBackup(context.Background(), c, backupFull, "", targets("a", "b"))
		var unclaimed *unclaimedBackupError
		if err == nil || errors.As(err, &unclaimed) {
			t.Fatalf("%s: got %v, want a plain failure", name, err)
		}
	}
}

// An unclaimed backup stays due, is not counted, and goes out again after a short delay instead of
// the failure backoff.
func TestUnclaimedBackupIsRetriedSoonWithoutAFailure(t *testing.T) {
	full := mustSchedule(t, "0 2 * * *")
	st := &kahunav1alpha1.BackupStatus{}
	job := backupJob{kind: backupFull, scheduled: t0, err: &unclaimedBackupError{refusals: []string{"a: x"}}}
	applyBackupResult(st, job, t0, logr.Discard())

	if st.ConsecutiveFailures != 0 || st.LastFailureTime != nil || st.LastFullScheduleTime != nil {
		t.Fatalf("status after an unclaimed backup: %+v", st)
	}
	if !st.UnclaimedSince.Time.Equal(t0) || !st.LastUnclaimedTime.Time.Equal(t0) {
		t.Fatalf("unclaimed times: since %v, last %v", st.UnclaimedSince, st.LastUnclaimedTime)
	}
	d := decideBackup(backupInput{full: full, status: *st, now: t0.Add(time.Second)})
	if d.kind != backupNone || !d.retryAt.Equal(t0.Add(unclaimedRetryDelay)) {
		t.Fatalf("got %+v, want the retry held until %v", d, t0.Add(unclaimedRetryDelay))
	}
	if d := decideBackup(backupInput{full: full, status: *st, now: t0.Add(unclaimedRetryDelay)}); d.kind != backupFull {
		t.Fatalf("got %+v, want the full backup again after the delay", d)
	}

	// A later unclaimed attempt keeps the start of the run.
	applyBackupResult(st, job, t0.Add(time.Minute), logr.Discard())
	if !st.UnclaimedSince.Time.Equal(t0) || !st.LastUnclaimedTime.Time.Equal(t0.Add(time.Minute)) || st.ConsecutiveFailures != 0 {
		t.Fatalf("status after a second unclaimed backup: %+v", st)
	}

	// Success ends the run.
	applyBackupResult(st, backupJob{kind: backupFull, scheduled: t0, info: &kahuna.BackupInfo{BackupID: "b1"}}, t0.Add(2*time.Minute), logr.Discard())
	if st.UnclaimedSince != nil || st.LastUnclaimedTime != nil {
		t.Fatalf("status after success: %+v", st)
	}
}

// A cluster that never gets a backup coordinator is still reported, once the bound is reached.
func TestUnclaimedBackupFailsAfterTheBound(t *testing.T) {
	st := &kahunav1alpha1.BackupStatus{UnclaimedSince: at(0), LastUnclaimedTime: at(4 * time.Minute)}
	job := backupJob{kind: backupFull, scheduled: t0, err: &unclaimedBackupError{refusals: []string{"a: x"}}}
	applyBackupResult(st, job, t0.Add(unclaimedFailAfter), logr.Discard())

	if st.ConsecutiveFailures != 1 || st.LastFailureTime == nil || !strings.Contains(st.LastFailureMessage, "a: x") {
		t.Fatalf("status after the bound: %+v", st)
	}
	if st.UnclaimedSince != nil || st.LastUnclaimedTime != nil {
		t.Fatalf("unclaimed times not cleared: %+v", st)
	}

	// From here an unclaimed attempt counts like any other failure, with the backoff.
	applyBackupResult(st, job, t0.Add(10*time.Minute), logr.Discard())
	if st.ConsecutiveFailures != 2 || st.UnclaimedSince != nil {
		t.Fatalf("status after an unclaimed backup with a failure outstanding: %+v", st)
	}
}

func TestSuccessfulFullBackupAdvancesBothAnchors(t *testing.T) {
	st := &kahunav1alpha1.BackupStatus{ConsecutiveFailures: 2, LastFailureTime: at(-time.Hour), LastFailureMessage: "x"}
	job := backupJob{kind: backupFull, scheduled: t0, info: &kahuna.BackupInfo{BackupID: "b1", Type: kahuna.BackupTypeFull}}
	applyBackupResult(st, job, t0.Add(2*time.Minute), logr.Discard())

	if st.LastBackupID != "b1" || st.LastBackupType != kahuna.BackupTypeFull || st.ConsecutiveFailures != 0 || st.LastFailureMessage != "" {
		t.Fatalf("status after success: %+v", st)
	}
	if !st.LastFullScheduleTime.Time.Equal(t0) || !st.LastIncrementalScheduleTime.Time.Equal(t0.Add(2*time.Minute)) {
		t.Fatalf("anchors: full %v, incremental %v", st.LastFullScheduleTime, st.LastIncrementalScheduleTime)
	}
}

// The server takes a full backup in place of an incremental when the incremental cannot continue
// the chain. Status reports what was taken, and the chain continues from it.
func TestSubstitutedIncrementalIsRecordedAsFull(t *testing.T) {
	st := &kahunav1alpha1.BackupStatus{LastFullScheduleTime: at(-time.Hour), LastBackupID: "b1"}
	info := &kahuna.BackupInfo{BackupID: "b2", Type: kahuna.BackupTypeFull, ActualKind: kahuna.BackupTypeFull, SubstitutionReason: "TopologyChanged"}
	applyBackupResult(st, backupJob{kind: backupIncremental, scheduled: t0, info: info}, t0, logr.Discard())

	if st.LastBackupID != "b2" || st.LastBackupType != kahuna.BackupTypeFull || !st.LastIncrementalScheduleTime.Time.Equal(t0) {
		t.Fatalf("status after substitution: %+v", st)
	}
}

func TestFailedBackupIsCounted(t *testing.T) {
	st := &kahunav1alpha1.BackupStatus{ConsecutiveFailures: 1}
	job := backupJob{kind: backupFull, scheduled: t0, err: errors.New(strings.Repeat("x", 4000))}
	applyBackupResult(st, job, t0, logr.Discard())

	if st.ConsecutiveFailures != 2 || st.LastFailureTime == nil || st.LastFullScheduleTime != nil {
		t.Fatalf("status after failure: %+v", st)
	}
	if len(st.LastFailureMessage) > maxFailureMessage+len("Full backup: ") {
		t.Fatalf("failure message is %d bytes, want it truncated", len(st.LastFailureMessage))
	}
}

// When the parent is gone, no incremental can continue the chain, so a new chain starts with a
// full backup at once, and that is not a failure.
func TestMissingParentStartsANewChain(t *testing.T) {
	st := &kahunav1alpha1.BackupStatus{LastFullScheduleTime: at(-time.Hour), LastBackupID: "b1"}
	missing := &kahuna.BackupError{StatusCode: 404, Outcome: kahuna.BackupOutcomeParentMissing}
	applyBackupResult(st, backupJob{kind: backupIncremental, scheduled: t0, parent: "b1", err: missing}, t0, logr.Discard())

	if st.LastBackupID != "" || st.LastFullScheduleTime != nil || st.ConsecutiveFailures != 0 {
		t.Fatalf("status after a missing parent: %+v", st)
	}
	if d := decideBackup(backupInput{full: mustSchedule(t, "0 2 * * *"), status: *st, now: t0}); d.kind != backupFull {
		t.Fatalf("got %s, want a full backup at once", d.kind)
	}
}

func backupCluster() *kahunav1alpha1.KahunaCluster {
	c := testCluster(3)
	c.Spec.Backup = &kahunav1alpha1.BackupSpec{
		Schedule: testBackupSchedule,
		Volume:   kahunav1alpha1.BackupVolumeSpec{Claim: &kahunav1alpha1.VolumeSpec{Size: resource.MustParse("10Gi")}},
	}
	return c
}

func TestBackupFlags(t *testing.T) {
	c := backupCluster()
	c.Spec.Backup.Retention = &kahunav1alpha1.BackupRetentionSpec{
		MaxChains: ptr.To(int32(7)),
		MaxAge:    &metav1.Duration{Duration: 168 * time.Hour},
		MaxBytes:  ptr.To(resource.MustParse("1Gi")),
	}
	args := strings.Join(staticArgs(c), " ")
	for _, want := range []string{
		"--pitr-backup-dir /backup/" + c.Name,
		"--pitr-backup-cluster-id " + c.Namespace + "/" + c.Name,
		"--pitr-backup-mac-key-file /etc/kahuna/backup-mac/key",
		"--backup-gc-interval 0",
		"--backup-retention-max-chains 7",
		"--backup-retention-max-age 604800",
		"--backup-retention-max-bytes 1073741824",
	} {
		if !strings.Contains(args, want) {
			t.Errorf("args lack %q:\n%s", want, args)
		}
	}

	c.Spec.Backup = nil
	if args := strings.Join(staticArgs(c), " "); strings.Contains(args, "backup") {
		t.Errorf("a cluster without backups got backup flags:\n%s", args)
	}
}

func TestBackupVolumes(t *testing.T) {
	c := backupCluster()
	c.Spec.Backup.Volume = kahunav1alpha1.BackupVolumeSpec{ClaimName: ptr.To("shared-nfs")}
	c.Spec.Backup.MACKeySecretRef = &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "my-mac"}, Key: "k",
	}

	var claim, mac *corev1.Volume
	for _, v := range podVolumes(c) {
		switch v.Name {
		case backupVolumeName:
			claim = &v
		case backupMACVolumeName:
			mac = &v
		}
	}
	if claim == nil || claim.PersistentVolumeClaim.ClaimName != "shared-nfs" {
		t.Fatalf("backup volume: %+v", claim)
	}
	if mac == nil || mac.Secret.SecretName != "my-mac" || mac.Secret.Items[0].Key != "k" || mac.Secret.Items[0].Path != backupMACFileName {
		t.Fatalf("MAC volume: %+v", mac)
	}
}

// The operator's claim is shared by every node, so it is ReadWriteMany unless the spec says
// otherwise.
func TestBackupClaimDefaultsToReadWriteMany(t *testing.T) {
	c := backupCluster()
	pvc := desiredBackupClaim(c)
	if pvc.Name != c.Name+"-backup" || !slices.Equal(pvc.Spec.AccessModes, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}) {
		t.Fatalf("claim %s with access modes %v", pvc.Name, pvc.Spec.AccessModes)
	}
	c.Spec.Backup.Volume.Claim.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	if got := desiredBackupClaim(c).Spec.AccessModes; !slices.Equal(got, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}) {
		t.Fatalf("access modes %v, want the spec's", got)
	}
}

// Nothing shares a single node's claim, and most StorageClasses cannot provision ReadWriteMany, so
// a single-node cluster asks for ReadWriteOnce.
func TestSingleNodeBackupClaimDefaultsToReadWriteOnce(t *testing.T) {
	c := backupCluster()
	c.Spec.Replicas = ptr.To(int32(1))
	if got := desiredBackupClaim(c).Spec.AccessModes; !slices.Equal(got, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}) {
		t.Fatalf("access modes %v, want ReadWriteOnce", got)
	}
	c.Spec.Backup.Volume.Claim.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}
	if got := desiredBackupClaim(c).Spec.AccessModes; !slices.Equal(got, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}) {
		t.Fatalf("access modes %v, want the spec's", got)
	}
}

// An unbound claim holds no data, so it follows the spec. A bound one is never replaced.
func TestUnboundBackupClaimFollowsTheSpec(t *testing.T) {
	c := backupCluster()
	existing := desiredBackupClaim(c)
	existing.Status.Phase = corev1.ClaimPending
	// The API server fills in the default class when the spec names none.
	existing.Spec.StorageClassName = ptr.To("standard")

	if claimNeedsReplacing(existing, desiredBackupClaim(c)) {
		t.Fatal("replaced a claim that matches the spec")
	}

	c.Spec.Backup.Volume.Claim.StorageClassName = ptr.To("nfs")
	if !claimNeedsReplacing(existing, desiredBackupClaim(c)) {
		t.Fatal("kept a claim with a different storage class")
	}
	c.Spec.Backup.Volume.Claim.StorageClassName = ptr.To("standard")

	c.Spec.Backup.Volume.Claim.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	if !claimNeedsReplacing(existing, desiredBackupClaim(c)) {
		t.Fatal("kept a claim with different access modes")
	}
	c.Spec.Backup.Volume.Claim.AccessModes = nil

	c.Spec.Backup.Volume.Claim.Size = resource.MustParse("20Gi")
	if !claimNeedsReplacing(existing, desiredBackupClaim(c)) {
		t.Fatal("kept a claim with a different size")
	}

	bound := existing.DeepCopy()
	bound.Status.Phase = corev1.ClaimBound
	if claimNeedsReplacing(bound, desiredBackupClaim(c)) {
		t.Fatal("replaced a bound claim")
	}
	prebound := existing.DeepCopy()
	prebound.Spec.VolumeName = "pv-1"
	if claimNeedsReplacing(prebound, desiredBackupClaim(c)) {
		t.Fatal("replaced a claim that has a volume")
	}
	deleting := existing.DeepCopy()
	deleting.DeletionTimestamp = ptr.To(metav1.NewTime(t0))
	if claimNeedsReplacing(deleting, desiredBackupClaim(c)) {
		t.Fatal("replaced a claim that is already being deleted")
	}
}

// Events written through the events.k8s.io API carry their time in eventTime or series, not in
// lastTimestamp.
func TestEventTimeUsesTheNewestField(t *testing.T) {
	e := &corev1.Event{ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(t0)}}
	if got := eventTime(e); !got.Equal(t0) {
		t.Fatalf("got %v, want the creation time", got)
	}
	e.LastTimestamp = metav1.NewTime(t0.Add(time.Minute))
	e.EventTime = metav1.NewMicroTime(t0.Add(2 * time.Minute))
	e.Series = &corev1.EventSeries{LastObservedTime: metav1.NewMicroTime(t0.Add(3 * time.Minute))}
	if got := eventTime(e); !got.Equal(t0.Add(3 * time.Minute)) {
		t.Fatalf("got %v, want the series time", got)
	}
}

func readyPod(c *kahunav1alpha1.KahunaCluster, ordinal int32, hash string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName(c, ordinal), Annotations: map[string]string{configHashAnnotation: hash}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: containerName, Image: c.Spec.Image}}},
		Status: corev1.PodStatus{PodIP: "10.0.0.1", Conditions: []corev1.PodCondition{{
			Type: corev1.PodReady, Status: corev1.ConditionTrue,
		}}},
	}
}

// A backup waits for every node to run the backup flags: the coordinated backup goes to the
// meta-partition leader, which can be any node.
func TestBackupWaitsForTheRollout(t *testing.T) {
	c := backupCluster()
	c.Status.Bootstrapped = true
	hash := configHash(entrypointScript(c))
	m := &kahuna.Membership{}
	for i := range int32(3) {
		m.Members = append(m.Members, kahuna.Member{Endpoint: raftEndpoint(c, i), Role: kahuna.RoleVoter})
	}

	pods := []corev1.Pod{readyPod(c, 0, hash), readyPod(c, 1, "old"), readyPod(c, 2, hash)}
	if reason := backupBlocked(c, m, pods); !strings.Contains(reason, "current configuration") {
		t.Fatalf("blocked = %q, want the rollout named", reason)
	}
	pods[1] = readyPod(c, 1, hash)
	if reason := backupBlocked(c, m, pods); reason != "" {
		t.Fatalf("blocked = %q on a steady cluster", reason)
	}
	if reason := backupBlocked(c, m, pods[:2]); reason == "" {
		t.Fatal("not blocked with a pod missing")
	}
	if reason := backupBlocked(c, nil, pods); reason == "" {
		t.Fatal("not blocked with an unknown roster")
	}
}

// The config hash does not cover the image, so an image rollout is checked on its own: under a
// replication factor the coordinator asks every node for a share of the partitions.
func TestBackupWaitsForTheImageRollout(t *testing.T) {
	c := backupCluster()
	c.Status.Bootstrapped = true
	hash := configHash(entrypointScript(c))
	m := &kahuna.Membership{}
	for i := range int32(3) {
		m.Members = append(m.Members, kahuna.Member{Endpoint: raftEndpoint(c, i), Role: kahuna.RoleVoter})
	}

	pods := []corev1.Pod{readyPod(c, 0, hash), readyPod(c, 1, hash), readyPod(c, 2, hash)}
	pods[2].Spec.Containers[0].Image = testOldImage
	if reason := backupBlocked(c, m, pods); !strings.Contains(reason, "does not run image") {
		t.Fatalf("blocked = %q, want the image rollout named", reason)
	}
}

func TestImageVersion(t *testing.T) {
	for image, want := range map[string][3]int{
		"kahunakv/kahuna:1.12.6":                      {1, 12, 6},
		"registry.local:5000/kahunakv/kahuna:v1.12.4": {1, 12, 4},
		"kahunakv/kahuna:1.12.5-rc.1@sha256:abc":      {1, 12, 5},
	} {
		if got, ok := imageVersion(image); !ok || got != want {
			t.Errorf("imageVersion(%q) = %v, %v; want %v", image, got, ok, want)
		}
	}
	for _, image := range []string{
		"kahunakv/kahuna", testLatestImage, testDevImage,
		"registry.local:5000/kahunakv/kahuna", "kahunakv/kahuna@sha256:abc", "kahunakv/kahuna:1.12",
	} {
		if got, ok := imageVersion(image); ok {
			t.Errorf("imageVersion(%q) = %v, want no version", image, got)
		}
	}
}

// Only a server before 1.12.5, on a cluster that can have more nodes than the factor, takes a
// backup that misses partitions. An image without a release tag is not refused.
func TestBackupUnsupportedOnlyBeforeComposedBackups(t *testing.T) {
	cases := []struct {
		name     string
		image    string
		replicas int32
		factor   int32
		maxNodes int32
		refused  bool
	}{
		{"old server, placed", testOldImage, 5, 3, 0, true},
		{"old server, autoscaled past the factor", testOldImage, 3, 3, 6, true},
		{"old server, factor covers every node", testOldImage, 3, 3, 0, false},
		{"old server, no factor", testOldImage, 5, 0, 0, false},
		{"composing server", "kahunakv/kahuna:1.12.5", 5, 3, 0, false},
		{"newer server", "kahunakv/kahuna:1.13.0", 5, 3, 0, false},
		{"unknown version", testLatestImage, 5, 3, 0, false},
	}
	for _, tc := range cases {
		c := backupCluster()
		c.Spec.Image = tc.image
		c.Spec.Replicas = ptr.To(tc.replicas)
		if tc.factor > 0 {
			c.Spec.Replication = &kahunav1alpha1.ReplicationSpec{Factor: tc.factor}
		}
		if tc.maxNodes > 0 {
			c.Spec.Autoscaling = &kahunav1alpha1.AutoscalingSpec{MinReplicas: 3, MaxReplicas: tc.maxNodes}
		}
		reason := backupUnsupported(c)
		if (reason != "") != tc.refused {
			t.Errorf("%s: reason %q, want refused=%v", tc.name, reason, tc.refused)
		}
		if tc.refused && !strings.Contains(reason, "Kahuna 1.12.5 or later") {
			t.Errorf("%s: reason %q does not name the minimum", tc.name, reason)
		}
	}
}
