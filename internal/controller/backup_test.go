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

func readyPod(c *kahunav1alpha1.KahunaCluster, ordinal int32, hash string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName(c, ordinal), Annotations: map[string]string{configHashAnnotation: hash}},
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
