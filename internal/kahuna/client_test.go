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

package kahuna

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Test bounds. The read bound is short enough that a delayed answer trips it, and the leave bound
// is far above the delay, so the two cases separate without depending on machine speed.
const (
	testReadTimeout  = 50 * time.Millisecond
	testLeaveTimeout = 10 * time.Second
	testServerDelay  = 400 * time.Millisecond
)

// delayedServer answers path after a delay, and answers anything else at once.
func delayedServer(t *testing.T, path, body string, delay time.Duration) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == path {
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				// The client gave up. Returning drops the connection, which is what a real
				// server does when the request is aborted.
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// A leave must not carry the read bound. The server holds the request open while it evacuates the
// departing node's replicas, and it links that work to the request, so a client that gives up
// early cancels the evacuation rather than merely stopping its own wait.
func TestLeaveIsNotBoundByTheReadTimeout(t *testing.T) {
	srv := delayedServer(t, "/v1/cluster/leave",
		`{"left":true,"outcome":"Committed","membershipVersion":7,"retryable":false}`, testServerDelay)

	c := NewHTTPClient(testReadTimeout, testLeaveTimeout)

	result, err := c.Leave(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("Leave failed although its own bound is %v: %v", testLeaveTimeout, err)
	}
	if !result.Removed() {
		t.Errorf("Removed() = false for outcome %q, want true", result.Outcome)
	}
	if result.MembershipVersion != 7 {
		t.Errorf("MembershipVersion = %d, want 7", result.MembershipVersion)
	}
}

// The read bound still applies to a status read, which must never wedge a reconcile.
func TestReadsAreBoundByTheReadTimeout(t *testing.T) {
	srv := delayedServer(t, "/v1/cluster/membership",
		`{"membershipVersion":1,"members":[],"localRole":"Voter","initialized":true}`, testServerDelay)

	c := NewHTTPClient(testReadTimeout, testLeaveTimeout)

	start := time.Now()
	if _, err := c.Membership(context.Background(), srv.URL); err == nil {
		t.Fatal("Membership succeeded although the server answered after the read bound")
	}
	if elapsed := time.Since(start); elapsed >= testServerDelay {
		t.Errorf("Membership waited %v, which is at least the server delay %v; the read bound did not apply",
			elapsed, testServerDelay)
	}
}

// The autoscaler's reads carry the same bound. It polls them on every pass, so an unbounded one
// would let a single unresponsive node stall the cluster's reconcile indefinitely.
func TestAutoscalerReadsAreBoundByTheReadTimeout(t *testing.T) {
	calls := map[string]func(*HTTPClient, string) error{
		"/v1/cluster/placement": func(c *HTTPClient, url string) error {
			_, err := c.Placement(context.Background(), url)
			return err
		},
		"/v1/dashboard/metrics": func(c *HTTPClient, url string) error {
			_, err := c.Metrics(context.Background(), url)
			return err
		},
	}
	for path, call := range calls {
		t.Run(path, func(t *testing.T) {
			srv := delayedServer(t, path, `{}`, testServerDelay)
			c := NewHTTPClient(testReadTimeout, testLeaveTimeout)

			start := time.Now()
			if err := call(c, srv.URL); err == nil {
				t.Fatalf("%s succeeded although the server answered after the read bound", path)
			}
			if elapsed := time.Since(start); elapsed >= testServerDelay {
				t.Errorf("%s waited %v, which is at least the server delay %v; the read bound did not apply",
					path, elapsed, testServerDelay)
			}
		})
	}
}

// A leave bound of zero means "no bound of our own", leaving the caller's context as the only
// limit. Without that, a zero value would read as an instant deadline and fail every call.
func TestZeroLeaveTimeoutDefersToTheCallerContext(t *testing.T) {
	srv := delayedServer(t, "/v1/cluster/leave", `{"left":false,"outcome":"NoLeader","retryable":true}`, 0)

	c := NewHTTPClient(testReadTimeout, 0)

	result, err := c.Leave(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("Leave failed with no bound of its own: %v", err)
	}
	if result.Outcome != "NoLeader" {
		t.Errorf("Outcome = %q, want NoLeader", result.Outcome)
	}
}

// The outcomes the operator does not name must not read as success. A scale-down that treats an
// unrecognized outcome as "the node is out" would delete a pod that is still a committed member,
// which is the exact situation the leave API exists to avoid.
func TestUnnamedOutcomesAreNotRemoved(t *testing.T) {
	// Every outcome the server can report beside the three the operator names.
	for _, outcome := range []string{
		"RefusedDrainInProgress", "DrainTimedOut", "NotInitialized", "NoLeader", "Timeout",
	} {
		result := &LeaveResult{Outcome: outcome}
		if result.Removed() {
			t.Errorf("Removed() = true for outcome %q, want false", outcome)
		}
	}
	for _, outcome := range []string{OutcomeCommitted, OutcomeNotAMember} {
		result := &LeaveResult{Outcome: outcome}
		if !result.Removed() {
			t.Errorf("Removed() = false for outcome %q, want true", outcome)
		}
	}
	if (&LeaveResult{Outcome: OutcomeRefusedInsufficientVoters}).Removed() {
		t.Error("Removed() = true for a last-voter refusal, want false")
	}
}

// Health reports a 503 as a result rather than an error: it is the documented "up but not
// serving" answer, and it carries a valid body.
func TestHealthAcceptsServiceUnavailable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"ready":false,"initialized":false,"localRole":"NotMember"}`))
	}))
	t.Cleanup(srv.Close)

	c := NewHTTPClient(testLeaveTimeout, testLeaveTimeout)

	health, err := c.Health(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("Health returned an error for a 503: %v", err)
	}
	if health.Ready {
		t.Error("Ready = true, want false")
	}
}

func TestPlacementReportsAServerWithoutTheEndpoint(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.NotFound(w, nil)
	}))
	t.Cleanup(srv.Close)

	c := NewHTTPClient(testLeaveTimeout, testLeaveTimeout)

	_, err := c.Placement(context.Background(), srv.URL)
	if !errors.Is(err, ErrPlacementUnsupported) {
		t.Fatalf("a 404 was reported as %v, want ErrPlacementUnsupported", err)
	}
}

// A refused backup carries its outcome in a header and in the problem body. The operator decides
// from the outcome whether another node can take the backup.
func TestBackupRefusalIsTyped(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/problem+json")
		w.Header().Set("X-Kahuna-Backup-Outcome", BackupOutcomeNotBackupCoordinator)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":503,"detail":"This node is not the backup coordinator","outcome":"NotBackupCoordinator"}`))
	}))
	t.Cleanup(srv.Close)

	_, err := NewHTTPClient(testReadTimeout, testLeaveTimeout).TakeCoordinatedBackup(context.Background(), srv.URL)
	var be *BackupError
	if !errors.As(err, &be) {
		t.Fatalf("got %v, want a *BackupError", err)
	}
	if be.StatusCode != 503 || be.Outcome != BackupOutcomeNotBackupCoordinator || !be.TryAnotherNode() {
		t.Fatalf("got %+v", be)
	}
	if !strings.Contains(be.Error(), "not the backup coordinator") {
		t.Fatalf("message %q lacks the server's detail", be.Error())
	}
}

// A backup is not a status read: a full backup copies the whole store, so it must not carry the
// short read bound.
func TestBackupIsNotBoundByTheReadTimeout(t *testing.T) {
	srv := delayedServer(t, "/v1/backups/coordinated",
		`{"backupId":"7d9e1c4a-0000-0000-0000-000000000001","type":"Full","parentBackupId":null}`, testServerDelay)

	info, err := NewHTTPClient(testReadTimeout, testLeaveTimeout).TakeCoordinatedBackup(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("TakeCoordinatedBackup: %v", err)
	}
	if info.BackupID != "7d9e1c4a-0000-0000-0000-000000000001" || info.Type != BackupTypeFull || info.ParentBackupID != "" {
		t.Fatalf("got %+v", info)
	}
}

func TestIncrementalBackupSendsTheParent(t *testing.T) {
	var got map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/backups/incremental" || r.Header.Get("Content-Type") != "application/json" {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
		_, _ = w.Write([]byte(`{"backupId":"b2","type":"Incremental","parentBackupId":"b1"}`))
	}))
	t.Cleanup(srv.Close)

	info, err := NewHTTPClient(testReadTimeout, testLeaveTimeout).TakeIncrementalBackup(context.Background(), srv.URL, "b1")
	if err != nil {
		t.Fatalf("TakeIncrementalBackup: %v", err)
	}
	if got["parentBackupId"] != "b1" || info.BackupID != "b2" || info.ParentBackupID != "b1" {
		t.Fatalf("sent %v, got %+v", got, info)
	}
}

// A standalone node lists its two phantom witnesses as Voters. They are not nodes: nothing serves
// them, so the roster the operator compares against its StatefulSet must not contain them.
func TestMembershipDropsPhantomWitnesses(t *testing.T) {
	const body = `{"membershipVersion":1,"members":[` +
		`{"endpoint":"solo-0.solo-peer.ns.svc.cluster.local:8082","nodeId":1,"role":"Voter","joinedVersion":1},` +
		`{"endpoint":"embedded-witness-1:0","nodeId":0,"role":"Voter","joinedVersion":1},` +
		`{"endpoint":"embedded-witness-2:0","nodeId":0,"role":"Voter","joinedVersion":1}],` +
		`"localRole":"Voter","initialized":true}`
	srv := delayedServer(t, "/none", body, 0)

	c := &HTTPClient{HTTP: srv.Client(), ReadTimeout: testLeaveTimeout}
	m, err := c.Membership(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("membership: %v", err)
	}
	if len(m.Members) != 1 || len(m.Voters()) != 1 {
		t.Fatalf("witnesses were kept: %+v", m.Members)
	}
	if !m.HasVoter("solo-0.solo-peer.ns.svc.cluster.local:8082") {
		t.Fatalf("the real node was dropped: %+v", m.Members)
	}
	if m.Contains("embedded-witness-1:0") {
		t.Fatalf("a witness is still in the roster: %+v", m.Members)
	}
}
