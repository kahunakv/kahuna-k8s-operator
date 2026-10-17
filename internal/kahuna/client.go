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

// Package kahuna is a thin read-only client for the Kahuna server's cluster endpoints. The
// operator gates every membership-changing step on what the cluster itself reports, rather than
// on what Kubernetes believes, because the Raft roster is the authoritative record of who is a
// member — a pod can be Running and not be one, and a roster entry can outlive its pod.
package kahuna

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Role values reported by the server. Only Voters count toward quorum.
const (
	RoleVoter     = "Voter"
	RoleLearner   = "Learner"
	RoleLeaving   = "Leaving"
	RoleNotMember = "NotMember"
)

// Member is one entry of the committed Raft roster.
type Member struct {
	Endpoint      string `json:"endpoint"`
	NodeID        int32  `json:"nodeId"`
	Role          string `json:"role"`
	JoinedVersion int64  `json:"joinedVersion"`
}

// Membership is the response of GET /v1/cluster/membership.
type Membership struct {
	MembershipVersion int64    `json:"membershipVersion"`
	Members           []Member `json:"members"`
	LocalRole         string   `json:"localRole"`
	Initialized       bool     `json:"initialized"`
}

// Voters returns the members that count toward quorum.
func (m *Membership) Voters() []Member {
	var out []Member
	for _, mem := range m.Members {
		if mem.Role == RoleVoter {
			out = append(out, mem)
		}
	}
	return out
}

// HasVoter reports whether the endpoint is a committed Voter — the condition the operator waits
// for before treating a newly added node as carrying its share of the cluster.
func (m *Membership) HasVoter(endpoint string) bool {
	for _, mem := range m.Members {
		if mem.Endpoint == endpoint && mem.Role == RoleVoter {
			return true
		}
	}
	return false
}

// Contains reports whether the endpoint appears in the roster in any role. Scale-down waits for
// this to go false, which is how a SWIM eviction is observed.
func (m *Membership) Contains(endpoint string) bool {
	for _, mem := range m.Members {
		if mem.Endpoint == endpoint {
			return true
		}
	}
	return false
}

// Health is the response of GET /v1/cluster/health.
type Health struct {
	Ready       bool   `json:"ready"`
	Initialized bool   `json:"initialized"`
	LocalRole   string `json:"localRole"`
}

// Leave outcomes reported by POST /v1/cluster/leave.
const (
	// OutcomeCommitted means the RemoveMember entry is committed: the node is out of the roster
	// and surviving members no longer count it toward quorum.
	OutcomeCommitted = "Committed"
	// OutcomeNotAMember means the endpoint was already absent. Success, for idempotency.
	OutcomeNotAMember = "NotAMember"
	// OutcomeRefusedInsufficientVoters means the removal would leave zero voters. Permanent:
	// retrying can never succeed while the roster is unchanged.
	OutcomeRefusedInsufficientVoters = "RefusedInsufficientVoters"
)

// LeaveResult is the response of POST /v1/cluster/leave.
type LeaveResult struct {
	Left              bool   `json:"left"`
	Outcome           string `json:"outcome"`
	MembershipVersion int64  `json:"membershipVersion"`
	Retryable         bool   `json:"retryable"`
	Reason            string `json:"reason"`
}

// Removed reports whether the node is out of the roster — either because this call committed the
// removal or because it was already gone. Both mean the pod is safe to stop.
func (l *LeaveResult) Removed() bool {
	return l.Outcome == OutcomeCommitted || l.Outcome == OutcomeNotAMember
}

// PartitionStateActive is the state of a partition that serves traffic, as reported by
// GET /v1/cluster/placement.
const PartitionStateActive = "Active"

// ReplicaPlacement is one replica of a partition as GET /v1/cluster/placement reports it.
type ReplicaPlacement struct {
	Endpoint string `json:"endpoint"`
	Role     string `json:"role"`
}

// PartitionPlacement is one partition's committed replica set. An empty Replicas list means legacy
// full replication: every roster Voter hosts the partition.
type PartitionPlacement struct {
	PartitionID                int32              `json:"partitionId"`
	State                      string             `json:"state"`
	EffectiveReplicationFactor int32              `json:"effectiveReplicationFactor"`
	Replicas                   []ReplicaPlacement `json:"replicas"`
}

// Placement is the response of GET /v1/cluster/placement.
type Placement struct {
	ReplicationFactor int32                `json:"replicationFactor"`
	RebalancerEnabled bool                 `json:"rebalancerEnabled"`
	Initialized       bool                 `json:"initialized"`
	Partitions        []PartitionPlacement `json:"partitions"`
}

// FullyReplicated returns the active data partitions that have no replica set. Under a replication
// factor these are the partitions that grow by one copy for every node added, because the server
// never converts them to placed ranges.
func (p *Placement) FullyReplicated() []int32 {
	var out []int32
	for _, part := range p.Partitions {
		// Partition 0 is the system partition, which always spans every node by design.
		if part.PartitionID > 0 && part.State == PartitionStateActive && len(part.Replicas) == 0 {
			out = append(out, part.PartitionID)
		}
	}
	return out
}

// Metric kinds reported by GET /v1/dashboard/metrics.
const (
	MetricKindCounter = "Counter"
	MetricKindGauge   = "Gauge"
)

// MetricRow is one instrument-and-tag-set row of GET /v1/dashboard/metrics. Counters carry their
// cumulative sum in Total; gauges carry the sampled value in Last.
type MetricRow struct {
	Metric string   `json:"metric"`
	Tags   string   `json:"tags"`
	Kind   string   `json:"kind"`
	Count  int64    `json:"count"`
	Total  *float64 `json:"total"`
	Last   *float64 `json:"last"`
}

// Metrics is the response of GET /v1/dashboard/metrics.
type Metrics struct {
	Rows []MetricRow `json:"rows"`
	// MonotonicMs is the node's monotonic clock at sampling time. Rates are computed against it
	// rather than the operator's clock, so request latency does not distort them.
	MonotonicMs int64 `json:"monotonicMs"`
}

// CounterTotal sums a counter across every tag set.
func (m *Metrics) CounterTotal(metric string) (float64, bool) {
	var sum float64
	found := false
	for _, r := range m.Rows {
		if r.Metric == metric && r.Kind == MetricKindCounter && r.Total != nil {
			sum += *r.Total
			found = true
		}
	}
	return sum, found
}

// GaugeSum sums a gauge's latest value across every tag set.
func (m *Metrics) GaugeSum(metric string) (float64, bool) {
	var sum float64
	found := false
	for _, r := range m.Rows {
		if r.Metric == metric && r.Kind == MetricKindGauge && r.Last != nil {
			sum += *r.Last
			found = true
		}
	}
	return sum, found
}

// Backup types reported by the server in BackupInfo.Type.
const (
	BackupTypeFull        = "Full"
	BackupTypeIncremental = "Incremental"
)

// Backup outcomes the server reports for a refused backup request, in the X-Kahuna-Backup-Outcome
// header and the "outcome" field of the problem body. Only the outcomes the operator acts on are
// named; the server has more.
const (
	// BackupOutcomeNotBackupCoordinator means the node does not lead the meta partition, so it
	// cannot take a coordinated backup. Another node can.
	BackupOutcomeNotBackupCoordinator = "NotBackupCoordinator"
	// BackupOutcomeRetryableLeadershipLoss means leadership moved during the backup.
	BackupOutcomeRetryableLeadershipLoss = "RetryableLeadershipLoss"
	// BackupOutcomeCutUnverified means transactions kept overtaking the cut. Nothing was published.
	BackupOutcomeCutUnverified = "CutUnverified"
	// BackupOutcomeNotConfigured means the node runs without --pitr-backup-dir, which is what a node
	// not yet restarted with the backup flags looks like.
	BackupOutcomeNotConfigured = "NotConfigured"
	// BackupOutcomeParentMissing means the catalog has no backup with the requested parent id.
	BackupOutcomeParentMissing = "ParentMissing"
)

// BackupInfo is the server's description of one backup (KahunaBackupInfo). Only the fields the
// operator uses are decoded.
type BackupInfo struct {
	BackupID        string `json:"backupId"`
	Type            string `json:"type"`
	ParentBackupID  string `json:"parentBackupId"`
	ClusterID       string `json:"clusterId"`
	CoordinatorNode string `json:"coordinatorNode"`
	// ActualKind differs from the requested kind when the server took a full backup in place of an
	// incremental one; SubstitutionReason then says why.
	ActualKind         string `json:"actualKind"`
	SubstitutionReason string `json:"substitutionReason"`
}

// BackupError is a backup request the server refused with a typed outcome.
type BackupError struct {
	StatusCode int
	Outcome    string
	Detail     string
}

func (e *BackupError) Error() string {
	if e.Detail == "" {
		return fmt.Sprintf("backup refused with HTTP %d (%s)", e.StatusCode, e.Outcome)
	}
	return fmt.Sprintf("backup refused with HTTP %d (%s): %s", e.StatusCode, e.Outcome, e.Detail)
}

// TryAnotherNode reports whether a different node can still take the backup now. Every other
// outcome is a refusal that a different node would repeat.
func (e *BackupError) TryAnotherNode() bool {
	switch e.Outcome {
	case BackupOutcomeNotBackupCoordinator, BackupOutcomeRetryableLeadershipLoss,
		BackupOutcomeCutUnverified, BackupOutcomeNotConfigured:
		return true
	}
	return false
}

// ClearsOnRetry reports whether the same request can succeed later with nothing changed: the
// refusal comes from a meta partition with no leader yet, a leader that just moved, or a cut that
// the transactions in flight overtook. NotConfigured is not one of these: it needs a restart with
// the backup flags.
func (e *BackupError) ClearsOnRetry() bool {
	switch e.Outcome {
	case BackupOutcomeNotBackupCoordinator, BackupOutcomeRetryableLeadershipLoss, BackupOutcomeCutUnverified:
		return true
	}
	return false
}

// Client reads cluster state from a Kahuna node, and asks one to decommission itself.
type Client interface {
	// Membership returns the committed roster as seen by the node at baseURL.
	Membership(ctx context.Context, baseURL string) (*Membership, error)
	// Health returns the node's readiness. A 503 is a valid answer, not an error: it means the
	// node is up but cannot serve.
	Health(ctx context.Context, baseURL string) (*Health, error)
	// Leave asks the node to remove itself from the committed roster. Every documented outcome
	// comes back as a result rather than an error, because a caller driving a scale-down has to
	// tell "the node is out" from "nothing happened" — and only a transport failure is genuinely
	// unclassifiable.
	Leave(ctx context.Context, baseURL string) (*LeaveResult, error)
	// Placement returns the committed replica set of every partition, as seen by the node.
	Placement(ctx context.Context, baseURL string) (*Placement, error)
	// Metrics returns the node's curated engine metrics. The endpoint does not exist when the
	// server runs with --disable-dashboard, which callers must treat as "signal unknown".
	Metrics(ctx context.Context, baseURL string) (*Metrics, error)
	// TakeCoordinatedBackup asks the node to take a full backup at one cluster-wide cut. Only the
	// meta-partition leader accepts it. A refusal comes back as a *BackupError.
	TakeCoordinatedBackup(ctx context.Context, baseURL string) (*BackupInfo, error)
	// TakeIncrementalBackup asks the node to back up the log written since the parent backup. A
	// refusal comes back as a *BackupError.
	TakeIncrementalBackup(ctx context.Context, baseURL, parentBackupID string) (*BackupInfo, error)
}

// Default timeouts. A status read and a leave need different bounds, and one value cannot serve
// both.
//
// DefaultLeaveTimeout covers the Kahuna server's own bound on a graceful leave: it holds the
// request open while it evacuates the departing node's replicas, for up to
// --raft-decommission-drain-timeout (120s by default) plus its 30s guard margin. The server links
// that work to the request, so a client that gives up first cancels the drain rather than merely
// stopping the wait. Under the default full replication no range names the node, so nothing is
// evacuated and the call returns at once; the long bound only matters where
// --raft-replication-factor is set.
const (
	DefaultReadTimeout  = 5 * time.Second
	DefaultLeaveTimeout = 150 * time.Second
)

// HTTPClient talks to the plaintext client port. The operator deliberately uses HTTP rather than
// the TLS port: the cluster's keystore is typically self-signed and scoped to in-cluster peer
// names, so requiring the operator to trust it would add a failure mode for a read-only status
// query that never leaves the cluster network.
type HTTPClient struct {
	HTTP *http.Client
	// ReadTimeout bounds Membership, Health, Placement and Metrics. It stays short: a status read
	// must never wedge a reconcile, and an unreachable node is reported as an unknown roster rather
	// than an error.
	ReadTimeout time.Duration
	// LeaveTimeout bounds Leave, which is not a status read and must not carry a read bound.
	LeaveTimeout time.Duration
}

// NewHTTPClient returns a client with a separate bound for reads and for a leave.
//
// The shared http.Client carries no Timeout of its own. Each call attaches its own deadline to the
// request context instead, which bounds the dial, the response and the body read alike, and lets
// one client hold two different bounds.
func NewHTTPClient(readTimeout, leaveTimeout time.Duration) *HTTPClient {
	return &HTTPClient{HTTP: &http.Client{}, ReadTimeout: readTimeout, LeaveTimeout: leaveTimeout}
}

// bound derives a deadline context from ctx. A non-positive duration means "no bound of our own",
// which leaves the caller's context as the only limit.
func bound(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, d)
}

func (c *HTTPClient) get(ctx context.Context, url string, out any) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return resp.StatusCode, err
	}
	if err := json.Unmarshal(body, out); err != nil {
		// A refusal need not carry the document that was asked for — a 404 comes with a plain
		// text body — and the status is then the whole answer. A 200 that does not decode is a
		// real error.
		if resp.StatusCode != http.StatusOK {
			return resp.StatusCode, nil
		}
		return resp.StatusCode, fmt.Errorf("decoding %s (status %d): %w", url, resp.StatusCode, err)
	}
	return resp.StatusCode, nil
}

// Membership implements Client.
func (c *HTTPClient) Membership(ctx context.Context, baseURL string) (*Membership, error) {
	ctx, cancel := bound(ctx, c.ReadTimeout)
	defer cancel()

	var m Membership
	code, err := c.get(ctx, baseURL+"/v1/cluster/membership", &m)
	if err != nil {
		return nil, err
	}
	if code != http.StatusOK {
		return nil, fmt.Errorf("membership query returned HTTP %d", code)
	}
	return &m, nil
}

// Health implements Client.
//
// 503 is the documented "up but not serving" answer and carries a valid body, so it is decoded
// and returned rather than surfaced as an error.
func (c *HTTPClient) Health(ctx context.Context, baseURL string) (*Health, error) {
	ctx, cancel := bound(ctx, c.ReadTimeout)
	defer cancel()

	var h Health
	code, err := c.get(ctx, baseURL+"/v1/cluster/health", &h)
	if err != nil {
		return nil, err
	}
	if code != http.StatusOK && code != http.StatusServiceUnavailable {
		return nil, fmt.Errorf("health query returned HTTP %d", code)
	}
	return &h, nil
}

// ErrPlacementUnsupported is returned by Placement when the server predates replica placement
// (added in 1.9.11). Such a server replicates every partition to every node and never moves a
// replica, so there is no placement to wait for.
var ErrPlacementUnsupported = errors.New("the server does not support replica placement")

// Placement implements Client.
func (c *HTTPClient) Placement(ctx context.Context, baseURL string) (*Placement, error) {
	ctx, cancel := bound(ctx, c.ReadTimeout)
	defer cancel()

	var p Placement
	code, err := c.get(ctx, baseURL+"/v1/cluster/placement", &p)
	if err != nil {
		return nil, err
	}
	switch code {
	case http.StatusOK:
		return &p, nil
	case http.StatusNotFound:
		return nil, ErrPlacementUnsupported
	default:
		return nil, fmt.Errorf("placement query returned HTTP %d", code)
	}
}

// Metrics implements Client.
func (c *HTTPClient) Metrics(ctx context.Context, baseURL string) (*Metrics, error) {
	ctx, cancel := bound(ctx, c.ReadTimeout)
	defer cancel()

	var m Metrics
	code, err := c.get(ctx, baseURL+"/v1/dashboard/metrics", &m)
	if err != nil {
		return nil, err
	}
	if code != http.StatusOK {
		return nil, fmt.Errorf("metrics query returned HTTP %d", code)
	}
	return &m, nil
}

// backupOutcomeHeader carries the typed outcome of a refused backup request.
const backupOutcomeHeader = "X-Kahuna-Backup-Outcome"

// TakeCoordinatedBackup implements Client.
//
// No bound of its own: a full backup copies the whole store and can take minutes. The caller's
// context carries the deadline, and the server cancels the backup when the request ends.
func (c *HTTPClient) TakeCoordinatedBackup(ctx context.Context, baseURL string) (*BackupInfo, error) {
	return c.postBackup(ctx, baseURL+"/v1/backups/coordinated", nil)
}

// TakeIncrementalBackup implements Client. It is bounded by the caller's context, as above.
func (c *HTTPClient) TakeIncrementalBackup(ctx context.Context, baseURL, parentBackupID string) (*BackupInfo, error) {
	body, err := json.Marshal(map[string]string{"parentBackupId": parentBackupID})
	if err != nil {
		return nil, err
	}
	return c.postBackup(ctx, baseURL+"/v1/backups/incremental", body)
}

// postBackup sends a backup request. A 200 carries the new backup; every refusal carries a problem
// body and the outcome header, and comes back as a *BackupError.
func (c *HTTPClient) postBackup(ctx context.Context, url string, body []byte) (*BackupInfo, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, reader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		var problem struct {
			Detail  string `json:"detail"`
			Outcome string `json:"outcome"`
		}
		// A body that does not decode still leaves the status and header, which are the answer.
		_ = json.Unmarshal(raw, &problem)
		outcome := resp.Header.Get(backupOutcomeHeader)
		if outcome == "" {
			outcome = problem.Outcome
		}
		return nil, &BackupError{StatusCode: resp.StatusCode, Outcome: outcome, Detail: problem.Detail}
	}
	var info BackupInfo
	if err := json.Unmarshal(raw, &info); err != nil {
		return nil, fmt.Errorf("decoding backup response: %w", err)
	}
	if info.BackupID == "" {
		return nil, fmt.Errorf("backup response carries no backup id")
	}
	return &info, nil
}

// Leave implements Client.
//
// The server answers every documented outcome with a body — 200 committed or already-gone, 409 the
// permanent last-voter refusal, 503 not-initialized, no-leader or another node mid-drain, 504
// unresolved before the deadline — so the status code is not consulted here beyond decoding. The
// caller branches on Outcome, which carries the distinction that matters.
//
// Every outcome the operator does not name is retryable on the server's own classification, so an
// unrecognized one holds the scale and is tried again. That covers DrainTimedOut, which resumes
// the drain rather than restarting it: replicas already moved stay moved.
func (c *HTTPClient) Leave(ctx context.Context, baseURL string) (*LeaveResult, error) {
	ctx, cancel := bound(ctx, c.LeaveTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/cluster/leave", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	var out LeaveResult
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("decoding leave response (status %d): %w", resp.StatusCode, err)
	}
	if out.Outcome == "" {
		return nil, fmt.Errorf("leave returned HTTP %d with no outcome", resp.StatusCode)
	}
	return &out, nil
}
