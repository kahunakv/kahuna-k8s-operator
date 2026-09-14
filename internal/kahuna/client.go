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
	"context"
	"encoding/json"
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
	// ReadTimeout bounds Membership and Health. It stays short: a status read must never wedge a
	// reconcile, and an unreachable node is reported as an unknown roster rather than an error.
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
