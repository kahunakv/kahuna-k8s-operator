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

// Client reads cluster state from a Kahuna node.
type Client interface {
	// Membership returns the committed roster as seen by the node at baseURL.
	Membership(ctx context.Context, baseURL string) (*Membership, error)
	// Health returns the node's readiness. A 503 is a valid answer, not an error: it means the
	// node is up but cannot serve.
	Health(ctx context.Context, baseURL string) (*Health, error)
}

// HTTPClient talks to the plaintext client port. The operator deliberately uses HTTP rather than
// the TLS port: the cluster's keystore is typically self-signed and scoped to in-cluster peer
// names, so requiring the operator to trust it would add a failure mode for a read-only status
// query that never leaves the cluster network.
type HTTPClient struct {
	HTTP *http.Client
}

// NewHTTPClient returns a client with a bounded timeout. Status reads must never wedge a
// reconcile, so the timeout is short and failures are treated as "unknown", not fatal.
func NewHTTPClient(timeout time.Duration) *HTTPClient {
	return &HTTPClient{HTTP: &http.Client{Timeout: timeout}}
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
