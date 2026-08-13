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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	kahunav1alpha1 "github.com/kahunakv/k8s-operator/api/v1alpha1"
)

func testCluster(replicas int32) *kahunav1alpha1.KahunaCluster {
	return &kahunav1alpha1.KahunaCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "kahuna", Namespace: "kv"},
		Spec: kahunav1alpha1.KahunaClusterSpec{
			Replicas:   ptr.To(replicas),
			Image:      "kahunakv/kahuna:latest",
			Partitions: ptr.To(int32(3)),
			Storage: kahunav1alpha1.StorageSpec{
				Backend: kahunav1alpha1.StorageBackendRocksDB,
				Data:    kahunav1alpha1.VolumeSpec{Size: resource.MustParse("10Gi")},
			},
			TLS: kahunav1alpha1.TLSSpec{
				SecretRef:          &corev1.LocalObjectReference{Name: "kahuna-tls"},
				InsecureSkipVerify: ptr.To(true),
			},
		},
	}
}

// runEntrypoint executes the generated script with the real Kahuna binary replaced by a stub
// that records its argv. This exercises the script's own logic — ordinal parsing, peer-list
// construction, join-mode selection — rather than asserting on the rendered text.
func runEntrypoint(t *testing.T, cluster *kahunav1alpha1.KahunaCluster, bootstrapSize int32, pod string, seedDataDir bool) []string {
	t.Helper()

	dir := t.TempDir()
	script := filepath.Join(dir, "entrypoint.sh")
	if err := os.WriteFile(script, []byte(entrypointScript(cluster)), 0o755); err != nil {
		t.Fatal(err)
	}

	// The topology the pod reads at startup, written where the rewritten script expects it.
	topology := filepath.Join(dir, "topology.env")
	if err := os.WriteFile(topology, []byte(topologyEnv(cluster, bootstrapSize)), 0o644); err != nil {
		t.Fatal(err)
	}

	// The script's storage paths are absolute, so redirect them into the temp dir with a stub
	// filesystem root. Simplest reliable approach: rewrite the two absolute prefixes.
	raw, err := os.ReadFile(script)
	if err != nil {
		t.Fatal(err)
	}
	rewritten := strings.ReplaceAll(string(raw), "'"+storageMountPath+"/", "'"+dir+"/storage/")
	rewritten = strings.ReplaceAll(rewritten, "'"+topologyPath+"'", "'"+topology+"'")
	// Replace the exec of the real server with one that prints its arguments and exits.
	rewritten = strings.ReplaceAll(rewritten, "exec dotnet "+serverDLLPath, "exec printf '%s\\n'")
	if err := os.WriteFile(script, []byte(rewritten), 0o755); err != nil {
		t.Fatal(err)
	}

	if seedDataDir {
		if err := os.MkdirAll(filepath.Join(dir, "storage", "data"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "storage", "data", "CURRENT"), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	cmd := exec.Command("/bin/bash", script)
	cmd.Env = append(os.Environ(), "POD_NAME="+pod)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("entrypoint failed: %v\noutput:\n%s\nscript:\n%s", err, out, rewritten)
	}

	var args []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "+ ") {
			args = append(args, line)
		}
	}
	return args
}

func TestEntrypointIsValidBash(t *testing.T) {
	script := entrypointScript(testCluster(3))
	cmd := exec.Command("/bin/bash", "-n")
	cmd.Stdin = strings.NewReader(script)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generated script is not valid bash: %v\n%s\n---\n%s", err, out, script)
	}
}

// argValues returns every value following the named flag, up to the next flag.
func argValues(args []string, flag string) []string {
	for i, a := range args {
		if a != flag {
			continue
		}
		var vals []string
		for _, v := range args[i+1:] {
			if strings.HasPrefix(v, "--") {
				break
			}
			vals = append(vals, v)
		}
		return vals
	}
	return nil
}

func hasFlag(args []string, flag string) bool {
	for _, a := range args {
		if a == flag {
			return true
		}
	}
	return false
}

func TestEntrypointDerivesNodeIdentityFromOrdinal(t *testing.T) {
	args := runEntrypoint(t, testCluster(3), 3, "kahuna-1", false)

	if got := argValues(args, "--raft-nodename"); len(got) != 1 || got[0] != "kahuna-1" {
		t.Errorf("--raft-nodename = %v, want [kahuna-1]", got)
	}
	// Ordinal 1 must map to node id 2: id 0 is indistinguishable from an unset flag.
	if got := argValues(args, "--raft-nodeid"); len(got) != 1 || got[0] != "2" {
		t.Errorf("--raft-nodeid = %v, want [2]", got)
	}
	want := "kahuna-1.kahuna-peer.kv.svc.cluster.local"
	if got := argValues(args, "--raft-host"); len(got) != 1 || got[0] != want {
		t.Errorf("--raft-host = %v, want [%s]", got, want)
	}
}

// The peer list must exclude the node itself: before the roster is seeded Kommander assigns the
// discovery list straight to its peer set with no self-filter, so a self-entry inflates quorum.
func TestEntrypointExcludesSelfFromInitialCluster(t *testing.T) {
	args := runEntrypoint(t, testCluster(3), 3, "kahuna-1", false)

	got := argValues(args, "--initial-cluster")
	want := []string{
		"kahuna-0.kahuna-peer.kv.svc.cluster.local:8082",
		"kahuna-2.kahuna-peer.kv.svc.cluster.local:8082",
	}
	if len(got) != len(want) {
		t.Fatalf("--initial-cluster = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("--initial-cluster = %v, want %v", got, want)
		}
	}
}

// A single-node cluster gets no --initial-cluster at all, which is how the server selects its
// standalone mode.
func TestEntrypointSingleNodeOmitsInitialCluster(t *testing.T) {
	args := runEntrypoint(t, testCluster(1), 1, "kahuna-0", false)
	if got := argValues(args, "--initial-cluster"); got != nil {
		t.Errorf("--initial-cluster = %v, want absent for a single-node cluster", got)
	}
}

func TestEntrypointJoinMode(t *testing.T) {
	tests := []struct {
		name          string
		replicas      int32
		bootstrapSize int32
		pod           string
		hasData       bool
		wantJoin      bool
	}{
		// A founding node forms the cluster through static discovery. --join-existing here could
		// fork it into a second roster.
		{"founding node, greenfield", 3, 3, "kahuna-0", false, false},
		// Still a founding node even after the cluster grew, so it must never switch modes.
		{"founding node after scale up", 5, 3, "kahuna-1", false, false},
		// Added after the cluster was running: it must enter as a Learner via the seeds.
		{"added node, empty volume", 5, 3, "kahuna-4", false, true},
		// Already a member: it replays its WAL and rejoins its partitions, so asking it to
		// re-admit itself is wrong.
		{"added node, restart with data", 5, 3, "kahuna-4", true, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			args := runEntrypoint(t, testCluster(tc.replicas), tc.bootstrapSize, tc.pod, tc.hasData)
			if got := hasFlag(args, "--join-existing"); got != tc.wantJoin {
				t.Errorf("--join-existing present = %v, want %v (args: %v)", got, tc.wantJoin, args)
			}
		})
	}
}

func TestEntrypointRejectsUnparseablePodName(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "entrypoint.sh")
	if err := os.WriteFile(script, []byte(entrypointScript(testCluster(3))), 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("/bin/bash", script)
	cmd.Env = append(os.Environ(), "POD_NAME=not-an-ordinal")
	if out, err := cmd.CombinedOutput(); err == nil {
		t.Fatalf("expected failure for an unparseable pod name, got success:\n%s", out)
	}
}

func TestStaticArgsOmitUnsetTuningFlags(t *testing.T) {
	args := staticArgs(testCluster(3))
	for _, flag := range []string{"--raft-suspicion-timeout", "--locks-workers", "--max-concurrent-sessions"} {
		if hasFlag(args, flag) {
			t.Errorf("%s emitted although unset; the server default should apply instead", flag)
		}
	}
	// Presence-only booleans are emitted only when they change behaviour.
	if !hasFlag(args, "--raft-allow-insecure-certificate-validation") {
		t.Error("--raft-allow-insecure-certificate-validation missing although insecureSkipVerify is true")
	}
	if hasFlag(args, "--disable-wal-sync-writes") {
		t.Error("--disable-wal-sync-writes emitted although syncWrites was left at its default")
	}
}

func TestStaticArgsEmitSetTuningFlags(t *testing.T) {
	cluster := testCluster(3)
	cluster.Spec.Raft = &kahunav1alpha1.RaftSpec{SuspicionTimeoutMs: ptr.To(int32(9000))}
	cluster.Spec.Storage.SyncWrites = ptr.To(false)
	cluster.Spec.ExtraArgs = []string{"--max-concurrent-sessions", "1"}

	args := staticArgs(cluster)
	if got := argValues(args, "--raft-suspicion-timeout"); len(got) != 1 || got[0] != "9000" {
		t.Errorf("--raft-suspicion-timeout = %v, want [9000]", got)
	}
	if !hasFlag(args, "--disable-wal-sync-writes") {
		t.Error("--disable-wal-sync-writes missing although syncWrites is false")
	}
	if got := argValues(args, "--max-concurrent-sessions"); len(got) != 1 || got[0] != "1" {
		t.Errorf("extraArgs not appended: %v", args)
	}
}

// The Raft port must also be bound as an HTTPS port: inter-node gRPC rides the TLS listener.
func TestStaticArgsBindRaftPortAsHTTPS(t *testing.T) {
	args := staticArgs(testCluster(3))
	got := argValues(args, "--https-ports")
	if len(got) != 2 || got[0] != "2071" || got[1] != "8082" {
		t.Errorf("--https-ports = %v, want [2071 8082]", got)
	}
}

// Scaling must NOT change the pod template. Topology is read from a mounted file at startup, so
// growing a cluster adds a node instead of first restarting every existing one.
func TestConfigHashIsStableAcrossScale(t *testing.T) {
	a := configHash(entrypointScript(testCluster(3)))
	b := configHash(entrypointScript(testCluster(5)))
	if a != b {
		t.Error("config hash changed with replica count; a scale would roll every existing node")
	}
}

// A real configuration change must still roll the cluster — that is the whole point of the hash.
func TestConfigHashChangesWithConfiguration(t *testing.T) {
	base := configHash(entrypointScript(testCluster(3)))

	tuned := testCluster(3)
	tuned.Spec.Raft = &kahunav1alpha1.RaftSpec{SuspicionTimeoutMs: ptr.To(int32(9000))}
	if configHash(entrypointScript(tuned)) == base {
		t.Error("config hash did not change when a tuning flag changed")
	}

	extra := testCluster(3)
	extra.Spec.ExtraArgs = []string{"--max-concurrent-sessions", "1"}
	if configHash(entrypointScript(extra)) == base {
		t.Error("config hash did not change when extraArgs changed")
	}
}

// The topology file is what carries the replica count to the pod.
func TestTopologyEnvCarriesReplicasAndBootstrapSize(t *testing.T) {
	got := topologyEnv(testCluster(5), 3)
	for _, want := range []string{"REPLICAS=5", "BOOTSTRAP_SIZE=3"} {
		if !strings.Contains(got, want) {
			t.Errorf("topology.env missing %q:\n%s", want, got)
		}
	}
}
