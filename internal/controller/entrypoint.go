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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	kahunav1alpha1 "github.com/kahunakv/k8s-operator/api/v1alpha1"
)

// serverDLLPath is where every published Kahuna image places the server assembly. The operator
// overrides the image's own entrypoint and invokes this directly, which is what lets the stock
// single-node image serve as a cluster member without modification.
const serverDLLPath = "/app/Kahuna.Server.dll"

// shellQuote renders s as a single-quoted POSIX shell word.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// flagArgs appends a flag and its value when v is non-nil. Every tuning field is a pointer
// precisely so that an unset field emits nothing and the server's own default applies —
// restating upstream defaults here would fork them the next time one changes.
func flagArgs(args []string, flag string, v *int32) []string {
	if v == nil {
		return args
	}
	return append(args, flag, fmt.Sprintf("%d", *v))
}

// boolFlag appends a presence-only flag when v is non-nil and matches want. Kahuna's boolean
// options take no value: the flag's presence is the value.
func boolFlag(args []string, flag string, v *bool, want bool) []string {
	if v == nil || *v != want {
		return args
	}
	return append(args, flag)
}

// staticArgs builds the portion of the command line that is identical on every node. The
// per-node identity flags (--raft-nodename/-nodeid/-host, --initial-cluster, --join-existing)
// are computed by the startup script from the pod's own ordinal, because a StatefulSet has a
// single pod template and cannot vary them per pod.
func staticArgs(cluster *kahunav1alpha1.KahunaCluster) []string {
	p := ports(cluster)
	s := cluster.Spec.Storage

	args := []string{
		// Both plaintext ports are bound on one flag; CommandLineParser collects the repeated
		// values into a list.
		"--http-ports", fmt.Sprintf("%d", p.HTTP), fmt.Sprintf("%d", p.InternalHTTP),
		// The Raft port must be one of the HTTPS ports: inter-node gRPC rides the TLS listener.
		"--https-ports", fmt.Sprintf("%d", p.HTTPS), fmt.Sprintf("%d", p.Raft),
		"--https-certificate", tlsMountPath + "/" + keystoreKey(cluster),
	}

	backend := string(s.Backend)
	if backend == "" {
		backend = string(kahunav1alpha1.StorageBackendRocksDB)
	}
	walBackend := string(s.WALBackend)
	if walBackend == "" {
		walBackend = string(kahunav1alpha1.StorageBackendRocksDB)
	}
	revision := s.Revision
	if revision == "" {
		revision = "v1"
	}
	walRevision := s.WALRevision
	if walRevision == "" {
		walRevision = "v1"
	}

	args = append(args,
		"--storage", backend,
		"--storage-path", dataPath,
		"--storage-revision", revision,
		"--wal-storage", walBackend,
		"--wal-path", walPath,
		"--wal-revision", walRevision,
	)

	args = boolFlag(args, "--disable-wal-sync-writes", s.SyncWrites, false)
	args = boolFlag(args, "--disable-rocksdb-direct-reads", s.DirectReads, false)
	args = boolFlag(args, "--rocksdb-shared-memory", s.SharedMemory, true)
	args = flagArgs(args, "--rocksdb-shared-memory-budget-mb", s.SharedMemoryBudgetMB)
	args = flagArgs(args, "--rocksdb-shared-memtable-budget-mb", s.SharedMemtableBudgetMB)

	partitions := int32(3)
	if cluster.Spec.Partitions != nil {
		partitions = *cluster.Spec.Partitions
	}
	args = append(args, "--initial-cluster-partitions", fmt.Sprintf("%d", partitions))

	args = boolFlag(args, "--raft-allow-insecure-certificate-validation",
		cluster.Spec.TLS.InsecureSkipVerify, true)

	if r := cluster.Spec.Raft; r != nil {
		args = flagArgs(args, "--raft-heartbeat-interval", r.HeartbeatIntervalMs)
		args = flagArgs(args, "--raft-voting-timeout", r.VotingTimeoutMs)
		args = flagArgs(args, "--raft-start-election-timeout", r.StartElectionTimeoutMs)
		args = flagArgs(args, "--raft-end-election-timeout", r.EndElectionTimeoutMs)
		args = boolFlag(args, "--raft-enable-check-quorum", r.EnableCheckQuorum, true)
		args = flagArgs(args, "--raft-gossip-interval", r.GossipIntervalMs)
		args = flagArgs(args, "--raft-gossip-fanout", r.GossipFanout)
		args = flagArgs(args, "--raft-ping-timeout", r.PingTimeoutMs)
		args = flagArgs(args, "--raft-indirect-ping-fanout", r.IndirectPingFanout)
		args = flagArgs(args, "--raft-suspicion-timeout", r.SuspicionTimeoutMs)
		args = flagArgs(args, "--raft-dead-member-eviction-grace", r.DeadMemberEvictionGraceMs)
		args = flagArgs(args, "--raft-backfill-threshold", r.BackfillThreshold)
		args = flagArgs(args, "--raft-max-backfill-entries-per-round", r.MaxBackfillEntriesPerRound)
		args = flagArgs(args, "--raft-learner-promotion-lag", r.LearnerPromotionLag)
		args = flagArgs(args, "--raft-learner-promotion-stable-window", r.LearnerPromotionStableWindowMs)
	}

	if t := cluster.Spec.Transactions; t != nil {
		args = flagArgs(args, "--default-transaction-timeout", t.DefaultTimeoutMs)
		args = flagArgs(args, "--default-admission-wait", t.DefaultAdmissionWaitMs)
		args = flagArgs(args, "--max-admission-wait", t.MaxAdmissionWaitMs)
		args = flagArgs(args, "--max-concurrent-transactions", t.MaxConcurrentTransactions)
		args = flagArgs(args, "--max-concurrent-sessions", t.MaxConcurrentSessions)
	}

	if w := cluster.Spec.Workers; w != nil {
		args = flagArgs(args, "--locks-workers", w.Locks)
		args = flagArgs(args, "--keyvalue-workers", w.KeyValue)
		args = flagArgs(args, "--sequencer-workers", w.Sequencer)
		args = flagArgs(args, "--background-writer-workers", w.BackgroundWriter)
	}

	// extraArgs go last so a user override wins over anything generated above.
	return append(args, cluster.Spec.ExtraArgs...)
}

// keystoreKey is the Secret key holding the PKCS#12 blob.
func keystoreKey(cluster *kahunav1alpha1.KahunaCluster) string {
	if cluster.Spec.TLS.CertManager != nil {
		// cert-manager writes its PKCS#12 output under this fixed key.
		return certManagerKeystoreKey
	}
	if cluster.Spec.TLS.KeystoreKey != "" {
		return cluster.Spec.TLS.KeystoreKey
	}
	return "certificate.pfx"
}

// topologyEnv renders the cluster topology the startup script reads at boot.
//
// It is deliberately NOT part of the script, and therefore not part of the pod template hash.
// Topology changes on every scale, and baking it into the template would mean growing a cluster
// from 3 to 5 first restarts all three existing nodes — disruption with no purpose, since after
// the roster is committed Kommander derives its peer set from the roster and consults the seed
// list only before the first seed. Static configuration stays in the script, where a change
// *should* roll the cluster.
func topologyEnv(cluster *kahunav1alpha1.KahunaCluster, bootstrapSize int32) string {
	return fmt.Sprintf(`# Generated by kahuna-operator. Read at startup; changes need no pod restart.
REPLICAS=%d
BOOTSTRAP_SIZE=%d
`, desiredReplicas(cluster), bootstrapSize)
}

// entrypointScript renders the startup script mounted into every pod.
//
// It exists because a StatefulSet has one pod template but each Kahuna node needs a distinct
// --raft-nodeid, --raft-nodename and --raft-host, plus an --initial-cluster list that excludes
// itself. The script derives all of them from the pod's own ordinal at startup.
func entrypointScript(cluster *kahunav1alpha1.KahunaCluster) string {
	var b strings.Builder

	quoted := make([]string, 0, len(staticArgs(cluster)))
	for _, a := range staticArgs(cluster) {
		quoted = append(quoted, shellQuote(a))
	}

	fmt.Fprintf(&b, `#!/usr/bin/env bash
# Generated by kahuna-operator for %s/%s. Do not edit — the next reconcile overwrites it.
set -euo pipefail

if [[ -z "${POD_NAME:-}" ]]; then
  echo "kahuna-operator: POD_NAME is unset; the downward API env var is missing" >&2
  exit 1
fi

# The StatefulSet ordinal is the node's durable identity: pod names are stable across restarts,
# so the Raft node id derived from it is too.
ORDINAL="${POD_NAME##*-}"
if ! [[ "$ORDINAL" =~ ^[0-9]+$ ]]; then
  echo "kahuna-operator: cannot derive an ordinal from POD_NAME=$POD_NAME" >&2
  exit 1
fi

STS_NAME=%s
PEER_DOMAIN=%s
RAFT_PORT=%d

# Topology lives in a separate file rather than in this script so that scaling the cluster does
# not change the pod template and therefore does not restart every existing node.
TOPOLOGY=%s
if [[ ! -f "$TOPOLOGY" ]]; then
  echo "kahuna-operator: $TOPOLOGY is missing" >&2
  exit 1
fi
# shellcheck source=/dev/null
source "$TOPOLOGY"

# +1 because --raft-nodeid defaults to 0, making a node given 0 indistinguishable from one that
# was never configured.
NODE_ID=$((ORDINAL + 1))
RAFT_HOST="${POD_NAME}.${PEER_DOMAIN}"

# --initial-cluster lists this node's PEERS and must exclude the node itself. Before the roster
# is seeded, Kommander assigns the discovery list straight to its peer set without filtering
# self, so a self-entry would inflate quorum math during bootstrap.
PEERS=()
for ((i = 0; i < REPLICAS; i++)); do
  if [[ "$i" -eq "$ORDINAL" ]]; then continue; fi
  PEERS+=("${STS_NAME}-${i}.${PEER_DOMAIN}:${RAFT_PORT}")
done

# An empty peer list is not an error: it is how a single-node cluster asks the server for its
# standalone mode. It is, however, the wrong thing to hand a multi-node member, so a node that
# expects peers and has none fails rather than silently starting a second one-node cluster.
CLUSTER_ARGS=()
if [[ ${#PEERS[@]} -gt 0 ]]; then
  CLUSTER_ARGS+=(--initial-cluster "${PEERS[@]}")
elif [[ "$REPLICAS" -ne 1 ]]; then
  echo "kahuna-operator: empty --initial-cluster with REPLICAS=$REPLICAS would start a standalone node" >&2
  exit 1
fi

# Join mode. BOOTSTRAP_SIZE is the size the cluster was first created at and never changes, so
# an ordinal at or above it is by definition a node added after the cluster was already running.
#
#   * Ordinal below BOOTSTRAP_SIZE -> one of the founding nodes. Static discovery seeds the
#                      roster; --join-existing here could fork it into a second roster.
#   * At or above, with data on disk -> already a member. It replays its WAL and rejoins its
#                      partitions; asking it to re-admit itself is wrong.
#   * At or above, empty -> a new node arriving at a running cluster (scale-up, or a node whose
#                      volume was lost). It must enter as a Learner via the seeds.
JOIN_ARGS=()
if [[ "$ORDINAL" -ge "$BOOTSTRAP_SIZE" && -z "$(ls -A %s 2>/dev/null)" ]]; then
  JOIN_ARGS+=(--join-existing)
fi

mkdir -p %s %s

# The keystore password would be visible in this container's argv. Kahuna reads it only from the
# command line, so there is no way to avoid that; it is not exposed outside the pod.
TLS_ARGS=()
if [[ -f %s ]]; then
  TLS_ARGS+=(--https-certificate-password "$(cat %s)")
fi

STATIC_ARGS=(%s)

# ${ARR[@]+"${ARR[@]}"} rather than "${ARR[@]}": under "set -u" an empty array is an unbound
# variable in bash before 4.4, and a single-node cluster leaves CLUSTER_ARGS empty.
set -x
exec dotnet %s \
  --raft-nodename "$POD_NAME" \
  --raft-nodeid "$NODE_ID" \
  --raft-host "$RAFT_HOST" \
  --raft-port "$RAFT_PORT" \
  ${CLUSTER_ARGS[@]+"${CLUSTER_ARGS[@]}"} \
  ${JOIN_ARGS[@]+"${JOIN_ARGS[@]}"} \
  ${TLS_ARGS[@]+"${TLS_ARGS[@]}"} \
  ${STATIC_ARGS[@]+"${STATIC_ARGS[@]}"}
`,
		cluster.Namespace, cluster.Name,
		shellQuote(stsName(cluster)),
		shellQuote(peerDomain(cluster)),
		ports(cluster).Raft,
		shellQuote(topologyPath),
		shellQuote(dataPath),
		shellQuote(dataPath), shellQuote(walPath),
		shellQuote(tlsPasswordMountPath+"/password"), shellQuote(tlsPasswordMountPath+"/password"),
		strings.Join(quoted, " "),
		serverDLLPath,
	)

	return b.String()
}

// configHash digests the rendered script. It rides on the pod template as an annotation so that
// editing the ConfigMap produces a new StatefulSet revision — without it a config change would
// sit in the ConfigMap and never reach a running pod.
func configHash(script string) string {
	sum := sha256.Sum256([]byte(script))
	return hex.EncodeToString(sum[:])[:32]
}
