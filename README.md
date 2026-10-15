# Kahuna Kubernetes Operator (Alpha)

A Kubernetes operator for [Kahuna](https://github.com/kahunakv/kahuna) — a distributed key/value
store, lock manager and sequencer built on Raft consensus.

A Kahuna cluster's membership is a **consensus-committed roster**, changed one node at a time. That
is why a plain StatefulSet is not enough: node identity, the peer list and the join mode are all
per-node startup flags, and adding or removing a member is a distributed transaction with rules
about quorum. This operator owns those rules.

```yaml
apiVersion: kahuna.kahunakv.io/v1alpha1
kind: KahunaCluster
metadata:
  name: kahuna
spec:
  replicas: 3
  partitions: 3
  storage:
    data:
      size: 100Gi
  tls:
    secretRef:
      name: kahuna-tls
```

```
$ kubectl get kahunacluster
NAME     DESIRED   READY   VOTERS   PHASE     AGE
kahuna   3         3       3        Running   12m
```

## What it does

| | |
|---|---|
| **Provisions** | Headless peer Service, client Service, per-node PVCs, ConfigMap, PodDisruptionBudget, StatefulSet. |
| **Bootstraps** | Brings up all founding nodes at once so they can form their first quorum, and reports `Running` only when the committed roster carries every node as a Voter. |
| **Scales** | One node at a time, in both directions, each step gated on the roster — never on what Kubernetes merely believes. Departing nodes are decommissioned through the cluster's leave API before their pods are removed. |
| **Autoscales** | Optional. Adds and removes nodes with write traffic between a minimum and a maximum, one node at a time, and never grows a cluster in a way that would put more than 3 copies on any data partition. |
| **Upgrades** | Rolling restarts one node at a time, held back automatically whenever the roster is not converged or unreadable. |
| **Reports** | `status.members` mirrors the committed roster joined to the pods serving it, plus `Available` / `Progressing` / `RosterConverged` conditions. |

## Design decisions worth knowing

These are the non-obvious ones. Each is load-bearing; changing it breaks something specific.

**The roster is the source of truth, not Kubernetes.** A `Running` pod need not be a cluster member,
and a roster entry routinely outlives its pod — that gap *is* the scale-down process. Every
membership step is gated on `GET /v1/cluster/membership`, and members are matched to pods by
endpoint string rather than by node id.

**`podManagementPolicy: Parallel` is required.** Under `OrderedReady` the StatefulSet waits for
pod-0 to become ready before creating pod-1, but pod-0 cannot become ready until a quorum forms,
which needs its peers to exist. Bootstrap would deadlock permanently.

**`publishNotReadyAddresses: true` on the peer Service is required**, for the same reason: peers
must resolve each other *before* any of them is ready.

**`--initial-cluster` must exclude the node itself.** Before the roster is seeded, Kommander assigns
the discovery list straight to its peer set with no self-filter
(`ClusterHandler.UpdateNodes`), so a self-entry inflates quorum math during bootstrap.

**Topology lives outside the pod template.** The replica count is written to a `topology.env` file
the pod reads at startup, not baked into the startup script. Only the script is hashed into the pod
template. Without this split, growing a cluster from 3 to 5 would first restart all three existing
nodes — pure disruption, because once the roster is committed Kommander derives peers from the
roster and consults the seed list only before the first seed.

**`status.bootstrapSize` is pinned at creation and never revised.** A starting node decides whether
to *form* the cluster (static discovery) or *join* it (`--join-existing`, entering as a Learner) by
comparing its ordinal against this number. Letting it drift with `spec.replicas` would make founding
nodes look like late arrivals after the first scale.

**Liveness does not use `/v1/cluster/health`.** That endpoint returns 503 for a node that is up but
not yet a serving member — a state every node passes through on restart, and one an evicted node can
sit in. Restarting the process fixes neither. Liveness asks only whether the server answers at all;
readiness carries the membership-aware check.

**Scale-down decommissions before deleting.** The departing node is asked to leave through
`POST /v1/cluster/leave`, and its pod is removed only once the cluster confirms the removal is
committed (or that it was already gone). Deleting the pod first would make a planned removal
indistinguishable from a crash and force the cluster through its failure-detector timeouts. If the
node cannot be reached, or refuses because it is the last voter, the scale **holds** rather than
falling back to deleting a live member.

**The KahunaCluster is read uncached.** An autoscaled cluster keeps its chosen size in
`status.autoscaling`, which the operator itself writes, and the informer cache can lag that write
by a reconcile. A pass that reads the previous size from the cache would regrow the StatefulSet
the last pass just shrank, bringing back the node it had decommissioned. The reconciler therefore
fetches the cluster through the manager's API reader; a stale StatefulSet read is harmless, since
an update carrying an old `resourceVersion` is rejected.

**`replicas: 2` is rejected, and 1 ↔ many is rejected.** A two-Voter cluster has a quorum of 2, so it
tolerates no failures while costing twice as much as a single node. A single node runs Kahuna in
standalone mode, which uses a different quorum model and on-disk identity, so it cannot be grown into
a real cluster in place.

## Immutable fields

Enforced by CEL rules on the CRD, so the API server rejects the edit — no webhook to install:

- `spec.partitions` — the partition map is built once at bootstrap
- `spec.storage.backend`, `.revision`, `.walBackend`, `.walRevision` — changing any makes existing
  data unreadable

## Configuration

Operationally meaningful flags get typed fields (`spec.raft`, `spec.transactions`, `spec.workers`,
`spec.storage`). Every tuning field is optional and **omitted means "do not pass the flag"**, so the
Kahuna server's own default applies rather than a copy of it that can silently fork.

Anything without a typed field goes through `spec.extraArgs`, appended verbatim after every
generated flag:

```yaml
spec:
  extraArgs: ["--max-concurrent-sessions", "1"]
```

Changing configuration rolls the cluster; changing `replicas` does not.

## Autoscaling

```yaml
spec:
  replicas: 4            # starting size only, once autoscaling is on
  replication:
    factor: 3            # at most 3 copies of each data partition (1-3)
  autoscaling:
    minReplicas: 3
    maxReplicas: 9
    targetWritesPerSecond: 2000   # direct writes per node
    targetQueueDepth: 32          # pending client proposals per node
    # scaleUpStabilizationSeconds: 60
    # scaleDownStabilizationSeconds: 300
    # tolerancePercent: 10
```

The operator polls `GET /v1/dashboard/metrics` on every ready node and turns
`kahuna.kv.write.admitted` into a write rate and `raft.executor.client_queue_depth` into a queue
depth, both averaged per node. The recommendation is the HorizontalPodAutoscaler's rule — current
size times the worst signal's ratio to its target, with a dead band — but a step is **one node**,
taken only after the recommendation has held for the whole stabilization window and only once the
previous step is complete. Complete means more than the roster having converged: the operator also
reads `GET /v1/cluster/placement` and waits until no replica move is in flight, no replica is left on
a node that departed, and every node carries a share of the data partitions. The share is waited for
at most five minutes after the step, because the server's planner stops at its own notion of even
and a cluster with fewer placed replicas than nodes leaves one empty for good; the two other
conditions are waited for as long as they hold. Removal goes through the same leave-then-delete
path as a manual scale-down.

What the autoscaler chose lives in `status.autoscaling` (`desiredReplicas`, `recommendedReplicas`,
the measured signals and the window timestamps), and `spec.replicas` is ignored after the first
pass. Removing `spec.autoscaling` hands the count back to `spec.replicas`.

**Why `replication` is required.** Without a replication factor every node holds every partition,
so a node added is one more copy of all data and one more voter on every write: more cost, no more
write capacity. With `factor: 3` the operator passes `--raft-replication-factor 3` and
`--raft-enable-placement-rebalancer`; the rebalancer is what gives a newly added node any data at all.
It also raises the rebalancer's pacing (`--raft-max-concurrent-replica-transfers 4`,
`--raft-max-replica-moves-per-pass 8`, `--raft-replica-count-deadband 0`): at the server defaults a
node added by a scale-up took a quarter of an hour to receive a handful of replicas and then stayed
under-filled for good, because every full node was within the deadband of the even share. Override
any of them through `extraArgs` if backfill competes too hard with client traffic.

**Create the cluster with more nodes than the factor.** The server places a partition only if it is
created while the cluster has more nodes than the factor, and it never converts a fully replicated
partition later. A cluster founded at 3 nodes with factor 3 therefore stays fully replicated
forever. The operator checks `GET /v1/cluster/placement` before every scale-up and refuses to grow
such a cluster past 3 nodes, reporting why in the `AutoscalingLimited` condition. Founding at 4 or
more is the workaround until the server can convert existing partitions.

**Signals are never guessed.** The autoscaler holds — and says so in `AutoscalingLimited` — when any
ready node's metrics cannot be read (including `--disable-dashboard` in `extraArgs`), and it never
scales down on a partial view, since the missing node may be the leader carrying the load.
`kahuna.kv.write.admitted` counts direct writes only; transactional writes show up through the queue
depth signal.

## TLS

TLS is not optional for a multi-node cluster: the Raft port is an HTTPS listener and inter-node gRPC
speaks `https://`, so a PKCS#12 keystore must be present. Supply one, or have cert-manager issue it.

**Bring your own keystore:**

```bash
kubectl create secret generic kahuna-tls --from-file=certificate.pfx=/path/to/certificate.pfx
```

```yaml
spec:
  tls:
    secretRef: {name: kahuna-tls}
    keystoreKey: certificate.pfx          # default
    passwordSecretRef: {name: ..., key: ...}   # omit for an unprotected keystore
```

**Or let cert-manager issue it:**

```yaml
spec:
  tls:
    certManager:
      issuerRef: {name: kahuna-selfsigned, kind: Issuer}
```

The operator creates a `Certificate` requesting a PKCS#12 keystore, generates a random password
Secret for it (once — the password is baked into the issued keystore, so it is never rewritten),
and points the server at cert-manager's fixed `keystore.p12` key. The per-pod SAN is a **wildcard**
(`*.<name>-peer.<ns>.svc.cluster.local`) rather than one name per ordinal, so scaling the cluster
does not reissue the certificate and restart every node that mounts it.

The `KeystoreReady` condition reports whether the keystore exists yet; without it pods would sit
stuck on a missing volume with no explanation.

`spec.tls.insecureSkipVerify` defaults to `true`, which is what a self-signed in-cluster keystore
needs. Set it to `false` when the issuer's CA is in the image's trust store.

## Server version

**Kahuna 1.8.1 or newer.** This is a floor, not a preference. Two things tie the operator to it:

- From 1.8.x a configured `--https-certificate` suppresses every cleartext listener unless
  `--allow-plaintext-listener` is also given. The operator always configures a keystore, so it
  always passes that flag. Both probes and the operator's own membership and leave calls use the
  plaintext port, so all three go dark without it.
- Kahuna's command-line parser rejects an option it does not define rather than ignoring it, so an
  older server fails at startup on that same flag.

`spec.image` therefore defaults to an exact tag, `kahunakv/kahuna:1.12.2`, rather than to `latest`.
A floating tag re-rolls a running cluster on a registry push, and the generated command line is
version-specific. Raise the tag deliberately, after checking the flags against that release.

## Install

```bash
helm install kahuna-operator ./charts/kahuna-operator \
  --namespace kahuna-system --create-namespace
```

See the [chart README](charts/kahuna-operator/README.md) — in particular the CRD upgrade caveat,
since Helm never upgrades files under `crds/`.

## Development

```bash
make test                                    # unit + envtest
make test-e2e                                # kind e2e (creates and deletes its own cluster)
make helm-lint                               # sync generated CRD/RBAC into the chart, then lint
make docker-build IMG=kahuna-operator:dev
kind load docker-image kahuna-operator:dev --name <cluster>
make install                                 # CRDs
make deploy IMG=kahuna-operator:dev
kubectl apply -f config/samples/kahuna_v1alpha1_kahunacluster.yaml
```

The operator reaches nodes by pod IP, so `make run` from a laptop cannot read the roster against a
kind cluster — pod IPs are not routable from the host. Deploy it in-cluster, or port-forward.

To build a Kahuna image for testing:

```bash
cd ../kahuna && docker build -f docker/Dockerfile.standalone -t kahunakv/kahuna:dev .
```

The stock single-node image works unmodified as a cluster member: the operator overrides the image's
entrypoint with its own generated startup script.

## Known gaps

- **A node rejoining after a long absence can hit the Raft compaction floor.** If the cluster has
  discarded the log the node needs, catch-up cannot complete; seed it from a recent backup first.
  See the [cluster membership operations guide](https://github.com/kahunakv/kahuna/blob/main/docs/cluster-membership-operations-guide.md), §8.
- **No Prometheus endpoint.** Kahuna exposes its metrics only as JSON on `/v1/dashboard/metrics`,
  which the autoscaler reads, so there is no ServiceMonitor to ship.
- **New nodes catch up through Raft, not from a backup.** A node added by a scale-up joins empty and
  the leader ships it a snapshot plus the log tail. Seeding it from the latest full/incremental
  backup in GCS instead needs two server changes first: the stock server registering the GCS
  backup provider, and `--pitr-bootstrap-from` downloading the full backup's checkpoint itself
  (today it assumes the checkpoint is already on disk).
- **`nodeId` reads 0 for founding roster members.** Kommander seeds the initial roster from
  discovery, which yields endpoints but not node ids, so peers are recorded with a provisional 0
  that a later Join RPC would replace — and a founding node never sends one
  (`RaftSystemCoordinator.cs:469`). This is intentional upstream and harmless: the roster keys on
  endpoint, and so does this operator. It only shows up as a cosmetic 0 in
  `kahuna.control --cluster-members` and `status.members[].nodeID`.

## Not yet implemented

- Backup/restore CRDs over the existing `/v1/backups/*` endpoints
- Scheduled coordinated backups to GCS, and seeding scale-up nodes from them (blocked on the two
  server changes listed under *Known gaps*)

## Verified

`make test-e2e` runs all of the below against a real kind cluster — 8 specs, ~4m, using the
published `kahunakv/kahuna` image. It is wired into CI on every push and pull request.

- Bootstrap to 3 Voters in ~20s, producing exactly **one** StatefulSet revision — no spurious roll
- Key/value round trip through the client Service
- Scale 3 → 5: stepped one node at a time, existing pods untouched (identical UIDs and start times),
  both new nodes started with `--join-existing`
- Scale 5 → 3: one at a time, each node decommissioned through `POST /v1/cluster/leave` before its
  pod is removed
- Rolling upgrade: one pod at a time, Voters held at 3 throughout, 0 restarts
- Data written before the scale still readable after 3 → 5 → 3
- `replicas: 2` and a `partitions` edit both rejected by the API server
- cert-manager path: `Certificate` issued with a PKCS#12 keystore and a generated password, cluster
  bootstrapped from it in under 40s with `KeystoreReady=True`
