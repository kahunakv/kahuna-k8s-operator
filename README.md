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
| **Scales** | One node at a time, in both directions, each step gated on the roster and on the replica placement having settled — never on what Kubernetes merely believes. Departing nodes are decommissioned through the cluster's leave API before their pods are removed. |
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

**A step is complete when the data has followed it, not when the roster has.** Before any further
membership change the operator reads `GET /v1/cluster/placement` and holds while a replica is still
in a transitional role or still assigned to a node that left. A scale-down that went ahead during
those repairs would take a second copy from partitions that have already lost one. A server that
predates placement (before 1.9.11) never moves a replica and is scaled on the roster alone; an
unreadable placement holds, like an unreadable roster.

**The KahunaCluster is read uncached.** An autoscaled cluster keeps its chosen size in
`status.autoscaling`, which the operator itself writes, and the informer cache can lag that write
by a reconcile. A pass that reads the previous size from the cache would regrow the StatefulSet
the last pass just shrank, bringing back the node it had decommissioned. The reconciler therefore
fetches the cluster through the manager's API reader; a stale StatefulSet read is harmless, since
an update carrying an old `resourceVersion` is rejected.

**`replicas: 2` is rejected, and 1 ↔ many is rejected.** A two-Voter cluster has a quorum of 2, so it
tolerates no failures while costing twice as much as a single node. A single node runs Kahuna in
standalone mode, which uses a different quorum model and on-disk identity, so it cannot be grown into
a real cluster in place. The standalone quorum is the node plus two phantom witnesses that the server
lists in its roster as Voters (`embedded-witness-N:0`); the operator drops them when it reads
membership, so a single-node cluster converges on one voter.

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

**Any founding size works, and so does turning the factor on later.** A partition created while
the cluster has no more nodes than the factor starts fully replicated (no replica set), and so does
every partition of a cluster that had no factor. From Kahuna 1.12.3 (Kommander 1.10.12) the
rebalancer converts such a partition: it first commits every current voter as the replica set,
which moves no data, and then trims that set down to the factor one replica at a time. Older
servers never convert one. The operator checks `GET /v1/cluster/placement` before every scale-up
and does not grow past 3 nodes while any data partition is still fully replicated, reporting why in
the `AutoscalingLimited` condition. On 1.12.3 that limit clears by itself a few rebalancer passes
after the cluster starts with the factor; on an older server it holds until the cluster is
recreated with more nodes than the factor.

**Signals are never guessed.** The autoscaler holds — and says so in `AutoscalingLimited` — when any
ready node's metrics cannot be read (including `--disable-dashboard` in `extraArgs`), and it never
scales down on a partial view, since the missing node may be the leader carrying the load.
`kahuna.kv.write.admitted` counts direct writes only; transactional writes show up through the queue
depth signal.

## Backups

```yaml
spec:
  backup:
    schedule: "0 2 * * *"            # full backups, cron, UTC unless CRON_TZ=<zone> is given
    incrementalSchedule: "*/15 * * * *"  # optional
    volume:
      claim:                         # or claimName: <existing ReadWriteMany claim>
        size: 100Gi
        storageClassName: filestore  # any class that supports ReadWriteMany
    retention:
      maxChains: 7
      # maxAge: 168h
      # maxBytes: 500Gi
    # clusterID: kv/kahuna           # defaults to <namespace>/<name>
    # macKeySecretRef: {name: my-key, key: key}
    # timeout: 1h
    # suspend: false
```

Leave `backup` out and the cluster takes no backups. With it, every node mounts one shared volume
and runs with `--pitr-backup-dir /backup/<name>`, so all nodes read one backup catalog. A shared
catalog is what lets a node find an incremental's parent and list every backup; a volume per node
would give each node only the backups it took itself.

The operator drives the schedule. A full backup is a coordinated backup
(`POST /v1/backups/coordinated`): the server takes it at one cut time that is the same for every
partition. Only the meta-partition leader accepts it, so the operator sends it to each ready node in
turn until one does. An incremental (`POST /v1/backups/incremental`) adds the log written since the
latest backup. The first full backup runs as soon as the cluster is steady after `backup` is added;
after that each kind runs on its own schedule, and missed runs collapse into one, as with a CronJob.

- **Backups run in the background.** A full backup can take many minutes, so the reconcile starts it
  and collects the result on a later pass; scaling and rollouts are not held for it.
- **A backup waits for a steady cluster:** bootstrap complete, roster converged, and every pod ready
  on the current configuration and image. Turning backups on rolls the cluster, and the first
  backup waits for the rollout to finish.
- **Status.** `status.backup` records the last backup id and type, the schedule anchors, and the
  failures. The `BackupFailing` condition is true while the latest attempt failed. A failed backup
  stays due and is retried after a backoff (1 minute, doubling to 30). If an incremental's parent is
  gone from the catalog, a new chain starts with a full backup at once.
- **A steady cluster can still have no backup coordinator.** Right after bootstrap no node leads the
  meta partition yet, so every node can refuse the first backup (`NotBackupCoordinator`, or
  `CutUnverified` from the node that just took the lead). A topology change during the backup
  (`TopologyChanged`) and a node's share of the partitions that did not arrive
  (`PartitionCaptureFailed`) also clear on retry. When every refusal is of these kinds, the
  backup is retried after 15 seconds and is not counted as a failure: `BackupFailing` stays false
  with reason `BackupWaiting`, and `status.backup.unclaimedSince` shows since when. Only if no node
  takes it for 5 minutes is it counted as a failure, with the usual backoff.
- **Retention** maps to `--backup-retention-*`. The server deletes whole chains and always keeps the
  newest one. The periodic GC pass is turned off (`--backup-gc-interval 0`): its orphan sweep would
  delete a backup that another node has in progress on the shared volume. GC still runs after each
  backup, on the node that took it.
- **The volume must be ReadWriteMany when the cluster has more than one node.** The operator's
  `<name>-backup` claim asks for ReadWriteMany, or for ReadWriteOnce on a single-node cluster, where
  nothing shares it. Most default StorageClasses are block storage and cannot give ReadWriteMany:
  Kind's `local-path`, EBS, Persistent Disk and Azure Disk among them. Use a file-storage class
  (NFS, Filestore, EFS, Azure Files, CephFS) or name your own claim in `claimName`. Every pod mounts
  the claim, so while it is unbound no pod starts and the cluster stays in bootstrap. The
  `BackupVolumeReady` condition then names the cause, for example the provisioner's
  `ProvisioningFailed` event. Until the claim binds, the operator replaces it when
  `volume.claim` changes, so a corrected `storageClassName` takes effect without a manual delete.
  After it binds, the operator does not change it.
- **The claim and the signing key outlive the cluster.** Neither the `<name>-backup` claim nor the
  generated `<name>-backup-mac` Secret has an owner reference, so deleting the KahunaCluster or
  removing `backup` keeps both. The server refuses to restore or seed from a signed backup without
  its key.

**Under a replication factor, backups need Kahuna 1.12.5 or later.** When `replication.factor` is
less than the most nodes the cluster can have (`autoscaling.maxReplicas`, or `replicas`), no node
hosts every partition. Kahuna 1.12.5 and later compose the coordinated backup from each node's
share of the partitions, captured at one cut, so the backup covers every partition. An older
server's backup holds only the partitions the node taking it hosts, and restore and seeding refuse
it. So on such a cluster the operator takes no backup when `image` has a release tag before 1.12.5:
`BackupFailing` is `True` with reason `UnsupportedImage`. The operator cannot read the version of
an image without a release tag (`latest`, a branch build, a digest alone), and takes backups with
it. A backup also waits until every pod runs the current `image`, because during an image rollout
an older node cannot capture its share. If one node's share does not arrive, the server publishes
nothing and answers `PartitionCaptureFailed`. The operator retries it soon, as above.

**On a single-node Kind cluster,** whose only class is `local-path`, a hostPath PersistentVolume
gives a working ReadWriteMany claim. It is correct there only because every pod runs on the one
node:

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: kahuna-backup
spec:
  capacity: {storage: 5Gi}
  accessModes: [ReadWriteMany]
  storageClassName: manual
  persistentVolumeReclaimPolicy: Retain
  hostPath: {path: /var/local/kahuna-backup, type: DirectoryOrCreate}
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: kahuna-backup
  namespace: kahuna
spec:
  accessModes: [ReadWriteMany]
  storageClassName: manual
  volumeName: kahuna-backup
  resources: {requests: {storage: 5Gi}}
```

Then set `spec.backup.volume.claimName: kahuna-backup`. The e2e suite takes its backups this way.

**Restore is manual for now.** See the
[server backup guide](https://github.com/kahunakv/kahuna/blob/main/docs/backups-and-point-in-time-recovery-guide.md).
The backup endpoints have no authentication of their own; anything that can reach a node's
plaintext port can start a backup or list the catalog.

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

`spec.image` therefore defaults to an exact tag, `kahunakv/kahuna:1.12.8`, rather than to `latest`.
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
  the leader ships it a snapshot plus the log tail. Kahuna 1.12.3 can seed a node itself
  (`--pitr-bootstrap-from latest` copies and verifies the full backup's checkpoint, replays the
  chain, then joins), but only from a backup that covers every partition. Under a replication
  factor, Kahuna 1.12.5 and later take such a backup, but the operator does not seed new nodes yet.
- **Backups go to a shared volume only.** The stock server registers no GCS backup provider (kahuna
  task `19b5142e`), so a bucket cannot be a backup target yet.
- **`nodeId` reads 0 for founding roster members.** Kommander seeds the initial roster from
  discovery, which yields endpoints but not node ids, so peers are recorded with a provisional 0
  that a later Join RPC would replace — and a founding node never sends one
  (`RaftSystemCoordinator.cs:469`). This is intentional upstream and harmless: the roster keys on
  endpoint, and so does this operator. It only shows up as a cosmetic 0 in
  `kahuna.control --cluster-members` and `status.members[].nodeID`.

## Not yet implemented

- Restore, as a CRD or as a field, over the existing `/v1/restore` endpoint
- Backups to GCS, and seeding scale-up nodes from them (blocked on the server, listed under *Known
  gaps*)

## Verified

`make test-e2e` runs all of the below against a real kind cluster — 9 specs, ~8m, using the
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
- Replication factor turned on for a cluster founded at 3 nodes: every data partition converts to a
  replica set, and after a scale to 4 each one holds exactly 3 Voters
- cert-manager path: `Certificate` issued with a PKCS#12 keystore and a generated password, cluster
  bootstrapped from it in under 40s with `KeystoreReady=True`
