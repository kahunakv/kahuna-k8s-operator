# Operate and Autoscale a Kahuna Cluster

The operator chooses pod counts through the committed Kahuna roster. Use the `KahunaCluster`, not the managed StatefulSet, as the control surface. The examples assume the operator is installed, the resource is named `kahuna` in namespace `kahuna`, and its keystore is configured as described in the [README](../README.md#tls).

## Found an Autoscaled Cluster with Placement

Any founding size works on Kahuna 1.12.3 or later. Give the cluster enough partitions to distribute the workload. Add this to a new cluster's spec:

```yaml
spec:
  replicas: 4
  image: kahunakv/kahuna:1.12.5
  partitions: 16
  replication:
    factor: 3
  autoscaling:
    minReplicas: 4
    maxReplicas: 9
    targetWritesPerSecond: 2000
    targetQueueDepth: 32
    scaleUpStabilizationSeconds: 60
    scaleDownStabilizationSeconds: 300
    tolerancePercent: 10
```

This is a spec fragment; the required storage and TLS fields still apply. Targets are examples to calibrate against a representative workload. A cluster founded at three nodes with factor 3, or one that adds `replication` afterward, starts with fully replicated partitions. Kahuna 1.12.3 converts them: the rebalancer first commits every current voter as the replica set, which moves no data, and then trims the set to the factor. Older servers never convert them. Before a traffic-driven growth step the controller reads placement, and any fully replicated data partition limits growth to three nodes until the conversion finishes. An unreadable placement response blocks growth too.

With placed partitions, a data partition has the configured 1–3 replicas, not one per cluster node. More nodes can spread partitions but cannot split a single hot key's write leader. Factor 3 tolerates one unavailable replica of a given data partition; increasing cluster node count does not raise that tolerance. Factor 1 has no data replica fault tolerance.

## Signals and Decisions

The controller resyncs an autoscaled cluster every 15 seconds and reads each eligible ready pod's `/v1/dashboard/metrics` over its plaintext management listener. Network policy must allow those calls; do not disable the dashboard.

- `targetWritesPerSecond` uses the average per-node delta of `kahuna.kv.write.admitted`: direct write admissions, not successful request throughput or all transactional writes.
- `targetQueueDepth` uses the average per-node sum of `raft.executor.client_queue_depth`: pending client proposals across partition executors.

At least one target is required. The largest known signal-to-target ratio drives `ceil(current desired size × ratio)` outside the tolerance band; the recommendation is then clamped to the bounds. Four nodes at twice target recommend eight, but a completed scale-up window requests only five. The controller waits for the expected roster and ready pods before considering another traffic step. A new step must earn a new stabilization window.

The default windows are 60 seconds up and 300 seconds down, with a 10% dead band. The direction must persist across observations; returning to the band or losing convergence clears the streak. This is not a historical maximum recommendation window and is not a promise that catch-up or draining finishes within the window.

The rate sampler needs two accepted readings at least ten seconds apart. A counter/time reset or a new pod UID resets its baseline. Operator restarts lose rate samples but retain desired replicas and window timestamps in resource status. An unknown configured signal prevents traffic-driven shrink; another known signal can still justify growth. A failed metrics request holds traffic decisions. Missing rows in a successful metrics response count as zero.

There is no CPU, memory, latency, read-QPS, or external HPA input. Read-only load may produce no scaling signal. Adding an HPA to the StatefulSet conflicts with the operator's reconciliation.

## Inspect Scaling

```bash
kubectl -n kahuna get kahunacluster kahuna -w
kubectl -n kahuna get kahunacluster kahuna -o yaml
kubectl -n kahuna get pods,pvc
kubectl -n kahuna logs kahuna-0 --tail=100
```

Inspect `status.autoscaling.desiredReplicas`, `recommendedReplicas`, `writesPerSecondPerNode`, `queueDepthPerNode`, `aboveTargetSince`, `belowTargetSince`, and `lastScaleTime`. The recommendation is already bounded by min/max; it does not show uncapped demand. `AutoscalingLimited` explains missing signals/placement, a growth ceiling, or maximum count. `RosterConverged` and `status.members` distinguish a running pod from a committed Voter. `Available` is a readiness/quorum summary, not a health proof for every data partition.

A scale-down asks the highest ordinal to leave before shrinking the StatefulSet. A reachable node's refusal or transport failure holds the count. If that pod is already absent or has no IP, the controller cannot decommission it and relies on membership eviction; another step waits for roster convergence. Do not force-delete live members to make a stalled scale appear complete.

Recent Kahuna join settings can be passed through `extraArgs` (no typed operator fields yet):

```yaml
spec:
  extraArgs:
    - --raft-join-timeout
    - "120000"
    - --raft-join-promotion-stall-timeout
    - "600000"
```

The first bounds admission/initialization. The second bounds lack of catch-up progress before Voter promotion, not total promotion time. Diagnose commit/snapshot progress before increasing it. A failed admitted join attempts withdrawal best-effort; verify the roster after failure.

## Return to a Fixed Size

While autoscaling is enabled, `spec.replicas` is only the initial size. To hand control back deliberately:

```bash
kubectl -n kahuna patch kahunacluster kahuna --type=merge \
  -p '{"spec":{"autoscaling":null,"replicas":4}}'
```

Removing autoscaling alone restores the old `spec.replicas` value and may immediately request a scale. Editing min/max clamps desired size immediately even without usable traffic metrics; actual membership still moves one node at a time. Two replicas and transitions between standalone and multi-node resources are rejected.

## Persistence, Updates, and Backups

`storage.pvcRetentionPolicy` defaults to `Retain` on scale-down. `Delete` reclaims scaled-away PVCs and their local state. Resource deletion retains PVCs regardless of that field; deleting the whole resource is not a graceful leave sequence. Changing an existing `storage.data.size` does not expand its PVCs: the reconciler does not mutate existing volume-claim templates.

Image/configuration changes roll pods; topology changes do not restart existing pods just to update seeds. Rollouts hold while the roster is unreadable or nonconverged. Secret content changes alone do not alter the startup-script hash, so keystore rotation must include a deliberate rollout. Default pod anti-affinity is soft; configure placement constraints for required failure-domain separation.

`spec.backup` schedules coordinated full backups, and optionally incrementals, to one ReadWriteMany volume that every node mounts (see the README's *Backups* section). It is refused when a replication factor is set below the most nodes the cluster can have, because a backup covers only the taking node's partitions. Check a cluster's backups with:

```bash
kubectl -n kahuna get kahunacluster kahuna -o jsonpath='{.status.backup}{"\n"}'
kubectl -n kahuna get kahunacluster kahuna \
  -o jsonpath='{.status.conditions[?(@.type=="BackupFailing")]}{"\n"}'
kubectl -n kahuna get kahunacluster kahuna \
  -o jsonpath='{.status.conditions[?(@.type=="BackupVolumeReady")]}{"\n"}'
```

If every pod is `Pending` and the cluster stays `Bootstrapping` after `backup` is added, read `BackupVolumeReady`. Every pod mounts the backup claim, so a claim that does not bind stops them all. The usual cause is a StorageClass without ReadWriteMany: the condition then carries the provisioner's `ProvisioningFailed` message. Set `spec.backup.volume.claim.storageClassName` to a class that supports ReadWriteMany. The operator replaces the claim while it is unbound. Alternatively, name a ReadWriteMany claim in `claimName`. The README shows a hostPath volume for single-node Kind.

To pause backups without a rollout, set `spec.backup.suspend: true`. Schedule and suspend changes do not restart pods; every other `backup` field changes the server command line and rolls the cluster. Do not take manual backups against the shared volume while a scheduled one runs: the inline GC after one backup can delete the other's artifacts before its manifest is published.

There is no restore CRD and no automatic seed-on-scale. The stock server has no built-in GCS provider, and `extraArgs` cannot register one.

See the [server backup guide](https://github.com/kahunakv/kahuna/blob/main/docs/backups-and-point-in-time-recovery-guide.md) for bootstrap, authentication, retention, and coverage requirements, and the [chart upgrade instructions](../charts/kahuna-operator/README.md#upgrading-and-the-crd-caveat) before updating CRDs.
