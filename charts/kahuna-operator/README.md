# kahuna-operator

Installs the [Kahuna](https://github.com/kahunakv/kahuna) Kubernetes operator: the controller, its
RBAC, and the `KahunaCluster` CRD.

The chart installs the **operator**, not a Kahuna cluster. Create `KahunaCluster` resources
afterwards — see the [operator README](../../README.md).

## Install

```bash
helm install kahuna-operator ./charts/kahuna-operator \
  --namespace kahuna-system --create-namespace
```

Then create a cluster:

```bash
kubectl create secret generic kahuna-tls \
  --from-file=certificate.pfx=/path/to/certificate.pfx

kubectl apply -f - <<'EOF'
apiVersion: kahuna.kahunakv.io/v1alpha1
kind: KahunaCluster
metadata:
  name: kahuna
spec:
  replicas: 3
  storage:
    data:
      size: 100Gi
  tls:
    secretRef:
      name: kahuna-tls
EOF
```

## Upgrading, and the CRD caveat

Helm installs files under `crds/` on first install but **never upgrades them**. That is a Helm
limitation, not a chart choice: `helm upgrade` silently leaves an old CRD in place, so new API
fields simply will not exist and the operator will behave as though you never set them.

When a release changes the CRD, apply it yourself before upgrading:

```bash
kubectl apply -f charts/kahuna-operator/crds/kahunaclusters.yaml
helm upgrade kahuna-operator ./charts/kahuna-operator --namespace kahuna-system
```

`helm uninstall` also leaves the CRD — and therefore every `KahunaCluster` — in place. Removing it
deletes every cluster the operator manages, so it stays a deliberate act:

```bash
kubectl delete crd kahunaclusters.kahuna.kahunakv.io   # destroys all managed clusters
```

## Values

| Key | Default | Description |
|---|---|---|
| `image.repository` | `ghcr.io/kahunakv/k8s-operator` | Operator image. |
| `image.tag` | `""` | Defaults to `.Chart.AppVersion`. |
| `replicaCount` | `1` | Above 1 is safe — leader election keeps one active reconciler; extras only shorten failover. |
| `leaderElection.enabled` | `true` | Required if `replicaCount > 1`. |
| `watchNamespace` | `""` | Restrict the operator to one namespace by scoping its cache. Empty watches all. RBAC stays cluster-scoped, so this is a scoping control, not a security boundary. |
| `rbac.create` | `true` | Disable only if you manage the ClusterRole yourself. |
| `crds.install` | `true` | See the caveat above. |
| `metrics.enabled` | `true` | controller-runtime metrics on `:8443` behind authn/authz. |
| `metrics.serviceMonitor.enabled` | `false` | Needs the Prometheus operator CRDs. |
| `resources` | 10m/128Mi → 500m/256Mi | Manager resources. |
| `extraArgs` | `[]` | Appended to the manager command line. |

## cert-manager

Only required for clusters that set `spec.tls.certManager` instead of supplying their own keystore
Secret. The chart neither installs nor depends on it.

## Keeping the chart in sync

`crds/kahunaclusters.yaml` and `files/manager-rules.yaml` are **generated**, not hand-written:
controller-gen derives them from the Go types and the kubebuilder markers on the reconciler. A
hand-edited copy drifts, and an RBAC drift surfaces as a permission error at runtime rather than at
install time. After `make manifests`, run:

```bash
./hack/sync-helm.sh
```
