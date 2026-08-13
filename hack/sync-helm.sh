#!/usr/bin/env bash
# Copies the generated CRD and RBAC rules into the Helm chart.
#
# The chart cannot own these: controller-gen derives them from the Go types and kubebuilder
# markers, so a hand-maintained copy would silently drift from what the operator actually needs —
# and an RBAC drift shows up as a permission error at runtime, not at install time. Run this
# whenever `make manifests` changes config/.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART="${ROOT}/charts/kahuna-operator"

mkdir -p "${CHART}/crds" "${CHART}/files"

cp "${ROOT}/config/crd/bases/kahuna.kahunakv.io_kahunaclusters.yaml" \
   "${CHART}/crds/kahunaclusters.yaml"

# Extract just the rules list from the generated ClusterRole. Everything before "rules:" is the
# object header the chart supplies itself.
awk '/^rules:/{found=1; next} found' "${ROOT}/config/rbac/role.yaml" > "${CHART}/files/manager-rules.yaml"

if [[ ! -s "${CHART}/files/manager-rules.yaml" ]]; then
  echo "sync-helm: extracted an empty rule set from config/rbac/role.yaml" >&2
  exit 1
fi

echo "synced CRD and $(grep -c '^- apiGroups:' "${CHART}/files/manager-rules.yaml") RBAC rule groups into the chart"
