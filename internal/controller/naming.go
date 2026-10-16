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
	"fmt"
	"maps"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kahunav1alpha1 "github.com/kahunakv/k8s-operator/api/v1alpha1"
)

// Mount paths and volume names used by the generated pod spec.
const (
	// storageMountPath is where the data PVC is mounted. Both the backend files and the WAL live
	// under it, so a cluster with no dedicated WAL volume needs exactly one claim.
	storageMountPath = "/storage"
	// dataPath holds the key-value/locks backend files.
	dataPath = storageMountPath + "/data"
	// walPath holds the Raft write-ahead log. When spec.storage.wal requests a dedicated volume
	// it is mounted here, nested inside the data volume's mount.
	walPath = storageMountPath + "/wal"

	configMountPath      = "/etc/kahuna"
	tlsMountPath         = "/etc/kahuna/tls"
	tlsPasswordMountPath = "/etc/kahuna/tls-password"

	entrypointFileName = "entrypoint.sh"
	entrypointPath     = configMountPath + "/" + entrypointFileName

	// topologyFileName holds the cluster size and bootstrap size. It is a separate key so that
	// scaling does not alter the pod template — see topologyEnv.
	topologyFileName = "topology.env"
	topologyPath     = configMountPath + "/" + topologyFileName

	// backupMountPath is where the shared backup volume is mounted. The server's backup directory
	// is a subdirectory named after the cluster; see backupPath.
	backupMountPath = "/backup"
	// backupMACMountPath holds the key that signs backup manifests.
	backupMACMountPath = "/etc/kahuna/backup-mac"
	backupMACFileName  = "key"
	backupMACKeyPath   = backupMACMountPath + "/" + backupMACFileName

	dataVolumeName        = "data"
	walVolumeName         = "wal"
	configVolumeName      = "config"
	tlsVolumeName         = "tls"
	tlsPasswordVolumeName = "tls-password"
	backupVolumeName      = "backup"
	backupMACVolumeName   = "backup-mac"

	containerName = "kahuna"

	// appName is the app.kubernetes.io/name every owned object carries.
	appName = "kahuna"

	// portNameHTTP and portNameHTTPS name the client-facing ports on both Services.
	portNameHTTP  = "http"
	portNameHTTPS = "https"

	// configHashAnnotation carries a digest of the generated startup script. It lives on the pod
	// template so that a config change produces a new template revision — a ConfigMap edit alone
	// would otherwise never reach a running pod.
	configHashAnnotation = "kahuna.kahunakv.io/config-hash"
)

// stsName is the StatefulSet name, and therefore the prefix of every pod name.
func stsName(cluster *kahunav1alpha1.KahunaCluster) string { return cluster.Name }

// peerServiceName is the headless Service that gives each pod a stable DNS name.
func peerServiceName(cluster *kahunav1alpha1.KahunaCluster) string { return cluster.Name + "-peer" }

// clientServiceName is the load-balanced Service clients connect to.
func clientServiceName(cluster *kahunav1alpha1.KahunaCluster) string { return cluster.Name }

// configMapName holds the generated startup script.
func configMapName(cluster *kahunav1alpha1.KahunaCluster) string { return cluster.Name + "-config" }

// pdbName is the PodDisruptionBudget guarding voluntary evictions.
func pdbName(cluster *kahunav1alpha1.KahunaCluster) string { return cluster.Name }

// certificateName is the cert-manager Certificate and the Secret it writes.
func certificateName(cluster *kahunav1alpha1.KahunaCluster) string { return cluster.Name + "-tls" }

// backupClaimName is the PersistentVolumeClaim holding the backups: the user's own claim, or the
// one the operator creates from spec.backup.volume.claim.
func backupClaimName(cluster *kahunav1alpha1.KahunaCluster) string {
	if v := cluster.Spec.Backup.Volume; v.ClaimName != nil {
		return *v.ClaimName
	}
	return cluster.Name + "-backup"
}

// backupMACSecretName is the Secret the operator generates when no MAC key is supplied.
func backupMACSecretName(cluster *kahunav1alpha1.KahunaCluster) string {
	return cluster.Name + "-backup-mac"
}

// backupPath is the server's backup directory. It is a subdirectory of the mount, not the mount
// itself, for two reasons. Two clusters can then share one claim. And the server refuses a backup
// directory that is group-writable, which the root of a volume often is (an fsGroup makes it so);
// a subdirectory that does not exist yet is created by the server with mode 0700.
func backupPath(cluster *kahunav1alpha1.KahunaCluster) string {
	return backupMountPath + "/" + cluster.Name
}

// backupClusterID is the id stamped into every backup.
func backupClusterID(cluster *kahunav1alpha1.KahunaCluster) string {
	if id := cluster.Spec.Backup.ClusterID; id != "" {
		return id
	}
	return cluster.Namespace + "/" + cluster.Name
}

// podName returns the pod name for an ordinal. StatefulSet pod names are stable, which is what
// makes the ordinal usable as a durable Raft node identity.
func podName(cluster *kahunav1alpha1.KahunaCluster, ordinal int32) string {
	return fmt.Sprintf("%s-%d", stsName(cluster), ordinal)
}

// peerDomain is the DNS suffix shared by every pod of the cluster.
func peerDomain(cluster *kahunav1alpha1.KahunaCluster) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local", peerServiceName(cluster), cluster.Namespace)
}

// podFQDN is a pod's stable per-pod DNS name, served by the headless Service.
func podFQDN(cluster *kahunav1alpha1.KahunaCluster, ordinal int32) string {
	return fmt.Sprintf("%s.%s", podName(cluster, ordinal), peerDomain(cluster))
}

// raftEndpoint is the address a node registers in the Raft roster: its per-pod DNS name and the
// Raft port. Roster entries are matched back to pods by this string.
func raftEndpoint(cluster *kahunav1alpha1.KahunaCluster, ordinal int32) string {
	return fmt.Sprintf("%s:%d", podFQDN(cluster, ordinal), ports(cluster).Raft)
}

// nodeIDForOrdinal maps a StatefulSet ordinal to a Raft node id.
//
// The +1 is load-bearing: --raft-nodeid defaults to 0, so a node handed 0 is indistinguishable
// from one that was never configured.
func nodeIDForOrdinal(ordinal int32) int32 { return ordinal + 1 }

// labels are the selector labels — the immutable subset. A StatefulSet's selector cannot be
// changed after creation, so nothing that varies with the spec may appear here.
func selectorLabels(cluster *kahunav1alpha1.KahunaCluster) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":     appName,
		"app.kubernetes.io/instance": cluster.Name,
	}
}

// podLabels are the selector labels plus descriptive labels and any user-supplied extras.
func podLabels(cluster *kahunav1alpha1.KahunaCluster) map[string]string {
	out := map[string]string{
		"app.kubernetes.io/component":  "server",
		"app.kubernetes.io/managed-by": "kahuna-operator",
	}
	maps.Copy(out, cluster.Spec.PodLabels)
	// Selector labels are applied last so a user-supplied label cannot break the selector match.
	maps.Copy(out, selectorLabels(cluster))
	return out
}

// selector is the label selector shared by the StatefulSet, Services and PDB.
func selector(cluster *kahunav1alpha1.KahunaCluster) *metav1.LabelSelector {
	return &metav1.LabelSelector{MatchLabels: selectorLabels(cluster)}
}

// ports resolves the port configuration, filling in defaults for an omitted block. The CRD
// defaults cover a partially specified block; this covers the block being absent entirely.
func ports(cluster *kahunav1alpha1.KahunaCluster) kahunav1alpha1.PortsSpec {
	p := kahunav1alpha1.PortsSpec{HTTP: 2070, HTTPS: 2071, InternalHTTP: 8081, Raft: 8082}
	if cluster.Spec.Ports == nil {
		return p
	}
	if cluster.Spec.Ports.HTTP != 0 {
		p.HTTP = cluster.Spec.Ports.HTTP
	}
	if cluster.Spec.Ports.HTTPS != 0 {
		p.HTTPS = cluster.Spec.Ports.HTTPS
	}
	if cluster.Spec.Ports.InternalHTTP != 0 {
		p.InternalHTTP = cluster.Spec.Ports.InternalHTTP
	}
	if cluster.Spec.Ports.Raft != 0 {
		p.Raft = cluster.Spec.Ports.Raft
	}
	return p
}

// desiredReplicas is the node count the cluster is being driven toward. Under autoscaling that is
// the autoscaler's choice, not spec.replicas: everything that sizes the cluster — the StatefulSet,
// the topology file, the PDB, status — must agree on one number, and only one actor may own it.
func desiredReplicas(cluster *kahunav1alpha1.KahunaCluster) int32 {
	if cluster.Spec.Autoscaling != nil {
		if as := cluster.Status.Autoscaling; as != nil && as.DesiredReplicas > 0 {
			return as.DesiredReplicas
		}
		return initialAutoscaledReplicas(cluster)
	}
	return specReplicas(cluster)
}

// specReplicas resolves spec.replicas, defaulting to 3.
func specReplicas(cluster *kahunav1alpha1.KahunaCluster) int32 {
	if cluster.Spec.Replicas == nil {
		return 3
	}
	return *cluster.Spec.Replicas
}

// initialAutoscaledReplicas is where the autoscaler starts before it has chosen anything:
// spec.replicas, held inside the autoscaling bounds.
func initialAutoscaledReplicas(cluster *kahunav1alpha1.KahunaCluster) int32 {
	as := cluster.Spec.Autoscaling
	return min(max(specReplicas(cluster), as.MinReplicas), as.MaxReplicas)
}
