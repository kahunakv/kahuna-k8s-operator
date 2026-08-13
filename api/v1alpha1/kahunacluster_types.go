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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// Tuning fields are pointers throughout: an unset field means "do not emit the flag" so the
// Kahuna server's own default applies. Mirroring defaults here would fork them silently the
// next time upstream changes one.

// StorageBackend selects a Kahuna persistence backend.
// +kubebuilder:validation:Enum=rocksdb;sqlite;memory
type StorageBackend string

const (
	StorageBackendRocksDB StorageBackend = "rocksdb"
	StorageBackendSQLite  StorageBackend = "sqlite"
	StorageBackendMemory  StorageBackend = "memory"
)

// ClusterPhase is a coarse, human-oriented summary of what the operator is currently doing.
// Conditions carry the precise state; this exists for `kubectl get`.
// +kubebuilder:validation:Enum=Pending;Bootstrapping;Running;ScalingUp;ScalingDown;Upgrading;Degraded
type ClusterPhase string

const (
	PhasePending       ClusterPhase = "Pending"
	PhaseBootstrapping ClusterPhase = "Bootstrapping"
	PhaseRunning       ClusterPhase = "Running"
	PhaseScalingUp     ClusterPhase = "ScalingUp"
	PhaseScalingDown   ClusterPhase = "ScalingDown"
	PhaseUpgrading     ClusterPhase = "Upgrading"
	PhaseDegraded      ClusterPhase = "Degraded"
)

// Condition types set by the operator.
const (
	// ConditionAvailable is true when a quorum of Voters is ready and serving.
	ConditionAvailable = "Available"
	// ConditionProgressing is true while the operator is mid-bootstrap, mid-scale, or mid-upgrade.
	ConditionProgressing = "Progressing"
	// ConditionRosterConverged is true when the committed Raft roster matches the desired replica set.
	ConditionRosterConverged = "RosterConverged"
	// ConditionDegraded is true when the cluster cannot reach or hold its desired state.
	ConditionDegraded = "Degraded"
)

// PVCRetentionPolicy controls what happens to a node's PersistentVolumeClaims when its pod is
// removed by a scale-down.
// +kubebuilder:validation:Enum=Retain;Delete
type PVCRetentionPolicy string

const (
	// PVCRetentionRetain keeps the PVCs so a re-scale-up reuses the data (and stays above the
	// Raft compaction floor). This is the default: deleting a Raft node's data is not reversible.
	PVCRetentionRetain PVCRetentionPolicy = "Retain"
	// PVCRetentionDelete reclaims the volumes when the node is scaled away.
	PVCRetentionDelete PVCRetentionPolicy = "Delete"
)

// VolumeSpec describes one PersistentVolumeClaim template.
type VolumeSpec struct {
	// size is the requested capacity for the volume.
	// +required
	Size resource.Quantity `json:"size"`

	// storageClassName selects the StorageClass. Unset uses the cluster default.
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`

	// accessModes for the claim. Defaults to ReadWriteOnce.
	// +optional
	AccessModes []corev1.PersistentVolumeAccessMode `json:"accessModes,omitempty"`
}

// StorageSpec configures Kahuna persistence. The backend and revision fields are immutable:
// changing any of them on a live cluster makes existing data unreadable.
type StorageSpec struct {
	// backend is the KV/locks persistence backend (--storage). Immutable.
	// +kubebuilder:default=rocksdb
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="storage.backend is immutable"
	// +optional
	Backend StorageBackend `json:"backend,omitempty"`

	// revision is the on-disk storage revision (--storage-revision). Immutable.
	// +kubebuilder:default="v1"
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="storage.revision is immutable"
	// +optional
	Revision string `json:"revision,omitempty"`

	// walBackend is the Raft write-ahead-log backend (--wal-storage). Immutable.
	// +kubebuilder:default=rocksdb
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="storage.walBackend is immutable"
	// +optional
	WALBackend StorageBackend `json:"walBackend,omitempty"`

	// walRevision is the on-disk WAL revision (--wal-revision). Immutable.
	// +kubebuilder:default="v1"
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="storage.walRevision is immutable"
	// +optional
	WALRevision string `json:"walRevision,omitempty"`

	// data is the PVC template for the key-value/locks data directory.
	// +required
	Data VolumeSpec `json:"data"`

	// wal is an optional separate PVC template for the Raft WAL. Give the WAL its own (fast)
	// volume when write latency matters — WAL writes are fsync-bound. When omitted the WAL
	// lives in a subdirectory of the data volume.
	// +optional
	WAL *VolumeSpec `json:"wal,omitempty"`

	// syncWrites enables durable synchronous WAL writes. Defaults to true (the server default).
	// Setting it false emits --disable-wal-sync-writes: faster, but a node can lose recently
	// acknowledged writes on an unclean shutdown. Do not disable in production.
	// +optional
	SyncWrites *bool `json:"syncWrites,omitempty"`

	// directReads enables RocksDB direct-I/O reads. Defaults to true (the server default).
	// Set false (emits --disable-rocksdb-direct-reads) on CSI drivers that do not support O_DIRECT.
	// +optional
	DirectReads *bool `json:"directReads,omitempty"`

	// sharedMemory enables a single RocksDB block cache and write-buffer manager shared between
	// the KV backend and the Raft WAL. Requires both backends to be rocksdb.
	// +optional
	SharedMemory *bool `json:"sharedMemory,omitempty"`

	// sharedMemoryBudgetMB is the total shared RocksDB block-cache budget in MiB.
	// +kubebuilder:validation:Minimum=1
	// +optional
	SharedMemoryBudgetMB *int32 `json:"sharedMemoryBudgetMB,omitempty"`

	// sharedMemtableBudgetMB is the shared memtable sub-budget in MiB. Must be <= sharedMemoryBudgetMB.
	// +kubebuilder:validation:Minimum=1
	// +optional
	SharedMemtableBudgetMB *int32 `json:"sharedMemtableBudgetMB,omitempty"`

	// pvcRetentionPolicy decides whether a scaled-away node's volumes are kept or deleted.
	// +kubebuilder:default=Retain
	// +optional
	PVCRetentionPolicy PVCRetentionPolicy `json:"pvcRetentionPolicy,omitempty"`
}

// CertManagerIssuerRef points at a cert-manager Issuer or ClusterIssuer.
type CertManagerIssuerRef struct {
	// name of the issuer.
	// +required
	Name string `json:"name"`

	// kind of the issuer.
	// +kubebuilder:default=Issuer
	// +kubebuilder:validation:Enum=Issuer;ClusterIssuer
	// +optional
	Kind string `json:"kind,omitempty"`

	// group of the issuer.
	// +kubebuilder:default="cert-manager.io"
	// +optional
	Group string `json:"group,omitempty"`
}

// CertManagerSpec asks the operator to provision the keystore via cert-manager. The generated
// Certificate enables a PKCS#12 keystore and carries SANs for every pod's stable peer DNS name.
type CertManagerSpec struct {
	// issuerRef selects the issuer that signs the certificate.
	// +required
	IssuerRef CertManagerIssuerRef `json:"issuerRef"`

	// duration of the issued certificate.
	// +optional
	Duration *metav1.Duration `json:"duration,omitempty"`

	// renewBefore is how long before expiry cert-manager renews.
	// +optional
	RenewBefore *metav1.Duration `json:"renewBefore,omitempty"`
}

// TLSSpec configures the PKCS#12 keystore Kahuna binds for HTTPS and for the Raft port.
//
// TLS is not optional for a cluster: the Raft port is an HTTPS Kestrel port and inter-node gRPC
// speaks https:// by default, so a keystore must be present. Supply exactly one of secretRef or
// certManager.
// +kubebuilder:validation:XValidation:rule="has(self.secretRef) != has(self.certManager)",message="exactly one of tls.secretRef or tls.certManager must be set"
type TLSSpec struct {
	// secretRef names an existing Secret holding a PKCS#12 keystore.
	// +optional
	SecretRef *corev1.LocalObjectReference `json:"secretRef,omitempty"`

	// keystoreKey is the Secret key holding the PKCS#12 blob.
	// +kubebuilder:default="certificate.pfx"
	// +optional
	KeystoreKey string `json:"keystoreKey,omitempty"`

	// passwordSecretRef points at the keystore password. Omit for an unprotected keystore.
	// +optional
	PasswordSecretRef *corev1.SecretKeySelector `json:"passwordSecretRef,omitempty"`

	// certManager provisions the keystore through cert-manager instead of using an existing Secret.
	// +optional
	CertManager *CertManagerSpec `json:"certManager,omitempty"`

	// insecureSkipVerify disables peer certificate validation on the Raft transport
	// (--raft-allow-insecure-certificate-validation). Defaults to true, which is what a
	// self-signed in-cluster keystore needs. Set false when the issuer's CA is trusted by
	// the container image.
	// +kubebuilder:default=true
	// +optional
	InsecureSkipVerify *bool `json:"insecureSkipVerify,omitempty"`
}

// PortsSpec overrides the container ports. The Raft port must be one of the HTTPS ports —
// inter-node gRPC rides the HTTPS listener — so the operator always binds https and raft together.
type PortsSpec struct {
	// http is the client REST/gRPC plaintext port (--http-ports).
	// +kubebuilder:default=2070
	// +optional
	HTTP int32 `json:"http,omitempty"`

	// https is the client TLS port (--https-ports).
	// +kubebuilder:default=2071
	// +optional
	HTTPS int32 `json:"https,omitempty"`

	// internalHTTP is the secondary plaintext port bound alongside the Raft listener.
	// +kubebuilder:default=8081
	// +optional
	InternalHTTP int32 `json:"internalHTTP,omitempty"`

	// raft is the inter-node Raft port. Bound as an HTTPS port and passed as --raft-port.
	// +kubebuilder:default=8082
	// +optional
	Raft int32 `json:"raft,omitempty"`
}

// RaftSpec exposes the consensus, catch-up and failure-detector knobs that operators actually
// tune. Everything else on the Raft surface is reachable through extraArgs.
type RaftSpec struct {
	// heartbeatIntervalMs is the leader heartbeat interval (--raft-heartbeat-interval).
	// +optional
	HeartbeatIntervalMs *int32 `json:"heartbeatIntervalMs,omitempty"`

	// votingTimeoutMs is how long a vote round waits (--raft-voting-timeout).
	// +optional
	VotingTimeoutMs *int32 `json:"votingTimeoutMs,omitempty"`

	// startElectionTimeoutMs is the minimum election timeout (--raft-start-election-timeout).
	// +optional
	StartElectionTimeoutMs *int32 `json:"startElectionTimeoutMs,omitempty"`

	// endElectionTimeoutMs is the maximum election timeout (--raft-end-election-timeout).
	// +optional
	EndElectionTimeoutMs *int32 `json:"endElectionTimeoutMs,omitempty"`

	// enableCheckQuorum makes a leader that has lost contact with a majority step down
	// (--raft-enable-check-quorum).
	// +optional
	EnableCheckQuorum *bool `json:"enableCheckQuorum,omitempty"`

	// gossipIntervalMs between roster/health gossip rounds (--raft-gossip-interval).
	// +optional
	GossipIntervalMs *int32 `json:"gossipIntervalMs,omitempty"`

	// gossipFanout is how many peers each gossip round contacts (--raft-gossip-fanout).
	// +optional
	GossipFanout *int32 `json:"gossipFanout,omitempty"`

	// pingTimeoutMs to wait for a direct SWIM ping reply (--raft-ping-timeout).
	// +optional
	PingTimeoutMs *int32 `json:"pingTimeoutMs,omitempty"`

	// indirectPingFanout is how many peers are asked to ping a suspect (--raft-indirect-ping-fanout).
	// +optional
	IndirectPingFanout *int32 `json:"indirectPingFanout,omitempty"`

	// suspicionTimeoutMs a member stays suspect before being declared dead
	// (--raft-suspicion-timeout). A dead verdict is irreversible — prefer generous values on
	// networks with latency spikes or nodes that pause under load.
	// +optional
	SuspicionTimeoutMs *int32 `json:"suspicionTimeoutMs,omitempty"`

	// deadMemberEvictionGraceMs of cushion before a dead member's removal is committed
	// (--raft-dead-member-eviction-grace). The operator also waits out this window during
	// scale-down, so raising it slows scale-down proportionally.
	// +optional
	DeadMemberEvictionGraceMs *int32 `json:"deadMemberEvictionGraceMs,omitempty"`

	// backfillThreshold is how far behind a follower may fall before catch-up starts
	// (--raft-backfill-threshold).
	// +optional
	BackfillThreshold *int32 `json:"backfillThreshold,omitempty"`

	// maxBackfillEntriesPerRound shipped per catch-up round (--raft-max-backfill-entries-per-round).
	// +optional
	MaxBackfillEntriesPerRound *int32 `json:"maxBackfillEntriesPerRound,omitempty"`

	// learnerPromotionLag is how close a Learner must get before promotion
	// (--raft-learner-promotion-lag).
	// +optional
	LearnerPromotionLag *int32 `json:"learnerPromotionLag,omitempty"`

	// learnerPromotionStableWindowMs it must stay caught up before promotion
	// (--raft-learner-promotion-stable-window).
	// +optional
	LearnerPromotionStableWindowMs *int32 `json:"learnerPromotionStableWindowMs,omitempty"`
}

// TransactionsSpec exposes the transaction timeout and admission-control knobs.
type TransactionsSpec struct {
	// defaultTimeoutMs for a transaction (--default-transaction-timeout).
	// +optional
	DefaultTimeoutMs *int32 `json:"defaultTimeoutMs,omitempty"`

	// defaultAdmissionWaitMs a caller queues for an admission slot (--default-admission-wait).
	// +optional
	DefaultAdmissionWaitMs *int32 `json:"defaultAdmissionWaitMs,omitempty"`

	// maxAdmissionWaitMs is the hard upper bound on any admission wait (--max-admission-wait).
	// +optional
	MaxAdmissionWaitMs *int32 `json:"maxAdmissionWaitMs,omitempty"`

	// maxConcurrentTransactions before further script transactions queue
	// (--max-concurrent-transactions). 0 means no limit.
	// +optional
	MaxConcurrentTransactions *int32 `json:"maxConcurrentTransactions,omitempty"`

	// maxConcurrentSessions is the ceiling on open interactive transaction sessions
	// (--max-concurrent-sessions). 0 means no limit.
	// +optional
	MaxConcurrentSessions *int32 `json:"maxConcurrentSessions,omitempty"`
}

// WorkersSpec sizes the per-subsystem actor worker pools.
type WorkersSpec struct {
	// locks worker count (--locks-workers).
	// +optional
	Locks *int32 `json:"locks,omitempty"`

	// keyValue worker count (--keyvalue-workers).
	// +optional
	KeyValue *int32 `json:"keyValue,omitempty"`

	// sequencer worker count (--sequencer-workers).
	// +optional
	Sequencer *int32 `json:"sequencer,omitempty"`

	// backgroundWriter worker count (--background-writer-workers).
	// +optional
	BackgroundWriter *int32 `json:"backgroundWriter,omitempty"`
}

// KahunaClusterSpec defines the desired state of a Kahuna cluster.
type KahunaClusterSpec struct {
	// replicas is the number of Kahuna nodes. Raft needs an odd count for a clean majority;
	// 3 tolerates one failure, 5 tolerates two.
	//
	// 2 is rejected: a two-Voter cluster has a quorum of 2, so it tolerates no failures at all
	// while costing twice as much as a single node.
	//
	// 1 is a special case — a single node runs Kahuna in standalone mode, which uses a different
	// quorum model and a different on-disk identity. It is fine for development, but a
	// single-node cluster cannot later be grown into a real one, which is why growing away from
	// 1 is rejected below rather than silently corrupting the node's state.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// image is the Kahuna server image. The operator overrides the image entrypoint with its own
	// generated startup script, so the stock single-node image works unmodified.
	// +kubebuilder:default="kahunakv/kahuna:latest"
	// +optional
	Image string `json:"image,omitempty"`

	// imagePullPolicy for the Kahuna container.
	// +optional
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// imagePullSecrets for a private registry.
	// +optional
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// partitions is the initial Raft partition count (--initial-cluster-partitions). Immutable:
	// it is fixed when the cluster is first bootstrapped and changing it later would orphan the
	// existing partition map.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="partitions is immutable once the cluster is created"
	// +optional
	Partitions *int32 `json:"partitions,omitempty"`

	// storage configures persistence and the per-node volumes.
	// +required
	Storage StorageSpec `json:"storage"`

	// tls configures the PKCS#12 keystore used for HTTPS and the Raft transport.
	// +required
	TLS TLSSpec `json:"tls"`

	// ports overrides the container port assignments.
	// +optional
	Ports *PortsSpec `json:"ports,omitempty"`

	// raft tunes consensus, catch-up and failure detection.
	// +optional
	Raft *RaftSpec `json:"raft,omitempty"`

	// transactions tunes transaction timeouts and admission control.
	// +optional
	Transactions *TransactionsSpec `json:"transactions,omitempty"`

	// workers sizes the per-subsystem actor worker pools.
	// +optional
	Workers *WorkersSpec `json:"workers,omitempty"`

	// logLevel for the Kahuna and Kommander log categories.
	// +kubebuilder:default="Information"
	// +kubebuilder:validation:Enum=Trace;Debug;Information;Warning;Error;Critical;None
	// +optional
	LogLevel string `json:"logLevel,omitempty"`

	// extraArgs are appended verbatim to the server command line, after every flag the operator
	// generates. This is the escape hatch for the Kahuna options that have no typed field here;
	// nothing validates them, and a bad value fails the pod at startup.
	// +optional
	ExtraArgs []string `json:"extraArgs,omitempty"`

	// resources for the Kahuna container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// nodeSelector for the Kahuna pods.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// tolerations for the Kahuna pods.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// affinity for the Kahuna pods. When unset the operator applies a soft anti-affinity that
	// prefers spreading nodes across hosts.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// topologySpreadConstraints for the Kahuna pods.
	// +optional
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`

	// podAnnotations added to every Kahuna pod.
	// +optional
	PodAnnotations map[string]string `json:"podAnnotations,omitempty"`

	// podLabels added to every Kahuna pod.
	// +optional
	PodLabels map[string]string `json:"podLabels,omitempty"`

	// serviceAccountName for the Kahuna pods. The pods make no Kubernetes API calls; this exists
	// for image-pull and admission-policy purposes.
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty"`

	// podSecurityContext for the Kahuna pods.
	// +optional
	PodSecurityContext *corev1.PodSecurityContext `json:"podSecurityContext,omitempty"`

	// securityContext for the Kahuna container.
	// +optional
	SecurityContext *corev1.SecurityContext `json:"securityContext,omitempty"`

	// terminationGracePeriodSeconds for the Kahuna pods.
	// +optional
	TerminationGracePeriodSeconds *int64 `json:"terminationGracePeriodSeconds,omitempty"`

	// podDisruptionBudget creates a PDB allowing at most one unavailable node. Defaults to true
	// when replicas >= 3.
	// +optional
	PodDisruptionBudget *bool `json:"podDisruptionBudget,omitempty"`
}

// MemberStatus is one entry of the committed Raft roster, joined to the pod that serves it.
type MemberStatus struct {
	// podName serving this member, derived from the endpoint. Empty if the endpoint does not
	// map to a pod this cluster owns — which is how an orphaned roster entry shows up.
	// +optional
	PodName string `json:"podName,omitempty"`

	// endpoint is the member's Raft address as recorded in the roster.
	// +required
	Endpoint string `json:"endpoint"`

	// nodeID is the member's stable Raft node id.
	// +optional
	NodeID int32 `json:"nodeID,omitempty"`

	// role is Voter, Learner, Leaving or NotMember. Only Voters count toward quorum.
	// +optional
	Role string `json:"role,omitempty"`

	// joinedVersion is the membership version at which this member was admitted.
	// +optional
	JoinedVersion int64 `json:"joinedVersion,omitempty"`

	// ready reports whether the member's pod passes /v1/cluster/health.
	// +optional
	Ready bool `json:"ready,omitempty"`
}

// KahunaClusterStatus defines the observed state of a Kahuna cluster.
type KahunaClusterStatus struct {
	// observedGeneration is the spec generation this status reflects.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// phase is a coarse summary of what the operator is doing. Conditions carry the detail.
	// +optional
	Phase ClusterPhase `json:"phase,omitempty"`

	// replicas is the number of pods the StatefulSet currently has.
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// readyReplicas is the number of pods passing the readiness probe.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// voters is the number of Voters in the committed roster. Quorum is a majority of this.
	// +optional
	Voters int32 `json:"voters,omitempty"`

	// learners is the number of members still catching up.
	// +optional
	Learners int32 `json:"learners,omitempty"`

	// membershipVersion is the committed roster version. It advances on every admitted change.
	// +optional
	MembershipVersion int64 `json:"membershipVersion,omitempty"`

	// members is the committed roster as last read from the cluster.
	// +optional
	// +listType=map
	// +listMapKey=endpoint
	Members []MemberStatus `json:"members,omitempty"`

	// bootstrapped records that the initial roster was seeded and every founding node was
	// admitted as a Voter.
	// +optional
	Bootstrapped bool `json:"bootstrapped,omitempty"`

	// bootstrapSize is the replica count the cluster was first created at. It is pinned when the
	// StatefulSet is created and never changes, because it is what tells a starting node whether
	// it is one of the founding members — which form the cluster through static discovery — or a
	// node added later, which must join the running cluster as a Learner instead.
	// +optional
	BootstrapSize int32 `json:"bootstrapSize,omitempty"`

	// currentImage is the image the fully-rolled-out nodes are running.
	// +optional
	CurrentImage string `json:"currentImage,omitempty"`

	// labelSelector is the serialized pod selector, required by the scale subresource.
	// +optional
	LabelSelector string `json:"labelSelector,omitempty"`

	// updatePartition is the StatefulSet rolling-update partition the operator is currently
	// holding. Nodes at or above this ordinal have been updated.
	// +optional
	UpdatePartition *int32 `json:"updatePartition,omitempty"`

	// conditions represent the current state of the KahunaCluster resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.labelSelector
// +kubebuilder:validation:XValidation:rule="self.spec.replicas != 2",message="replicas must not be 2: a two-Voter Raft cluster has a quorum of 2 and therefore tolerates no failures; use 1 or 3"
// +kubebuilder:validation:XValidation:rule="(oldSelf.spec.replicas == 1) == (self.spec.replicas == 1)",message="a single-node (standalone) cluster cannot be scaled into a multi-node cluster, or vice versa: the two use different quorum models and on-disk identities. Create a new KahunaCluster and migrate the data."
// +kubebuilder:resource:shortName=kahuna;kc
// +kubebuilder:printcolumn:name="Desired",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Voters",type=integer,JSONPath=`.status.voters`
// +kubebuilder:printcolumn:name="Roster",type=integer,JSONPath=`.status.membershipVersion`,priority=1
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// KahunaCluster is the Schema for the kahunaclusters API
type KahunaCluster struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of KahunaCluster
	// +required
	Spec KahunaClusterSpec `json:"spec"`

	// status defines the observed state of KahunaCluster
	// +optional
	Status KahunaClusterStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// KahunaClusterList contains a list of KahunaCluster
type KahunaClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []KahunaCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &KahunaCluster{}, &KahunaClusterList{})
		return nil
	})
}
