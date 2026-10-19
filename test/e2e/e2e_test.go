//go:build e2e
// +build e2e

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

package e2e

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/kahunakv/k8s-operator/internal/kahuna"
	"github.com/kahunakv/k8s-operator/test/utils"
)

const (
	operatorNamespace = "kahuna-operator-system"
	testNamespace     = "kahuna-e2e"
	clusterName       = "kahuna"
	// backupVolumeName names both the hostPath volume and the claim the backup spec mounts.
	backupVolumeName = "kahuna-e2e-backup"

	// Scale steps are one consensus round plus the operator's requeue interval now that nodes are
	// asked to leave rather than evicted by the failure detector. The allowance stays generous so
	// a slow CI runner does not turn a timing difference into a failure.
	membershipTimeout = 5 * time.Minute
	bootstrapTimeout  = 4 * time.Minute
	pollInterval      = 5 * time.Second
)

// kubectl runs a kubectl command and returns its trimmed stdout.
func kubectl(args ...string) (string, error) {
	out, err := utils.Run(exec.Command("kubectl", args...))
	return strings.TrimSpace(out), err
}

// mustKubectl runs a kubectl command and fails the spec if it errors.
func mustKubectl(args ...string) string {
	out, err := kubectl(args...)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "kubectl %s: %s", strings.Join(args, " "), out)
	return out
}

// applyYAML pipes a manifest into kubectl apply.
func applyYAML(manifest string) {
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	out, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "applying manifest: %s", out)
}

// clusterField reads a jsonpath expression off the KahunaCluster.
func clusterField(path string) string {
	out, err := kubectl("-n", testNamespace, "get", "kahunacluster", clusterName,
		"-o", "jsonpath="+path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}

// podUIDs maps pod name to UID, which is how a restart is detected: a restarted pod keeps its
// name but gets a new UID.
func podUIDs() map[string]string {
	return podFieldOf(clusterName, "{.metadata.uid}")
}

// podImagesOf maps each pod of the named cluster to the image of its Kahuna container.
func podImagesOf(cluster string) map[string]string {
	return podFieldOf(cluster, `{.spec.containers[?(@.name=="kahuna")].image}`)
}

// podFieldOf maps each pod of the named cluster to a jsonpath expression read off that pod.
func podFieldOf(cluster, path string) map[string]string {
	out, err := kubectl("-n", testNamespace, "get", "pods",
		"-l", "app.kubernetes.io/instance="+cluster,
		"-o", `jsonpath={range .items[*]}{.metadata.name}=`+path+` {end}`)
	fields := map[string]string{}
	if err != nil {
		return fields
	}
	for _, kv := range strings.Fields(out) {
		if name, value, ok := strings.Cut(kv, "="); ok {
			fields[name] = value
		}
	}
	return fields
}

// withPortForward opens a port-forward to the client Service and runs fn against it. Pod and
// Service IPs inside Kind are not routable from the test process, so this is how the suite talks
// to the cluster it just built.
func withPortForward(fn func(baseURL string)) {
	GinkgoHelper()
	withPortForwardTo(clusterName, fn)
}

// withPortForwardTo is withPortForward for the named cluster.
func withPortForwardTo(cluster string, fn func(baseURL string)) {
	GinkgoHelper()
	const localPort = 32070
	cmd := exec.Command("kubectl", "-n", testNamespace, "port-forward",
		"svc/"+cluster, fmt.Sprintf("%d:2070", localPort))
	Expect(cmd.Start()).To(Succeed())
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	baseURL := fmt.Sprintf("http://localhost:%d", localPort)
	// The forward takes a moment to bind; poll rather than sleep a fixed amount.
	Eventually(func() error {
		resp, err := http.Get(baseURL + "/")
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		return nil
	}, 60*time.Second, time.Second).Should(Succeed(), "port-forward never became usable")

	fn(baseURL)
}

// kvSet writes a key. Durability 1 is Persistent.
func kvSet(baseURL, key, value string) {
	ExpectWithOffset(1, kvTrySet(baseURL, key, value)).To(Succeed())
}

// kvTrySet writes a key and returns why the write did not succeed, without failing the spec. A
// background writer uses it, where a failed assertion cannot stop the spec.
func kvTrySet(baseURL, key, value string) error {
	body := map[string]any{
		"key":        key,
		"value":      base64.StdEncoding.EncodeToString([]byte(value)),
		"durability": 1,
	}
	raw, _ := json.Marshal(body)
	resp, err := http.Post(baseURL+"/v1/kv/try-set", "application/json", strings.NewReader(string(raw)))
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	var out struct {
		Type int `json:"type"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return err
	}
	// 0 is KeyValueResponseType.Set.
	if out.Type != 0 {
		return fmt.Errorf("set of %s returned type %d", key, out.Type)
	}
	return nil
}

// kvGet reads a key back.
func kvGet(baseURL, key string) string {
	body := map[string]any{"key": key, "revision": -1, "durability": 1}
	raw, _ := json.Marshal(body)
	resp, err := http.Post(baseURL+"/v1/kv/try-get", "application/json", strings.NewReader(string(raw)))
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	defer func() { _ = resp.Body.Close() }()

	var out struct {
		Type  int    `json:"type"`
		Value string `json:"value"`
	}
	ExpectWithOffset(1, json.NewDecoder(resp.Body).Decode(&out)).To(Succeed())
	ExpectWithOffset(1, out.Type).To(Equal(3), "get did not return a value (type %d)", out.Type)

	decoded, err := base64.StdEncoding.DecodeString(out.Value)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return string(decoded)
}

// clusterManifest renders the KahunaCluster at the given size.
func clusterManifest(replicas int, extraArgs string) string {
	return clusterManifestFor(clusterName, kahunaImage, replicas, extraArgs)
}

// clusterManifestFor renders a KahunaCluster with the given name, image and size.
func clusterManifestFor(name, image string, replicas int, extraArgs string) string {
	return fmt.Sprintf(`
apiVersion: kahuna.kahunakv.io/v1alpha1
kind: KahunaCluster
metadata:
  name: %s
  namespace: %s
spec:
  replicas: %d
  image: %s
  imagePullPolicy: IfNotPresent
  partitions: 6
  storage:
    backend: rocksdb
    data:
      size: 1Gi
  tls:
    certManager:
      issuerRef:
        name: kahuna-selfsigned
        kind: Issuer
    insecureSkipVerify: true
  resources:
    requests:
      cpu: 100m
      memory: 256Mi
%s
`, name, testNamespace, replicas, image, extraArgs)
}

// dumpClusterState prints what a failed spec left behind: the KahunaCluster, its pods and
// events, the pods' logs and the operator's. Each command's output is printed even when it fails,
// since a half-torn-down namespace is exactly when the partial view is the useful one.
func dumpClusterState() {
	show := func(args ...string) {
		out, err := kubectl(args...)
		if err != nil {
			out = err.Error()
		}
		_, _ = fmt.Fprintf(GinkgoWriter, "===== kubectl %s =====\n%s\n", strings.Join(args, " "), out)
	}
	show("-n", testNamespace, "get", "kahunacluster", "-o", "yaml")
	show("-n", testNamespace, "get", "pods,sts,svc,pvc,certificate", "-o", "wide")
	show("-n", testNamespace, "get", "events", "--sort-by=.lastTimestamp")
	show("-n", testNamespace, "describe", "pods")
	pods, _ := kubectl("-n", testNamespace, "get", "pods", "-o", "name")
	for _, pod := range strings.Fields(pods) {
		show("-n", testNamespace, "logs", pod, "--all-containers", "--tail=200")
		// A crash loop's cause is in the log of the attempt that died, not the one now starting.
		restarts, _ := kubectl("-n", testNamespace, "get", pod, "-o",
			"jsonpath={.status.containerStatuses[*].restartCount}")
		if strings.ContainsFunc(restarts, func(r rune) bool { return r >= '1' && r <= '9' }) {
			show("-n", testNamespace, "logs", pod, "--all-containers", "--previous", "--tail=100")
		}
	}
	show("-n", operatorNamespace, "logs", "deployment/kahuna-operator-controller-manager", "--tail=300")
}

// expectConverged waits for the operator to report a converged cluster of the given size.
func expectConverged(replicas int) {
	GinkgoHelper()
	expectConvergedOf(clusterName, replicas)
}

// expectConvergedOf is expectConverged for the named cluster.
func expectConvergedOf(cluster string, replicas int) {
	GinkgoHelper()
	field := func(path string) string {
		return clusterFieldIn(testNamespace, "kahunacluster", cluster, path)
	}
	Eventually(func() string {
		return fmt.Sprintf("%s/%s/%s",
			field("{.status.voters}"),
			field("{.status.readyReplicas}"),
			field("{.status.phase}"))
	}, membershipTimeout, pollInterval).Should(
		Equal(fmt.Sprintf("%d/%d/Running", replicas, replicas)),
		"%s never converged at %d nodes", cluster, replicas)
}

var _ = Describe("KahunaCluster", Ordered, func() {
	BeforeAll(func() {
		By("deploying the operator")
		mustKubectl("create", "namespace", operatorNamespace, "--dry-run=client", "-o", "yaml")
		_, err := utils.Run(exec.Command("make", "install"))
		Expect(err).NotTo(HaveOccurred(), "failed to install CRDs")
		_, err = utils.Run(exec.Command("make", "deploy", "IMG="+managerImage))
		Expect(err).NotTo(HaveOccurred(), "failed to deploy the operator")

		// The image tag is fixed, so a rebuilt image leaves the Deployment spec byte-identical and
		// Kubernetes has no reason to restart anything — on a reused cluster that silently tests
		// the *previous* build. Forcing a rollout makes "the operator under test" mean the binary
		// this run just built.
		By("restarting the operator so the freshly built image is the one running")
		mustKubectl("-n", operatorNamespace, "rollout", "restart",
			"deployment/kahuna-operator-controller-manager")
		mustKubectl("-n", operatorNamespace, "rollout", "status",
			"deployment/kahuna-operator-controller-manager", "--timeout=5m")

		Eventually(func() string {
			return clusterFieldIn(operatorNamespace, "deployment", "kahuna-operator-controller-manager",
				"{.status.readyReplicas}")
		}, 3*time.Minute, pollInterval).Should(Equal("1"), "operator never became ready")

		By("creating the test namespace and a self-signed issuer")
		applyYAML(fmt.Sprintf("apiVersion: v1\nkind: Namespace\nmetadata:\n  name: %s\n", testNamespace))
		applyYAML(fmt.Sprintf(`
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: kahuna-selfsigned
  namespace: %s
spec:
  selfSigned: {}
`, testNamespace))
	})

	// The specs are ordered, so one failure skips the rest and the container ends. Whether any
	// spec failed is recorded here because AfterAll only sees the report of the last spec to run,
	// which on a failure is a skipped one.
	var failed bool
	JustAfterEach(func() {
		if CurrentSpecReport().Failed() {
			failed = true
		}
	})

	AfterAll(func() {
		if failed {
			// The namespace is the evidence. Deleting it here would leave CI's post-failure dump
			// with nothing to show, and a local run with nothing to inspect.
			By("keeping the test namespace for inspection because a spec failed")
			dumpClusterState()
			return
		}
		By("removing the test namespace")
		_, _ = kubectl("delete", "namespace", testNamespace, "--wait=false")
		_, _ = kubectl("delete", "pv", backupVolumeName, "--ignore-not-found", "--wait=false")
	})

	It("rejects configurations that cannot work", func() {
		By("refusing a two-node cluster")
		cmd := exec.Command("kubectl", "apply", "-f", "-")
		cmd.Stdin = strings.NewReader(clusterManifest(2, ""))
		out, err := utils.Run(cmd)
		Expect(err).To(HaveOccurred(), "a 2-replica cluster was accepted: %s", out)
		Expect(out).To(ContainSubstring("must not be 2"))
	})

	It("bootstraps a three node cluster without an extra rollout", func() {
		applyYAML(clusterManifest(3, ""))

		Eventually(func() string {
			return clusterField("{.status.phase}")
		}, bootstrapTimeout, pollInterval).ShouldNot(BeEmpty(), "operator never reconciled the cluster")

		expectConverged(3)

		By("checking the keystore came from cert-manager")
		Expect(mustKubectl("-n", testNamespace, "get", "certificate", clusterName+"-tls",
			"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}")).To(Equal("True"))

		// A second revision here would mean the cluster rolled itself immediately after
		// bootstrapping — the failure mode that moving topology out of the pod template fixed.
		By("checking the StatefulSet has exactly one revision")
		revs := mustKubectl("-n", testNamespace, "get", "controllerrevision",
			"-o", "jsonpath={.items[*].metadata.name}")
		Expect(strings.Fields(revs)).To(HaveLen(1), "the cluster rolled itself after bootstrap")
	})

	It("serves reads and writes through the client service", func() {
		withPortForward(func(baseURL string) {
			kvSet(baseURL, "e2e/key", "before-scale")
			Expect(kvGet(baseURL, "e2e/key")).To(Equal("before-scale"))
		})
	})

	It("rejects edits to fields that are fixed at bootstrap", func() {
		out, err := kubectl("-n", testNamespace, "patch", "kahunacluster", clusterName,
			"--type=merge", "-p", `{"spec":{"partitions":5}}`)
		Expect(err).To(HaveOccurred(), "partitions was mutable: %s", out)
		Expect(out).To(ContainSubstring("immutable"))
	})

	It("scales up one node at a time without disturbing the existing ones", func() {
		before := podUIDs()
		Expect(before).To(HaveLen(3))

		mustKubectl("-n", testNamespace, "patch", "kahunacluster", clusterName,
			"--type=merge", "-p", `{"spec":{"replicas":5}}`)

		expectConverged(5)

		By("checking the founding nodes were never restarted")
		after := podUIDs()
		for name, uid := range before {
			Expect(after).To(HaveKeyWithValue(name, uid),
				"%s was restarted by the scale-up", name)
		}

		By("checking the new nodes joined rather than formed a cluster")
		for _, pod := range []string{clusterName + "-3", clusterName + "-4"} {
			logs, err := kubectl("-n", testNamespace, "logs", pod)
			Expect(err).NotTo(HaveOccurred())
			Expect(logs).To(ContainSubstring("--join-existing"),
				"%s did not join the running cluster", pod)
		}
	})

	It("scales down one node at a time, decommissioning each before removing it", func() {
		mustKubectl("-n", testNamespace, "patch", "kahunacluster", clusterName,
			"--type=merge", "-p", `{"spec":{"replicas":3}}`)

		expectConverged(3)

		// A node that was asked to leave is out of the roster before its pod goes away, so the
		// operator never has to wait for the cluster to notice a member is missing.
		By("checking the operator asked nodes to leave rather than letting them be evicted")
		logs, err := kubectl("-n", operatorNamespace, "logs",
			"deployment/kahuna-operator-controller-manager", "--tail=2000")
		Expect(err).NotTo(HaveOccurred())
		Expect(logs).To(ContainSubstring("node left the cluster"))
		Expect(logs).NotTo(ContainSubstring("falling back to eviction"))

		Eventually(func() string {
			return clusterField("{.status.replicas}")
		}, membershipTimeout, pollInterval).Should(Equal("3"))
	})

	It("still serves the data written before the scale", func() {
		withPortForward(func(baseURL string) {
			Expect(kvGet(baseURL, "e2e/key")).To(Equal("before-scale"))
		})
	})

	It("rolls the cluster when configuration changes, holding quorum throughout", func() {
		before := podUIDs()

		mustKubectl("-n", testNamespace, "patch", "kahunacluster", clusterName,
			"--type=merge", "-p", `{"spec":{"logLevel":"Debug"}}`)

		// The roll is observable as every pod getting a new UID.
		Eventually(func() bool {
			after := podUIDs()
			if len(after) != len(before) {
				return false
			}
			for name, uid := range before {
				if after[name] == uid {
					return false
				}
			}
			return true
		}, membershipTimeout, pollInterval).Should(BeTrue(), "the cluster never rolled")

		expectConverged(3)

		By("checking no node crash-looped through the roll")
		restarts := mustKubectl("-n", testNamespace, "get", "pods",
			"-l", "app.kubernetes.io/instance="+clusterName,
			"-o", "jsonpath={.items[*].status.containerStatuses[0].restartCount}")
		for _, r := range strings.Fields(restarts) {
			Expect(r).To(Equal("0"), "a node restarted during the rolling update")
		}
	})

	// The cluster was founded at 3 nodes without a factor, so every data partition starts fully
	// replicated. From Kahuna 1.12.3 the rebalancer converts such partitions to replica sets, which
	// is what lets the autoscaler grow a cluster founded at its factor without adding copies.
	It("converts fully replicated partitions once a factor is set, then places a new node", func() {
		before := podUIDs()
		mustKubectl("-n", testNamespace, "patch", "kahunacluster", clusterName,
			"--type=merge", "-p", `{"spec":{"replication":{"factor":3}}}`)

		// The factor is a server flag, so the patch rolls every pod. A port-forward opened before
		// the roll ends would die with the pod it is bound to.
		Eventually(func() bool {
			after := podUIDs()
			if len(after) != len(before) {
				return false
			}
			for name, uid := range before {
				if after[name] == uid {
					return false
				}
			}
			return true
		}, membershipTimeout, pollInterval).Should(BeTrue(), "the factor never rolled the cluster")
		expectConverged(3)

		withPortForward(func(baseURL string) {
			By("waiting for every data partition to get a replica set")
			Eventually(func() ([]int32, error) {
				p, err := placement(baseURL)
				if err != nil {
					return nil, err
				}
				if p.ReplicationFactor != 3 || !p.RebalancerEnabled {
					return nil, fmt.Errorf("factor %d, rebalancer %t", p.ReplicationFactor, p.RebalancerEnabled)
				}
				return p.FullyReplicated(), nil
			}, membershipTimeout, pollInterval).Should(BeEmpty(), "partitions stayed fully replicated")
		})

		mustKubectl("-n", testNamespace, "patch", "kahunacluster", clusterName,
			"--type=merge", "-p", `{"spec":{"replicas":4}}`)
		expectConverged(4)

		withPortForward(func(baseURL string) {
			By("checking each data partition holds exactly 3 voters on 4 nodes")
			Eventually(func() (map[int32]int, error) {
				p, err := placement(baseURL)
				if err != nil {
					return nil, err
				}
				off := map[int32]int{}
				for _, part := range p.Partitions {
					if part.PartitionID == 0 {
						continue
					}
					voters := 0
					for _, r := range part.Replicas {
						if r.Role == kahuna.RoleVoter {
							voters++
						}
					}
					if voters != 3 || len(part.Replicas) != 3 {
						off[part.PartitionID] = len(part.Replicas)
					}
				}
				return off, nil
			}, membershipTimeout, pollInterval).Should(BeEmpty(), "a data partition is not at 3 voter replicas")

			Expect(kvGet(baseURL, "e2e/key")).To(Equal("before-scale"))
		})
	})

	// Kind's only StorageClass, local-path, cannot give ReadWriteMany, so the operator's own claim
	// would never bind here. A hostPath volume is a correct shared volume on a single Kind node,
	// because every pod runs on that node.
	//
	// The cluster has 4 nodes and replication factor 3, so no node hosts every data partition.
	// Kahuna 1.12.5 and later compose the backup from each node's share of the partitions. The
	// server's chain validation refuses a chain that misses a partition (RestrictedCoverage), as
	// restore and seeding do, so a chain that validates is a chain that covers the cluster.
	It("takes backups that cover every partition under a replication factor", func() {
		withPortForward(func(baseURL string) {
			By("checking that no node hosts every data partition")
			Eventually(func() (string, error) {
				p, err := placement(baseURL)
				if err != nil {
					return "", err
				}
				return nodeHostingEveryPartition(p), nil
			}, membershipTimeout, pollInterval).Should(BeEmpty(), "one node hosts every data partition")
		})

		By("creating a ReadWriteMany hostPath volume and a claim bound to it")
		// A volume left by an earlier run on a reused cluster is Released, and no new claim binds it.
		mustKubectl("delete", "pv", backupVolumeName, "--ignore-not-found")
		applyYAML(fmt.Sprintf(`
apiVersion: v1
kind: PersistentVolume
metadata:
  name: %[1]s
spec:
  capacity:
    storage: 1Gi
  accessModes: [ReadWriteMany]
  storageClassName: manual
  persistentVolumeReclaimPolicy: Retain
  hostPath:
    path: /var/local/%[1]s
    type: DirectoryOrCreate
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  accessModes: [ReadWriteMany]
  storageClassName: manual
  volumeName: %[1]s
  resources:
    requests:
      storage: 1Gi
`, backupVolumeName, testNamespace))

		before := podUIDs()
		mustKubectl("-n", testNamespace, "patch", "kahunacluster", clusterName, "--type=merge", "-p",
			fmt.Sprintf(`{"spec":{"backup":{"schedule":"@daily","incrementalSchedule":"* * * * *","volume":{"claimName":%q}}}}`,
				backupVolumeName))

		By("waiting for the rollout and the first full backup")
		// The backup flags roll every pod. A port-forward opened before the roll ends would die
		// with the pod it is bound to.
		expectRolled(before)
		expectConverged(4)
		Eventually(func() string {
			return clusterField("{.status.backup.lastBackupID}")
		}, 2*membershipTimeout, pollInterval).ShouldNot(BeEmpty(), "no backup was taken")

		Expect(clusterField(`{.status.conditions[?(@.type=="BackupVolumeReady")].status}`)).To(Equal("True"))
		Expect(clusterField(`{.status.conditions[?(@.type=="BackupFailing")].status}`)).To(Equal("False"))
		full := clusterField("{.status.backup.lastBackupID}")

		withPortForward(func(baseURL string) {
			By("checking the full backup's chain covers every partition")
			chain, err := backupChain(baseURL, full)
			Expect(err).NotTo(HaveOccurred())
			Expect(chain).NotTo(BeEmpty())
			Expect(chain[0].Type).To(Equal(kahuna.BackupTypeFull))

			By("writing under load while an incremental backup is taken")
			stop := make(chan struct{})
			var written atomic.Int64
			done := make(chan struct{})
			go func() {
				defer GinkgoRecover()
				defer close(done)
				for i := 0; ; i++ {
					select {
					case <-stop:
						return
					default:
					}
					if kvTrySet(baseURL, fmt.Sprintf("e2e/load/%d", i), "under-backup") == nil {
						written.Add(1)
					}
				}
			}()
			defer func() {
				close(stop)
				<-done
			}()

			Eventually(func() string {
				return clusterField("{.status.backup.lastBackupType}")
			}, membershipTimeout, pollInterval).Should(Equal(kahuna.BackupTypeIncremental),
				"no incremental backup was taken")
			incremental := clusterField("{.status.backup.lastBackupID}")

			By("checking the incremental's chain covers every partition")
			chain, err = backupChain(baseURL, incremental)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(chain)).To(BeNumerically(">=", 2), "the incremental is not on a chain")
			Expect(chain[0].Type).To(Equal(kahuna.BackupTypeFull))
			Expect(chain[len(chain)-1].BackupID).To(Equal(incremental))

			Expect(written.Load()).To(BeNumerically(">", 0), "no write succeeded during the backups")
			Expect(clusterField(`{.status.conditions[?(@.type=="BackupFailing")].status}`)).To(Equal("False"))
			Expect(kvGet(baseURL, "e2e/key")).To(Equal("before-scale"))
		})
		expectConverged(4)
	})

	// The other specs run kahunaImage from the start. This one founds a second cluster on the
	// previous release and then moves it to kahunaImage, which is how a user upgrades. The cluster
	// above stays up, so this spec runs at its end, where no spec counts the namespace's revisions.
	It("upgrades a running cluster to a new Kahuna release, keeping its data", func() {
		const upgraded = clusterName + "-upgrade"
		field := func(path string) string {
			return clusterFieldIn(testNamespace, "kahunacluster", upgraded, path)
		}

		applyYAML(clusterManifestFor(upgraded, kahunaPreviousImage, 3, ""))
		expectConvergedOf(upgraded, 3)
		Expect(field("{.status.currentImage}")).To(Equal(kahunaPreviousImage))

		withPortForwardTo(upgraded, func(baseURL string) {
			kvSet(baseURL, "e2e/upgrade", "before-upgrade")
		})

		before := podFieldOf(upgraded, "{.metadata.uid}")
		Expect(before).To(HaveLen(3))
		mustKubectl("-n", testNamespace, "patch", "kahunacluster", upgraded,
			"--type=merge", "-p", fmt.Sprintf(`{"spec":{"image":%q}}`, kahunaImage))

		By("checking currentImage moves only once every node runs the new image")
		// currentImage is read before the pods, and a replaced pod never goes back to the old
		// image. So a cluster that reports the new image while a pod still runs the old one is a
		// real failure, not a race between the two reads.
		Eventually(func() string {
			current := field("{.status.currentImage}")
			images := podImagesOf(upgraded)
			if current == kahunaImage {
				for pod, image := range images {
					Expect(image).To(Equal(kahunaImage),
						"currentImage is %s while %s still runs %s", current, pod, image)
				}
			}
			return current
		}, membershipTimeout, pollInterval).Should(Equal(kahunaImage), "currentImage never moved to the new image")

		expectRolledOf(upgraded, before)
		expectConvergedOf(upgraded, 3)

		By("checking no node crash-looped through the upgrade")
		restarts := podFieldOf(upgraded, "{.status.containerStatuses[0].restartCount}")
		Expect(restarts).To(HaveLen(3))
		for pod, r := range restarts {
			Expect(r).To(Equal("0"), "%s restarted during the upgrade", pod)
		}

		withPortForwardTo(upgraded, func(baseURL string) {
			By("checking the data written on the previous release survived")
			Expect(kvGet(baseURL, "e2e/upgrade")).To(Equal("before-upgrade"))
			kvSet(baseURL, "e2e/upgrade", "after-upgrade")
			Expect(kvGet(baseURL, "e2e/upgrade")).To(Equal("after-upgrade"))
		})
	})
})

// expectRolled waits until every pod in before was replaced, which is how a roll is observed.
func expectRolled(before map[string]string) {
	GinkgoHelper()
	expectRolledOf(clusterName, before)
}

// expectRolledOf is expectRolled for the named cluster.
func expectRolledOf(cluster string, before map[string]string) {
	GinkgoHelper()
	Eventually(func() bool {
		after := podFieldOf(cluster, "{.metadata.uid}")
		if len(after) != len(before) {
			return false
		}
		for name, uid := range before {
			if after[name] == uid {
				return false
			}
		}
		return true
	}, membershipTimeout, pollInterval).Should(BeTrue(), "the cluster never rolled")
}

// nodeHostingEveryPartition returns a node that hosts a replica of every active data partition,
// or "" when there is none. Partition 0 is the system partition, which spans every node.
func nodeHostingEveryPartition(p *kahuna.Placement) string {
	hosted := map[string]int{}
	data := 0
	for _, part := range p.Partitions {
		if part.PartitionID == 0 || part.State != kahuna.PartitionStateActive {
			continue
		}
		data++
		for _, r := range part.Replicas {
			hosted[r.Endpoint]++
		}
	}
	for endpoint, n := range hosted {
		if n == data {
			return endpoint
		}
	}
	return ""
}

// backupChain resolves and validates the chain that ends at leaf, as a restore would. The server
// refuses a chain that misses a partition with RestrictedCoverage.
func backupChain(baseURL, leaf string) ([]kahuna.BackupInfo, error) {
	resp, err := http.Get(baseURL + "/v1/backups/" + leaf + "/chain")
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("chain of %s returned HTTP %d (%s): %s",
			leaf, resp.StatusCode, resp.Header.Get("X-Kahuna-Backup-Outcome"), body)
	}
	var chain []kahuna.BackupInfo
	if err := json.NewDecoder(resp.Body).Decode(&chain); err != nil {
		return nil, err
	}
	return chain, nil
}

// placement reads the committed replica sets through the client service.
func placement(baseURL string) (*kahuna.Placement, error) {
	resp, err := http.Get(baseURL + "/v1/cluster/placement")
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("placement returned HTTP %d", resp.StatusCode)
	}
	var p kahuna.Placement
	if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
		return nil, err
	}
	return &p, nil
}

// clusterFieldIn reads a jsonpath expression off an arbitrary resource.
func clusterFieldIn(namespace, kind, name, path string) string {
	out, err := kubectl("-n", namespace, "get", kind, name, "-o", "jsonpath="+path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}
