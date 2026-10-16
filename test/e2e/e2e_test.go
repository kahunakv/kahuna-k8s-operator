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
	"net/http"
	"os/exec"
	"strings"
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
	out, err := kubectl("-n", testNamespace, "get", "pods",
		"-l", "app.kubernetes.io/instance="+clusterName,
		"-o", `jsonpath={range .items[*]}{.metadata.name}={.metadata.uid} {end}`)
	uids := map[string]string{}
	if err != nil {
		return uids
	}
	for _, kv := range strings.Fields(out) {
		if name, uid, ok := strings.Cut(kv, "="); ok {
			uids[name] = uid
		}
	}
	return uids
}

// withPortForward opens a port-forward to the client Service and runs fn against it. Pod and
// Service IPs inside Kind are not routable from the test process, so this is how the suite talks
// to the cluster it just built.
func withPortForward(fn func(baseURL string)) {
	const localPort = 32070
	cmd := exec.Command("kubectl", "-n", testNamespace, "port-forward",
		"svc/"+clusterName, fmt.Sprintf("%d:2070", localPort))
	ExpectWithOffset(1, cmd.Start()).To(Succeed())
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	baseURL := fmt.Sprintf("http://localhost:%d", localPort)
	// The forward takes a moment to bind; poll rather than sleep a fixed amount.
	EventuallyWithOffset(1, func() error {
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
	body := map[string]any{
		"key":        key,
		"value":      base64.StdEncoding.EncodeToString([]byte(value)),
		"durability": 1,
	}
	raw, _ := json.Marshal(body)
	resp, err := http.Post(baseURL+"/v1/kv/try-set", "application/json", strings.NewReader(string(raw)))
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	defer func() { _ = resp.Body.Close() }()

	var out struct {
		Type int `json:"type"`
	}
	ExpectWithOffset(1, json.NewDecoder(resp.Body).Decode(&out)).To(Succeed())
	// 0 is KeyValueResponseType.Set.
	ExpectWithOffset(1, out.Type).To(Equal(0), "set did not succeed")
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

// clusterManifest renders a KahunaCluster at the given size.
func clusterManifest(replicas int, extraArgs string) string {
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
  partitions: 3
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
`, clusterName, testNamespace, replicas, kahunaImage, extraArgs)
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
	show("-n", testNamespace, "get", "kahunacluster", clusterName, "-o", "yaml")
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
	EventuallyWithOffset(1, func() string {
		return fmt.Sprintf("%s/%s/%s",
			clusterField("{.status.voters}"),
			clusterField("{.status.readyReplicas}"),
			clusterField("{.status.phase}"))
	}, membershipTimeout, pollInterval).Should(
		Equal(fmt.Sprintf("%d/%d/Running", replicas, replicas)),
		"cluster never converged at %d nodes", replicas)
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
})

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
