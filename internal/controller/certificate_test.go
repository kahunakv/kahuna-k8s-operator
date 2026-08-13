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
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	kahunav1alpha1 "github.com/kahunakv/k8s-operator/api/v1alpha1"
)

func certManagerCluster() *kahunav1alpha1.KahunaCluster {
	c := testCluster(3)
	c.Spec.TLS = kahunav1alpha1.TLSSpec{
		CertManager: &kahunav1alpha1.CertManagerSpec{
			IssuerRef: kahunav1alpha1.CertManagerIssuerRef{
				Name: "kahuna-ca", Kind: "ClusterIssuer", Group: "cert-manager.io",
			},
		},
	}
	return c
}

func TestCertificateRequestsAPKCS12Keystore(t *testing.T) {
	obj := desiredCertificate(certManagerCluster())

	create, found, err := unstructured.NestedBool(obj.Object, "spec", "keystores", "pkcs12", "create")
	if err != nil || !found || !create {
		t.Fatalf("certificate does not request a PKCS#12 keystore: %v", obj.Object["spec"])
	}
	// Kahuna binds a keystore, not a tls.crt/tls.key pair, so a certificate without one is
	// useless to it.
	name, _, _ := unstructured.NestedString(obj.Object, "spec", "keystores", "pkcs12", "passwordSecretRef", "name")
	if name != "kahuna-tls-password" {
		t.Errorf("password secret = %q, want kahuna-tls-password", name)
	}
	if got := obj.GetName(); got != testTLSSecret {
		t.Errorf("certificate name = %q, want kahuna-tls", got)
	}
	secretName, _, _ := unstructured.NestedString(obj.Object, "spec", "secretName")
	if secretName != testTLSSecret {
		t.Errorf("secretName = %q, want kahuna-tls", secretName)
	}
}

// The per-pod SAN is a wildcard so that scaling does not reissue the certificate and restart the
// nodes that mount it.
func TestCertificateCoversPodsWithAWildcard(t *testing.T) {
	obj := desiredCertificate(certManagerCluster())
	names, _, _ := unstructured.NestedStringSlice(obj.Object, "spec", "dnsNames")

	want := "*.kahuna-peer.kv.svc.cluster.local"
	var found bool
	for _, n := range names {
		if n == want {
			found = true
		}
		if strings.HasPrefix(n, "kahuna-0.") {
			t.Errorf("certificate pins a per-ordinal name %q; a scale would reissue it", n)
		}
	}
	if !found {
		t.Errorf("dnsNames %v missing the per-pod wildcard %q", names, want)
	}

	// The client Service is reached under its own names too.
	for _, w := range []string{"kahuna.kv.svc.cluster.local", "kahuna"} {
		var ok bool
		for _, n := range names {
			if n == w {
				ok = true
			}
		}
		if !ok {
			t.Errorf("dnsNames %v missing client name %q", names, w)
		}
	}
}

func TestCertificateCarriesIssuerAndLifetime(t *testing.T) {
	c := certManagerCluster()
	c.Spec.TLS.CertManager.Duration = &metav1.Duration{Duration: 720 * time.Hour}
	c.Spec.TLS.CertManager.RenewBefore = &metav1.Duration{Duration: 240 * time.Hour}

	obj := desiredCertificate(c)
	kind, _, _ := unstructured.NestedString(obj.Object, "spec", "issuerRef", "kind")
	if kind != "ClusterIssuer" {
		t.Errorf("issuerRef.kind = %q, want ClusterIssuer", kind)
	}
	if d, _, _ := unstructured.NestedString(obj.Object, "spec", "duration"); d != "720h0m0s" {
		t.Errorf("duration = %q", d)
	}
	if d, _, _ := unstructured.NestedString(obj.Object, "spec", "renewBefore"); d != "240h0m0s" {
		t.Errorf("renewBefore = %q", d)
	}
}

// cert-manager writes its keystore under a fixed key, so the server must be pointed at that name
// rather than the default one used for a hand-supplied keystore.
func TestCertManagerKeystoreIsMountedAndPassedToTheServer(t *testing.T) {
	c := certManagerCluster()

	if got := keystoreKey(c); got != certManagerKeystoreKey {
		t.Errorf("keystoreKey = %q, want %q", got, certManagerKeystoreKey)
	}
	args := staticArgs(c)
	want := tlsMountPath + "/" + certManagerKeystoreKey
	if got := argValues(args, "--https-certificate"); len(got) != 1 || got[0] != want {
		t.Errorf("--https-certificate = %v, want [%s]", got, want)
	}

	// The generated password must actually reach the pod, or the server cannot open the keystore.
	var mounted bool
	for _, m := range podVolumeMounts(c) {
		if m.Name == tlsPasswordVolumeName {
			mounted = true
		}
	}
	if !mounted {
		t.Error("keystore password volume is not mounted; the server could not open the keystore")
	}

	var vol *corev1.Volume
	for i, v := range podVolumes(c) {
		if v.Name == tlsPasswordVolumeName {
			vol = &podVolumes(c)[i]
		}
	}
	if vol == nil || vol.Secret == nil || vol.Secret.SecretName != certPasswordSecretName(c) {
		t.Errorf("password volume does not reference the generated secret: %+v", vol)
	}
}

// A hand-supplied keystore keeps the user's own key name and password secret.
func TestSuppliedKeystoreIsUsedVerbatim(t *testing.T) {
	c := testCluster(3)
	c.Spec.TLS.KeystoreKey = "custom.pfx"
	c.Spec.TLS.PasswordSecretRef = &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "my-pw"},
		Key:                  "pw",
	}

	if got := keystoreKey(c); got != "custom.pfx" {
		t.Errorf("keystoreKey = %q, want custom.pfx", got)
	}
	ref := tlsPasswordRef(c)
	if ref == nil || ref.Name != "my-pw" || ref.Key != "pw" {
		t.Errorf("password ref = %+v, want my-pw/pw", ref)
	}
	if got := tlsSecretName(c); got != testTLSSecret {
		t.Errorf("tlsSecretName = %q", got)
	}
}

// Without cert-manager and without a supplied password, no password volume is mounted at all —
// an unprotected keystore is a legitimate configuration.
func TestNoPasswordVolumeWhenNoneIsConfigured(t *testing.T) {
	c := testCluster(3)
	if ref := tlsPasswordRef(c); ref != nil {
		t.Errorf("password ref = %+v, want nil", ref)
	}
	for _, v := range podVolumes(c) {
		if v.Name == tlsPasswordVolumeName {
			t.Error("password volume mounted although no password is configured")
		}
	}
}
