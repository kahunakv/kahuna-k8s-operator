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
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"software.sslmate.com/src/go-pkcs12"

	kahunav1alpha1 "github.com/kahunakv/k8s-operator/api/v1alpha1"
)

// certManagerGVK identifies the resource the operator creates when asked to provision TLS through
// cert-manager. It is handled as an unstructured object on purpose: the shape used here is small
// and stable, and depending on the cert-manager Go module would drag its whole API surface — and
// its version skew — into an operator that only ever writes these few fields.
var certManagerGVK = schema.GroupVersionKind{
	Group:   "cert-manager.io",
	Version: "v1",
	Kind:    "Certificate",
}

// certManagerKeystoreKey is the key cert-manager writes a PKCS#12 keystore under. It is fixed by
// cert-manager, not configurable.
const certManagerKeystoreKey = "keystore.p12"

// certManagerCertificateAnnotation is set by cert-manager on every Secret it issues and names the
// Certificate the Secret belongs to.
const certManagerCertificateAnnotation = "cert-manager.io/certificate-name"

// keystorePasswordKey is the key inside the generated password Secret.
const keystorePasswordKey = "password"

// certPasswordSecretName is the Secret holding the generated keystore password.
func certPasswordSecretName(cluster *kahunav1alpha1.KahunaCluster) string {
	return cluster.Name + "-tls-password"
}

// tlsPasswordRef resolves where the keystore password lives, whether the user supplied it or the
// operator generated one for cert-manager. cert-manager requires a password to build a PKCS#12
// keystore, so the certManager path always has one.
func tlsPasswordRef(cluster *kahunav1alpha1.KahunaCluster) *corev1.SecretKeySelector {
	if cluster.Spec.TLS.PasswordSecretRef != nil {
		return cluster.Spec.TLS.PasswordSecretRef
	}
	if cluster.Spec.TLS.CertManager != nil {
		return &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: certPasswordSecretName(cluster)},
			Key:                  keystorePasswordKey,
		}
	}
	return nil
}

// certificateDNSNames are the identities the Raft and client endpoints are reached by.
//
// The per-pod entry is a wildcard rather than one name per ordinal so that scaling the cluster
// does not invalidate the certificate: a fixed list would have to be rewritten on every scale,
// and each rewrite reissues the certificate and restarts the nodes that mount it.
func certificateDNSNames(cluster *kahunav1alpha1.KahunaCluster) []string {
	peer := peerServiceName(cluster)
	svc := clientServiceName(cluster)
	ns := cluster.Namespace
	return []string{
		fmt.Sprintf("*.%s.%s.svc.cluster.local", peer, ns),
		fmt.Sprintf("%s.%s.svc.cluster.local", peer, ns),
		fmt.Sprintf("%s.%s.svc.cluster.local", svc, ns),
		fmt.Sprintf("%s.%s.svc", svc, ns),
		fmt.Sprintf("%s.%s", svc, ns),
		svc,
	}
}

// desiredCertificate builds the cert-manager Certificate backing the cluster's keystore.
func desiredCertificate(cluster *kahunav1alpha1.KahunaCluster) *unstructured.Unstructured {
	cm := cluster.Spec.TLS.CertManager
	pw := tlsPasswordRef(cluster)

	issuer := map[string]any{"name": cm.IssuerRef.Name}
	if cm.IssuerRef.Kind != "" {
		issuer["kind"] = cm.IssuerRef.Kind
	}
	if cm.IssuerRef.Group != "" {
		issuer["group"] = cm.IssuerRef.Group
	}

	dns := certificateDNSNames(cluster)
	names := make([]any, 0, len(dns))
	for _, n := range dns {
		names = append(names, n)
	}

	spec := map[string]any{
		"secretName": certificateName(cluster),
		"issuerRef":  issuer,
		"commonName": fmt.Sprintf("%s.%s.svc", clientServiceName(cluster), cluster.Namespace),
		"dnsNames":   names,
		// Kahuna binds a PKCS#12 keystore, so the certificate has to be issued as one; the
		// tls.crt/tls.key pair cert-manager writes alongside it is unused here.
		"keystores": map[string]any{
			"pkcs12": map[string]any{
				"create": true,
				"passwordSecretRef": map[string]any{
					"name": pw.Name,
					"key":  pw.Key,
				},
			},
		},
	}
	if cm.Duration != nil {
		spec["duration"] = cm.Duration.Duration.String()
	}
	if cm.RenewBefore != nil {
		spec["renewBefore"] = cm.RenewBefore.Duration.String()
	}

	obj := &unstructured.Unstructured{Object: map[string]any{"spec": spec}}
	obj.SetGroupVersionKind(certManagerGVK)
	obj.SetName(certificateName(cluster))
	obj.SetNamespace(cluster.Namespace)
	obj.SetLabels(podLabels(cluster))
	return obj
}

// reconcileKeystorePassword creates the password Secret backing a cert-manager keystore.
//
// It is created once and never rewritten: the password is baked into the issued keystore, so
// regenerating it on a later reconcile would leave every node unable to open its own certificate.
func (r *KahunaClusterReconciler) reconcileKeystorePassword(ctx context.Context, cluster *kahunav1alpha1.KahunaCluster) error {
	// Only the generated Secret is managed here; a user-supplied one belongs to the user.
	if cluster.Spec.TLS.CertManager == nil || cluster.Spec.TLS.PasswordSecretRef != nil {
		return nil
	}

	name := certPasswordSecretName(cluster)
	existing := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, existing)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Errorf("generating keystore password: %w", err)
	}

	secret := &corev1.Secret{
		ObjectMeta: objectMeta(cluster, name),
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{keystorePasswordKey: []byte(base64.RawURLEncoding.EncodeToString(buf))},
	}
	if err := controllerutil.SetControllerReference(cluster, secret, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, secret); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

// reconcileCertificate ensures the cert-manager Certificate exists when the spec asks for one.
//
// A missing cert-manager installation is reported rather than retried into a crash loop: the
// operator cannot install it, so the useful outcome is a status message naming the cause.
func (r *KahunaClusterReconciler) reconcileCertificate(ctx context.Context, cluster *kahunav1alpha1.KahunaCluster) error {
	if cluster.Spec.TLS.CertManager == nil {
		return nil
	}

	desired := desiredCertificate(cluster)
	spec := desired.Object["spec"]
	lbls := desired.GetLabels()

	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(certManagerGVK)
	obj.SetName(desired.GetName())
	obj.SetNamespace(desired.GetNamespace())

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, obj, func() error {
		obj.SetLabels(lbls)
		obj.Object["spec"] = spec
		return controllerutil.SetControllerReference(cluster, obj, r.Scheme)
	})
	if err != nil && apimeta.IsNoMatchError(err) {
		return fmt.Errorf("spec.tls.certManager is set but cert-manager is not installed in this cluster: %w", err)
	}
	if err != nil {
		return err
	}
	return r.replaceStaleKeystore(ctx, cluster)
}

// replaceStaleKeystore deletes a cert-manager keystore Secret that its password cannot open, so
// that cert-manager issues a new one.
//
// cert-manager does not reissue a keystore when its password changes. Recreating a KahunaCluster
// under the same name causes exactly that: the cluster owns the password Secret, so the password is
// collected with it and generated anew, while cert-manager sets no owner on the keystore Secret.
// That Secret outlives the cluster, still satisfies the recreated Certificate, and every pod then
// fails to open it.
//
// The keystore is tested with the password exactly as cert-manager reads it, so a reissue always
// cures the mismatch and the delete cannot repeat. Only a Secret cert-manager issued for this
// cluster's Certificate is deleted; anything else is left for its owner.
func (r *KahunaClusterReconciler) replaceStaleKeystore(ctx context.Context, cluster *kahunav1alpha1.KahunaCluster) error {
	if cluster.Spec.TLS.CertManager == nil {
		return nil
	}
	k, err := r.readKeystore(ctx, cluster)
	if err != nil || k == nil {
		return err
	}
	if k.secret.Annotations[certManagerCertificateAnnotation] != certificateName(cluster) || k.opensWith(string(k.password)) {
		return nil
	}

	// The preconditions pin the delete to the Secret just tested: if cert-manager rewrote it in
	// the meantime, the new keystore is left alone.
	err = r.Delete(ctx, k.secret, client.Preconditions{UID: &k.secret.UID, ResourceVersion: &k.secret.ResourceVersion})
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("deleting stale keystore Secret: %w", err)
	}
	logf.FromContext(ctx).Info("Deleted keystore Secret its password could not open, for cert-manager to reissue",
		"secret", k.secret.Name, "passwordSecret", tlsPasswordRef(cluster).Name)
	return nil
}

// keystoreMaterial is the keystore Secret and the password meant to open it.
type keystoreMaterial struct {
	secret   *corev1.Secret
	keystore []byte
	password []byte
}

// readKeystore fetches the keystore and its password. It returns nil when either is missing,
// which leaves nothing to test yet.
func (r *KahunaClusterReconciler) readKeystore(ctx context.Context, cluster *kahunav1alpha1.KahunaCluster) (*keystoreMaterial, error) {
	ref := tlsPasswordRef(cluster)
	if ref == nil {
		return nil, nil
	}

	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: tlsSecretName(cluster), Namespace: cluster.Namespace}, secret); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	keystore, ok := secret.Data[keystoreKey(cluster)]
	if !ok {
		return nil, nil
	}

	pw := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: cluster.Namespace}, pw); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	password, ok := pw.Data[ref.Key]
	if !ok {
		return nil, nil
	}
	return &keystoreMaterial{secret: secret, keystore: keystore, password: password}, nil
}

// opensWith reports whether password opens the keystore. Only a wrong password counts as a
// failure: a keystore this code cannot parse is assumed to be fine, so nothing is deleted or
// reported on a guess.
func (k *keystoreMaterial) opensWith(password string) bool {
	_, _, _, err := pkcs12.DecodeChain(k.keystore, password)
	return !errors.Is(err, pkcs12.ErrIncorrectPassword)
}

// certificateReady reports whether cert-manager has issued the keystore. Until it has, the Secret
// the pods mount does not exist and the StatefulSet would sit unschedulable with no explanation.
// A keystore the pods' password cannot open is not ready either: every node would crash-loop on it.
func (r *KahunaClusterReconciler) certificateReady(ctx context.Context, cluster *kahunav1alpha1.KahunaCluster) (bool, string) {
	secret := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{Name: tlsSecretName(cluster), Namespace: cluster.Namespace}, secret)
	if apierrors.IsNotFound(err) {
		return false, fmt.Sprintf("waiting for keystore Secret %q", tlsSecretName(cluster))
	}
	if err != nil {
		return false, err.Error()
	}
	key := keystoreKey(cluster)
	if _, ok := secret.Data[key]; !ok {
		return false, fmt.Sprintf("Secret %q has no %q key", tlsSecretName(cluster), key)
	}

	k, err := r.readKeystore(ctx, cluster)
	if err != nil {
		return false, err.Error()
	}
	// The pods read the password through "$(cat ...)", which drops trailing newlines, so the
	// keystore is tested with the password as they will see it.
	if k != nil && !k.opensWith(strings.TrimRight(string(k.password), "\n")) {
		return false, fmt.Sprintf("keystore in Secret %q does not open with the password in Secret %q",
			tlsSecretName(cluster), tlsPasswordRef(cluster).Name)
	}
	return true, ""
}

// conditionKeystoreReady reports whether the PKCS#12 keystore the pods mount exists yet and opens
// with the password they are given.
const conditionKeystoreReady = "KeystoreReady"
