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

package cluster

import (
	"strconv"

	corev1 "k8s.io/api/core/v1"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
)

// This file wires PulsarCluster.spec.offload into the child Broker, mirroring
// the metadata-store wiring in pulsarcluster_metadata.go. Tiered-storage
// offload is cost/retention tiering for closed ledgers, NOT a backup
// mechanism: offloaded objects are only reachable through Pulsar's own
// managed-ledger metadata, and are deleted along with the topic that
// offloaded them (see docs/docs/backup-and-dr.md).

const (
	// offloadDriverAWSS3 / offloadDriverGCS / offloadDriverAzureBlob /
	// offloadDriverFilesystem mirror OffloadSpec.Driver's kubebuilder enum
	// exactly - they select the broker.conf keys each driver reads.
	offloadDriverAWSS3      = "aws-s3"
	offloadDriverGCS        = "google-cloud-storage"
	offloadDriverAzureBlob  = "azureblob"
	offloadDriverFilesystem = "filesystem"

	// confKeyManagedLedgerOffloadDriver / confKeyManagedLedgerOffloadThresholdBytes
	// are the driver-agnostic broker.conf keys every offload driver shares.
	confKeyManagedLedgerOffloadDriver         = "managedLedgerOffloadDriver"
	confKeyManagedLedgerOffloadThresholdBytes = "managedLedgerOffloadAutoTriggerSizeThresholdBytes"

	// S3 ledger offload keys (broker.conf "Ledger Offloading" section).
	confKeyS3OffloadBucket          = "s3ManagedLedgerOffloadBucket"
	confKeyS3OffloadRegion          = "s3ManagedLedgerOffloadRegion"
	confKeyS3OffloadServiceEndpoint = "s3ManagedLedgerOffloadServiceEndpoint"

	// Google Cloud Storage ledger offload keys.
	confKeyGCSOffloadBucket = "gcsManagedLedgerOffloadBucket"
	confKeyGCSOffloadRegion = "gcsManagedLedgerOffloadRegion"

	// confKeyAzureOffloadBucket is the Azure BlobStore ledger offload bucket
	// key - upstream broker.conf has no azure-prefixed key, unlike s3/gcs; the
	// jcloud azureblob provider reads the same generic
	// managedLedgerOffloadBucket key.
	confKeyAzureOffloadBucket = "managedLedgerOffloadBucket"

	// Offload credential env vars, read by the jcloud BlobStore provider each
	// driver selects. They are wired from spec.offload.credentialsSecretRef,
	// which must contain a key matching each var name below for the selected
	// driver.
	envAWSAccessKeyID               = "AWS_ACCESS_KEY_ID"
	envAWSSecretAccessKey           = "AWS_SECRET_ACCESS_KEY"
	envGoogleApplicationCredentials = "GOOGLE_APPLICATION_CREDENTIALS"
	envAzureStorageAccount          = "AZURE_STORAGE_ACCOUNT"
	envAzureStorageAccessKey        = "AZURE_STORAGE_ACCESS_KEY"

	// pulsarAllImageRepository is the Pulsar "full" image, which bundles the
	// tiered-storage offloader NARs under offloadersDirectory (./offloaders by
	// default) that the slim apachepulsar/pulsar image omits. A broker cannot
	// offload without them.
	pulsarAllImageRepository = "apachepulsar/pulsar-all"
)

// withBrokerOffloadDefaults sets managedLedgerOffloadDriver, its
// driver-specific bucket/region/endpoint keys, and the auto-trigger size
// threshold from spec.offload, unless the user already set any of them
// (mirrors withBrokerProxyMetadataDefaults's "never overwrite a user-set
// key" contract). A nil offload leaves cfg untouched.
func withBrokerOffloadDefaults(cfg map[string]string, offload *clusterv1alpha1.OffloadSpec) map[string]string {
	if offload == nil {
		return cfg
	}

	cfg = setConfigDefault(cfg, confKeyManagedLedgerOffloadDriver, offload.Driver)

	switch offload.Driver {
	case offloadDriverAWSS3:
		cfg = setConfigDefaultIfSet(cfg, confKeyS3OffloadBucket, offload.Bucket)
		cfg = setConfigDefaultIfSet(cfg, confKeyS3OffloadRegion, offload.Region)
		cfg = setConfigDefaultIfSet(cfg, confKeyS3OffloadServiceEndpoint, offload.Endpoint)
	case offloadDriverGCS:
		cfg = setConfigDefaultIfSet(cfg, confKeyGCSOffloadBucket, offload.Bucket)
		cfg = setConfigDefaultIfSet(cfg, confKeyGCSOffloadRegion, offload.Region)
	case offloadDriverAzureBlob:
		cfg = setConfigDefaultIfSet(cfg, confKeyAzureOffloadBucket, offload.Bucket)
	case offloadDriverFilesystem:
		// No bucket/region: filesystem offload writes to a local path
		// (fileSystemProfilePath/fileSystemURI), which OffloadSpec has no
		// dedicated field for yet.
	}

	if offload.OffloadThresholdBytes != nil {
		cfg = setConfigDefault(cfg, confKeyManagedLedgerOffloadThresholdBytes, strconv.FormatInt(*offload.OffloadThresholdBytes, 10))
	}

	return cfg
}

// setConfigDefaultIfSet is setConfigDefault guarded against an empty value,
// so an unset optional OffloadSpec field (e.g. Region when Endpoint is used
// instead) never plants an empty-string key a user could mistake for an
// explicit override.
func setConfigDefaultIfSet(cfg map[string]string, key, value string) map[string]string {
	if value == "" {
		return cfg
	}
	return setConfigDefault(cfg, key, value)
}

// pulsarAllImage builds the apachepulsar/pulsar-all image reference pinned to
// pulsarVersion, or "" when pulsarVersion is unset (mirrors
// clusterDefaultImage's own empty-version fallback).
func pulsarAllImage(pulsarVersion string) string {
	if pulsarVersion == "" {
		return ""
	}
	return pulsarAllImageRepository + ":" + pulsarVersion
}

// offloadCredentialEnv returns the env vars that wire spec.offload.
// credentialsSecretRef into the broker container so the offloader driver can
// authenticate, keyed by driver since each jcloud BlobStore provider reads a
// different credential env var pair. It expects the referenced Secret to
// contain a key named exactly like each returned env var. Filesystem offload
// needs no credentials, and a nil offload or unset credentialsSecretRef
// yields no env vars.
func offloadCredentialEnv(offload *clusterv1alpha1.OffloadSpec) []corev1.EnvVar {
	if offload == nil || offload.CredentialsSecretRef == nil {
		return nil
	}

	switch offload.Driver {
	case offloadDriverAWSS3:
		return secretEnvVars(offload.CredentialsSecretRef.Name, envAWSAccessKeyID, envAWSSecretAccessKey)
	case offloadDriverGCS:
		return secretEnvVars(offload.CredentialsSecretRef.Name, envGoogleApplicationCredentials)
	case offloadDriverAzureBlob:
		return secretEnvVars(offload.CredentialsSecretRef.Name, envAzureStorageAccount, envAzureStorageAccessKey)
	default:
		return nil
	}
}

// secretEnvVars builds one corev1.EnvVar per key, each sourced via
// secretKeyRef from secretName at the identically-named key.
func secretEnvVars(secretName string, keys ...string) []corev1.EnvVar {
	envVars := make([]corev1.EnvVar, 0, len(keys))
	for _, key := range keys {
		envVars = append(envVars, corev1.EnvVar{
			Name: key,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
					Key:                  key,
				},
			},
		})
	}
	return envVars
}
