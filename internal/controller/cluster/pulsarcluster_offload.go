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
//
// The offloader jars ship only in apachepulsar/pulsar-all, not the slim
// apachepulsar/pulsar image, so offload needs an offloader-capable image.
// This file does NOT derive one: a PulsarClusterSpec XValidation rule
// (api/cluster/v1alpha1/pulsarcluster_types.go) requires the user to set
// spec.image or spec.broker.image explicitly whenever spec.offload is set,
// since apachepulsar/pulsar-all:<pulsarVersion> is not guaranteed to be
// published for every pulsarVersion (e.g. milestone releases like
// 5.0.0-M1) and silently synthesizing that tag produced an
// ImagePullBackOff instead of a clear error.

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

	// confKeyGCSOffloadServiceAccountKeyFile names the FILESYSTEM PATH to the
	// GCS service-account JSON key file the offloader reads. Unlike AWS/Azure
	// (whose credential env vars ARE read as literal secret values), GCS
	// authenticates from a key file on disk - GOOGLE_APPLICATION_CREDENTIALS is
	// itself a path, not the credential content - so the operator mounts
	// credentialsSecretRef as a secret volume and points this key at the mount
	// path rather than injecting an env var.
	confKeyGCSOffloadServiceAccountKeyFile = "gcsManagedLedgerOffloadServiceAccountKeyFile"

	// confKeyAzureOffloadBucket is the Azure BlobStore ledger offload bucket
	// key - upstream broker.conf has no azure-prefixed key, unlike s3/gcs; the
	// jcloud azureblob provider reads the same generic
	// managedLedgerOffloadBucket key.
	confKeyAzureOffloadBucket = "managedLedgerOffloadBucket"

	// Offload credential env vars, read as literal secret values by the jcloud
	// BlobStore provider each driver selects. They are wired from
	// spec.offload.credentialsSecretRef, which must contain a key matching each
	// var name below for the selected driver. GCS is deliberately absent: it
	// authenticates from a mounted key file, not an env var (see
	// offloadCredentialVolumes).
	envAWSAccessKeyID        = "AWS_ACCESS_KEY_ID"
	envAWSSecretAccessKey    = "AWS_SECRET_ACCESS_KEY"
	envAzureStorageAccount   = "AZURE_STORAGE_ACCOUNT"
	envAzureStorageAccessKey = "AZURE_STORAGE_ACCESS_KEY"

	// offloadCredentialVolumeName / gcsOffloadKeyDir / gcsOffloadKeySecretKey /
	// gcsOffloadKeyPath place the GCS service-account JSON key file at a fixed,
	// well-known path inside the broker container. The referenced Secret must
	// carry the JSON under the gcsOffloadKeySecretKey key.
	offloadCredentialVolumeName = "offload-gcs-credentials"
	gcsOffloadKeyDir            = "/etc/pulsar/offload-gcs"
	gcsOffloadKeySecretKey      = "key.json"
	gcsOffloadKeyPath           = gcsOffloadKeyDir + "/" + gcsOffloadKeySecretKey
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
		if offload.CredentialsSecretRef != nil {
			cfg = setConfigDefault(cfg, confKeyGCSOffloadServiceAccountKeyFile, gcsOffloadKeyPath)
		}
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

// offloadCredentialEnv returns the env vars that wire spec.offload.
// credentialsSecretRef into the broker container so the offloader driver can
// authenticate, keyed by driver since each jcloud BlobStore provider reads a
// different credential env var pair. It expects the referenced Secret to
// contain a key named exactly like each returned env var. GCS is deliberately
// excluded: it authenticates from a mounted key file, not an env var (see
// offloadCredentialVolumes). Filesystem offload needs no credentials, and a
// nil offload or unset credentialsSecretRef yields no env vars.
func offloadCredentialEnv(offload *clusterv1alpha1.OffloadSpec) []corev1.EnvVar {
	if offload == nil || offload.CredentialsSecretRef == nil {
		return nil
	}

	switch offload.Driver {
	case offloadDriverAWSS3:
		return secretEnvVars(offload.CredentialsSecretRef.Name, envAWSAccessKeyID, envAWSSecretAccessKey)
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

// offloadCredentialVolumes returns the pod volume(s) that project
// spec.offload.credentialsSecretRef into the broker as a file, for drivers
// that authenticate from a key file on disk rather than an env value. Only GCS
// needs this today: gcsManagedLedgerOffloadServiceAccountKeyFile names a PATH
// to the service-account JSON, so the secret's gcsOffloadKeySecretKey entry is
// mounted at gcsOffloadKeyPath. AWS/Azure use env vars (offloadCredentialEnv);
// a nil offload or unset credentialsSecretRef yields no volumes.
func offloadCredentialVolumes(offload *clusterv1alpha1.OffloadSpec) []corev1.Volume {
	if !gcsKeyFileMounted(offload) {
		return nil
	}
	return []corev1.Volume{{
		Name: offloadCredentialVolumeName,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: offload.CredentialsSecretRef.Name,
				Items: []corev1.KeyToPath{
					{Key: gcsOffloadKeySecretKey, Path: gcsOffloadKeySecretKey},
				},
			},
		},
	}}
}

// offloadCredentialVolumeMounts returns the broker-container mount(s) paired
// with offloadCredentialVolumes, placing the GCS service-account key file at
// gcsOffloadKeyPath (the path gcsManagedLedgerOffloadServiceAccountKeyFile
// points at).
func offloadCredentialVolumeMounts(offload *clusterv1alpha1.OffloadSpec) []corev1.VolumeMount {
	if !gcsKeyFileMounted(offload) {
		return nil
	}
	return []corev1.VolumeMount{{
		Name:      offloadCredentialVolumeName,
		MountPath: gcsOffloadKeyDir,
		ReadOnly:  true,
	}}
}

// gcsKeyFileMounted reports whether a GCS service-account key file should be
// mounted: the driver is google-cloud-storage and a credentials secret is set.
func gcsKeyFileMounted(offload *clusterv1alpha1.OffloadSpec) bool {
	return offload != nil && offload.Driver == offloadDriverGCS && offload.CredentialsSecretRef != nil
}
