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

package backup

import (
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	backupv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/backup/v1alpha1"
	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	backuptool "github.com/andrew01234567890/pulsar-operator/internal/backup"
	"github.com/andrew01234567890/pulsar-operator/internal/builder"
	oxiaurl "github.com/andrew01234567890/pulsar-operator/internal/metadata"
	"github.com/andrew01234567890/pulsar-operator/internal/objectstore"
)

const (
	// backupComponentName labels the export Job and its pods, keyed by the
	// Backup's own name (as instance) so several Backups in a namespace never
	// collide on selector labels.
	backupComponentName = "backup-export"

	// exportContainerName is the export Job's single container; the reconciler
	// reads this container's termination message to recover the ExportResult.
	exportContainerName = "backup-export"

	// exportJobBackoffLimit bounds the export Job's pod-level retries before
	// it is marked Failed. A backup is a one-shot capture, so a few retries
	// absorb a transient Oxia/object-store blip without spinning forever.
	exportJobBackoffLimit int32 = 3

	// exportJobActiveDeadlineSeconds is a hard backstop: the kubelet/job
	// controller terminates the Job with DeadlineExceeded if it hasn't
	// finished within this window. It bounds a pod that never starts (e.g. a
	// volume that can never mount) even in cases the reconciler's own
	// stuck-pod detection might miss, so a Backup can never hang forever.
	exportJobActiveDeadlineSeconds int64 = 3600

	// manifestKeySuffix is appended to a Backup's name to form the object key
	// the manifest is written to (under destination.prefix).
	manifestKeySuffix = ".manifest"

	// managerBinary is the operator image's entrypoint; the export Job runs it
	// with the `backup-export` subcommand.
	managerBinary = "/manager"

	// terminationMessagePath is the single source of truth for WHERE the
	// export tool writes its ExportResult and WHERE the kubelet reads the
	// container's termination message from: it is both the container's
	// TerminationMessagePath and the value passed to `--result-path`, so the
	// two can never drift out of sync.
	terminationMessagePath = "/dev/termination-log"
)

// Object-store credential env vars, wired from destination.credentialsSecretRef
// onto the export Job exactly as the tiered-storage offload feature wires them
// onto the broker (see internal/controller/cluster/pulsarcluster_offload.go),
// so both features authenticate object storage identically. GCS is absent: it
// authenticates from a mounted key FILE, not an env value (see
// destCredentialVolumes).
const (
	envAWSAccessKeyID        = "AWS_ACCESS_KEY_ID"
	envAWSSecretAccessKey    = "AWS_SECRET_ACCESS_KEY"
	envAzureStorageAccount   = "AZURE_STORAGE_ACCOUNT"
	envAzureStorageAccessKey = "AZURE_STORAGE_ACCESS_KEY"

	// envGoogleAppCredentials is the GCS SDK's credential env var. Unlike
	// AWS/Azure (whose env vars ARE the literal secret values), this holds a
	// PATH to the service-account JSON key file, so the operator mounts the
	// credentials secret as a volume and points this env at the mount path.
	envGoogleAppCredentials = "GOOGLE_APPLICATION_CREDENTIALS"

	backupCredentialVolumeName = "backup-gcs-credentials"
	gcsKeyDir                  = "/etc/pulsar/backup-gcs"
	gcsKeySecretKey            = "key.json"
	gcsKeyPath                 = gcsKeyDir + "/" + gcsKeySecretKey
)

// backupJobName deterministically names the export Job after its owning Backup.
func backupJobName(backupName string) string {
	return backupName + "-export"
}

// manifestObjectKey is the object key (relative to destination.prefix) the
// manifest is written to for a Backup.
func manifestObjectKey(backup *backupv1alpha1.Backup) string {
	return backup.Name + manifestKeySuffix
}

// destConfig maps a BackupDestination onto the objectstore.Config the export
// tool and the reconciler both build from it. Credentials are deliberately
// absent - they are wired onto the Job as env/volume, not carried here.
func destConfig(dest backupv1alpha1.BackupDestination) objectstore.Config {
	return objectstore.Config{
		Driver:   dest.Driver,
		Bucket:   dest.Bucket,
		Region:   dest.Region,
		Endpoint: dest.Endpoint,
		Prefix:   dest.Prefix,
	}
}

// oxiaExportAddress returns the host:port the export Job connects its Oxia
// client to: the cluster's public oxia-server Service (never the coordinator,
// which only assigns shards), derived exactly as the umbrella derives
// metadataStoreUrl - PublicServiceName(<cluster>-oxia) on the server port.
func oxiaExportAddress(cluster *clusterv1alpha1.PulsarCluster) string {
	svc := oxiaurl.PublicServiceName(cluster.Name + "-oxia")
	return fmt.Sprintf("%s:%d", svc, oxiaurl.ServerPort)
}

// buildExportJob renders the Job that runs the operator image's
// `manager backup-export` subcommand against the cluster's Oxia store and
// uploads the manifest to the Backup's destination. It is a pure function of
// its inputs so it is unit-testable without a client; the caller sets the
// owner reference and creates it.
func buildExportJob(backup *backupv1alpha1.Backup, cluster *clusterv1alpha1.PulsarCluster, image string) *batchv1.Job {
	labels := builder.Labels(backup.Name, backupComponentName)
	name := backupJobName(backup.Name)
	dest := backup.Spec.Destination

	args := []string{
		backuptool.ExportCommandName,
		"--oxia", oxiaExportAddress(cluster),
		"--dest-driver", dest.Driver,
		"--dest-key", manifestObjectKey(backup),
		// Pass the result path explicitly (rather than relying on the CLI's
		// own default matching this container's TerminationMessagePath) so the
		// two are provably the same constant.
		"--result-path", terminationMessagePath,
	}
	args = appendFlagIfSet(args, "--dest-bucket", dest.Bucket)
	args = appendFlagIfSet(args, "--dest-region", dest.Region)
	args = appendFlagIfSet(args, "--dest-endpoint", dest.Endpoint)
	args = appendFlagIfSet(args, "--dest-prefix", dest.Prefix)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: backup.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:          &[]int32{exportJobBackoffLimit}[0],
			ActiveDeadlineSeconds: &[]int64{exportJobActiveDeadlineSeconds}[0],
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:    exportContainerName,
							Image:   image,
							Command: []string{managerBinary},
							Args:    args,
							Env:     destCredentialEnv(dest),
							// The export tool writes its ExportResult here; the
							// reconciler reads it back from the pod's
							// terminated-container Message.
							TerminationMessagePath:   terminationMessagePath,
							TerminationMessagePolicy: corev1.TerminationMessageReadFile,
							VolumeMounts:             destCredentialVolumeMounts(dest),
						},
					},
					Volumes: destCredentialVolumes(dest),
				},
			},
		},
	}
}

// appendFlagIfSet appends "flag value" only when value is non-empty, so an
// unset optional destination field never plants an empty CLI flag value.
func appendFlagIfSet(args []string, flag, value string) []string {
	if value == "" {
		return args
	}
	return append(args, flag, value)
}

// destCredentialEnv returns the env vars wiring destination.credentialsSecretRef
// into the export Job so the object-store driver can authenticate, keyed by
// driver. The referenced Secret must carry a key named exactly like each
// returned env var. GCS is excluded (it uses a mounted key file - see
// destCredentialVolumes); filesystem needs no credentials.
func destCredentialEnv(dest backupv1alpha1.BackupDestination) []corev1.EnvVar {
	switch dest.Driver {
	case objectstore.DriverAWSS3:
		if dest.CredentialsSecretRef == nil {
			return nil
		}
		return secretEnvVars(dest.CredentialsSecretRef.Name, envAWSAccessKeyID, envAWSSecretAccessKey)
	case objectstore.DriverAzureBlob:
		if dest.CredentialsSecretRef == nil {
			return nil
		}
		return secretEnvVars(dest.CredentialsSecretRef.Name, envAzureStorageAccount, envAzureStorageAccessKey)
	case objectstore.DriverGCS:
		if dest.CredentialsSecretRef == nil {
			return nil
		}
		// The value is the PATH to the mounted key file, not a secret ref.
		return []corev1.EnvVar{{Name: envGoogleAppCredentials, Value: gcsKeyPath}}
	default:
		return nil
	}
}

// secretEnvVars builds one EnvVar per key, each sourced via secretKeyRef from
// secretName at the identically-named key.
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

// destCredentialVolumes returns the pod volume(s) projecting
// destination.credentialsSecretRef as a file, for drivers that authenticate
// from a key file rather than an env value. Only GCS needs this today.
func destCredentialVolumes(dest backupv1alpha1.BackupDestination) []corev1.Volume {
	if !gcsKeyFileMounted(dest) {
		return nil
	}
	return []corev1.Volume{{
		Name: backupCredentialVolumeName,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: dest.CredentialsSecretRef.Name,
				Items:      []corev1.KeyToPath{{Key: gcsKeySecretKey, Path: gcsKeySecretKey}},
			},
		},
	}}
}

// destCredentialVolumeMounts returns the container mount(s) paired with
// destCredentialVolumes, placing the GCS key file at the path
// GOOGLE_APPLICATION_CREDENTIALS points at.
func destCredentialVolumeMounts(dest backupv1alpha1.BackupDestination) []corev1.VolumeMount {
	if !gcsKeyFileMounted(dest) {
		return nil
	}
	return []corev1.VolumeMount{{
		Name:      backupCredentialVolumeName,
		MountPath: gcsKeyDir,
		ReadOnly:  true,
	}}
}

// gcsKeyFileMounted reports whether a GCS service-account key file should be
// mounted: the driver is google-cloud-storage and a credentials secret is set.
func gcsKeyFileMounted(dest backupv1alpha1.BackupDestination) bool {
	return dest.Driver == objectstore.DriverGCS && dest.CredentialsSecretRef != nil
}
