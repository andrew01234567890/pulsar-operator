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
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	backupv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/backup/v1alpha1"
	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	backuptool "github.com/andrew01234567890/pulsar-operator/internal/backup"
	"github.com/andrew01234567890/pulsar-operator/internal/builder"
)

const (
	// restoreComponentName labels the import Job and its pods, keyed by the
	// Restore's own name (as instance) so several Restores in a namespace
	// never collide on selector labels.
	restoreComponentName = "restore-import"

	// importContainerName is the import Job's single container; the
	// reconciler reads this container's termination message to recover the
	// ImportResult.
	importContainerName = "restore-import"

	// importJobBackoffLimit bounds the import Job's pod-level retries before
	// it is marked Failed. A restore is a one-shot replay, so a few retries
	// absorb a transient Oxia/object-store blip without spinning forever.
	importJobBackoffLimit int32 = 3

	// importJobActiveDeadlineSeconds is a hard backstop mirroring the export
	// Job's: it bounds a pod that never starts even in cases the
	// reconciler's own stuck-pod detection might miss.
	importJobActiveDeadlineSeconds int64 = 3600

	// oxiaFlagName is the manager subcommand's Oxia-address flag, named as
	// its own constant (rather than a literal repeated across this file and
	// its tests) purely so this restore-specific file doesn't duplicate the
	// export side's own identical literal in backup_job.go.
	oxiaFlagName = "--oxia"
)

// restoreJobName deterministically names the import Job after its owning
// Restore.
func restoreJobName(restoreName string) string {
	return restoreName + "-import"
}

// buildImportJob renders the Job that runs the operator image's
// `manager backup-import` subcommand: it downloads the manifest at srcKey
// (the Restore reconciler has already resolved spec.source.artifactURI down
// to this bare key - see objectstore.KeyFromURI) from spec.source.destination
// and replays it into the target cluster's Oxia. It is a pure function of its
// inputs so it is unit-testable without a client; the caller sets the owner
// reference and creates it.
//
// The cookie-lineage decision itself is NOT made here: by the time this Job
// is created, the reconciler has already run checkCookieLineage and decided
// to proceed (Passed, or a supervised override) - a policy=enforce mismatch
// must leave no Job behind at all (see advance), so the Job never needs to
// re-litigate lineage.
func buildImportJob(restore *backupv1alpha1.Restore, cluster *clusterv1alpha1.PulsarCluster, srcKey, image string) *batchv1.Job {
	labels := builder.Labels(restore.Name, restoreComponentName)
	name := restoreJobName(restore.Name)
	dest := restore.Spec.Source.Destination

	args := []string{
		backuptool.ImportCommandName,
		// oxiaExportAddress derives "<cluster>-oxia-oxia:<port>" - the same
		// derivation the Backup reconciler uses for the cluster it reads
		// from; here it names the cluster the import Job writes into.
		oxiaFlagName, oxiaExportAddress(cluster),
		"--src-driver", dest.Driver,
		"--src-key", srcKey,
		"--result-path", terminationMessagePath,
	}
	args = appendFlagIfSet(args, "--src-bucket", dest.Bucket)
	args = appendFlagIfSet(args, "--src-region", dest.Region)
	args = appendFlagIfSet(args, "--src-endpoint", dest.Endpoint)
	args = appendFlagIfSet(args, "--src-prefix", dest.Prefix)
	if !skipEphemeralEnabled(restore.Spec) {
		args = append(args, "--include-ephemeral")
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: restore.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:          &[]int32{importJobBackoffLimit}[0],
			ActiveDeadlineSeconds: &[]int64{importJobActiveDeadlineSeconds}[0],
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:    importContainerName,
							Image:   image,
							Command: []string{managerBinary},
							Args:    args,
							// destCredentialEnv/destCredentialVolume(Mounts) are
							// generic over any BackupDestination (they carry no
							// export-specific assumption), so the source
							// destination's credentials are wired onto this
							// import Job exactly as the Backup export Job wires
							// its own destination's.
							Env: destCredentialEnv(dest),
							// The import tool writes its ImportResult here; the
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
