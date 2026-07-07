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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	backupv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/backup/v1alpha1"
)

func testRestore(dest backupv1alpha1.BackupDestination) *backupv1alpha1.Restore {
	return &backupv1alpha1.Restore{
		ObjectMeta: metav1.ObjectMeta{Name: "r1", Namespace: "ns"},
		Spec: backupv1alpha1.RestoreSpec{
			Source:           backupv1alpha1.RestoreSource{Destination: dest, ArtifactURI: testArtifactURI},
			TargetClusterRef: corev1.LocalObjectReference{Name: "c1"},
		},
	}
}

func TestBuildImportJobCoreSpec(t *testing.T) {
	dest := backupv1alpha1.BackupDestination{
		Driver: testDriverAWSS3,
		Bucket: testBucket,
		Region: testRegionUSEast,
		Prefix: testPrefixC1,
	}
	job := buildImportJob(testRestore(dest), testCluster(), "r1.manifest", testOperatorImage)

	if job.Name != "r1-import" {
		t.Errorf("job name = %q, want r1-import", job.Name)
	}
	if job.Namespace != "ns" {
		t.Errorf("job namespace = %q, want ns", job.Namespace)
	}

	pod := job.Spec.Template.Spec
	if pod.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("RestartPolicy = %q, want Never", pod.RestartPolicy)
	}
	if len(pod.Containers) != 1 {
		t.Fatalf("want 1 container, got %d", len(pod.Containers))
	}
	c := pod.Containers[0]
	if c.Image != testOperatorImage {
		t.Errorf("image = %q, want %q", c.Image, testOperatorImage)
	}
	if len(c.Command) != 1 || c.Command[0] != managerBinary {
		t.Errorf("command = %v, want [/manager]", c.Command)
	}
	if c.Args[0] != "backup-import" {
		t.Errorf("args[0] = %q, want backup-import", c.Args[0])
	}

	resultPath, ok := argValue(c.Args, "--result-path")
	if !ok {
		t.Error("missing --result-path arg")
	}
	if resultPath != c.TerminationMessagePath {
		t.Errorf("--result-path %q != TerminationMessagePath %q", resultPath, c.TerminationMessagePath)
	}
	if c.TerminationMessagePolicy != corev1.TerminationMessageReadFile {
		t.Errorf("TerminationMessagePolicy = %q, want ReadFile", c.TerminationMessagePolicy)
	}

	for flag, want := range map[string]string{
		oxiaFlagName:   "c1-oxia-oxia:6648",
		"--src-driver": testDriverAWSS3,
		"--src-bucket": "my-bucket",
		"--src-region": testRegionUSEast,
		"--src-prefix": testPrefixC1,
		"--src-key":    "r1.manifest",
	} {
		got, ok := argValue(c.Args, flag)
		if !ok {
			t.Errorf("missing arg %s", flag)
			continue
		}
		if got != want {
			t.Errorf("arg %s = %q, want %q", flag, got, want)
		}
	}

	if _, ok := argValue(c.Args, "--include-ephemeral"); ok {
		t.Error("--include-ephemeral should not be a flag=value pair")
	}
	found := false
	for _, a := range c.Args {
		if a == "--include-ephemeral" {
			found = true
		}
	}
	if found {
		t.Error("--include-ephemeral must not be set when skipEphemeral defaults to true")
	}
}

func TestBuildImportJobIncludeEphemeralWhenSkipEphemeralFalse(t *testing.T) {
	dest := backupv1alpha1.BackupDestination{Driver: testDriverFilesystem, Bucket: "/mnt/restore-backups"}
	restore := testRestore(dest)
	skip := false
	restore.Spec.SkipEphemeral = &skip

	job := buildImportJob(restore, testCluster(), "r1.manifest", testOperatorImage)
	c := job.Spec.Template.Spec.Containers[0]

	found := false
	for _, a := range c.Args {
		if a == "--include-ephemeral" {
			found = true
		}
	}
	if !found {
		t.Error("expected --include-ephemeral when spec.skipEphemeral=false")
	}
}

func TestBuildImportJobAWSCredentials(t *testing.T) {
	dest := backupv1alpha1.BackupDestination{
		Driver:               testDriverAWSS3,
		Bucket:               "b",
		CredentialsSecretRef: &corev1.LocalObjectReference{Name: testSecretS3},
	}
	job := buildImportJob(testRestore(dest), testCluster(), "r1.manifest", testOperatorImage)
	env := job.Spec.Template.Spec.Containers[0].Env

	for _, key := range []string{envAWSAccessKeyID, envAWSSecretAccessKey} {
		ev := findEnv(env, key)
		if ev == nil {
			t.Fatalf("missing env %s", key)
		}
		if ev.ValueFrom == nil || ev.ValueFrom.SecretKeyRef == nil {
			t.Fatalf("env %s should be a secretKeyRef", key)
		}
		if ev.ValueFrom.SecretKeyRef.Name != testSecretS3 || ev.ValueFrom.SecretKeyRef.Key != key {
			t.Errorf("env %s secretKeyRef = %+v", key, ev.ValueFrom.SecretKeyRef)
		}
	}
}

func TestBuildImportJobGCSMountsKeyFile(t *testing.T) {
	dest := backupv1alpha1.BackupDestination{
		Driver:               "google-cloud-storage",
		Bucket:               "gb",
		CredentialsSecretRef: &corev1.LocalObjectReference{Name: "restore-gcs-creds"},
	}
	job := buildImportJob(testRestore(dest), testCluster(), "r1.manifest", testOperatorImage)
	pod := job.Spec.Template.Spec
	c := pod.Containers[0]

	ev := findEnv(c.Env, envGoogleAppCredentials)
	if ev == nil {
		t.Fatalf("missing env %s", envGoogleAppCredentials)
	}
	if ev.Value != gcsKeyPath {
		t.Errorf("%s = %q, want %q", envGoogleAppCredentials, ev.Value, gcsKeyPath)
	}
	if len(pod.Volumes) != 1 || pod.Volumes[0].Secret == nil || pod.Volumes[0].Secret.SecretName != "restore-gcs-creds" {
		t.Fatalf("expected a secret volume for restore-gcs-creds, got %+v", pod.Volumes)
	}
}

func TestBuildImportJobFilesystemNoCredentials(t *testing.T) {
	dest := backupv1alpha1.BackupDestination{Driver: testDriverFilesystem, Bucket: "/mnt/restore-backups"}
	job := buildImportJob(testRestore(dest), testCluster(), "r1.manifest", testOperatorImage)
	c := job.Spec.Template.Spec.Containers[0]
	if len(c.Env) != 0 {
		t.Errorf("filesystem driver should wire no credential env, got %+v", c.Env)
	}
	if len(job.Spec.Template.Spec.Volumes) != 0 {
		t.Errorf("filesystem driver should mount no credential volume")
	}
}
