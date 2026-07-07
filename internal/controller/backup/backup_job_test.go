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
	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
)

const testOperatorImage = "ghcr.io/example/pulsar-operator:v1.2.3"

func testBackup(dest backupv1alpha1.BackupDestination) *backupv1alpha1.Backup {
	return &backupv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "b1", Namespace: "ns"},
		Spec: backupv1alpha1.BackupSpec{
			ClusterRef:  corev1.LocalObjectReference{Name: "c1"},
			Destination: dest,
		},
	}
}

func testCluster() *clusterv1alpha1.PulsarCluster {
	return &clusterv1alpha1.PulsarCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "ns"},
	}
}

func argValue(args []string, flag string) (string, bool) {
	for i := 0; i < len(args)-1; i++ {
		if args[i] == flag {
			return args[i+1], true
		}
	}
	return "", false
}

func TestOxiaExportAddress(t *testing.T) {
	got := oxiaExportAddress(testCluster())
	const want = "c1-oxia-oxia:6648"
	if got != want {
		t.Fatalf("oxiaExportAddress = %q, want %q", got, want)
	}
}

func TestBuildExportJobCoreSpec(t *testing.T) {
	dest := backupv1alpha1.BackupDestination{
		Driver: testDriverAWSS3,
		Bucket: testBucket,
		Region: testRegionUSEast,
		Prefix: testPrefixC1,
	}
	job := buildExportJob(testBackup(dest), testCluster(), testOperatorImage)

	if job.Name != "b1-export" {
		t.Errorf("job name = %q, want b1-export", job.Name)
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
	if c.Args[0] != "backup-export" {
		t.Errorf("args[0] = %q, want backup-export", c.Args[0])
	}

	// The --result-path arg must match the container's TerminationMessagePath
	// exactly (both are the terminationMessagePath constant), so the tool
	// writes its result where the reconciler reads it.
	resultPath, ok := argValue(c.Args, "--result-path")
	if !ok {
		t.Error("missing --result-path arg")
	}
	if resultPath != c.TerminationMessagePath {
		t.Errorf("--result-path %q != TerminationMessagePath %q", resultPath, c.TerminationMessagePath)
	}
	if c.TerminationMessagePolicy != corev1.TerminationMessageReadFile {
		t.Errorf("TerminationMessagePolicy = %q, want FallbackToLogsOnError/ReadFile", c.TerminationMessagePolicy)
	}

	for flag, want := range map[string]string{
		"--oxia":        "c1-oxia-oxia:6648",
		"--dest-driver": testDriverAWSS3,
		"--dest-bucket": "my-bucket",
		"--dest-region": testRegionUSEast,
		"--dest-prefix": testPrefixC1,
		"--dest-key":    "b1.manifest",
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
}

func TestBuildExportJobAWSCredentials(t *testing.T) {
	dest := backupv1alpha1.BackupDestination{
		Driver:               testDriverAWSS3,
		Bucket:               "b",
		CredentialsSecretRef: &corev1.LocalObjectReference{Name: testSecretS3},
	}
	job := buildExportJob(testBackup(dest), testCluster(), testOperatorImage)
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

func TestBuildExportJobAzureCredentials(t *testing.T) {
	dest := backupv1alpha1.BackupDestination{
		Driver:               "azureblob",
		Bucket:               "container",
		CredentialsSecretRef: &corev1.LocalObjectReference{Name: "az-creds"},
	}
	job := buildExportJob(testBackup(dest), testCluster(), testOperatorImage)
	env := job.Spec.Template.Spec.Containers[0].Env
	for _, key := range []string{envAzureStorageAccount, envAzureStorageAccessKey} {
		if findEnv(env, key) == nil {
			t.Errorf("missing azure env %s", key)
		}
	}
}

func TestBuildExportJobGCSMountsKeyFile(t *testing.T) {
	dest := backupv1alpha1.BackupDestination{
		Driver:               "google-cloud-storage",
		Bucket:               "gb",
		CredentialsSecretRef: &corev1.LocalObjectReference{Name: "gcs-creds"},
	}
	job := buildExportJob(testBackup(dest), testCluster(), testOperatorImage)
	pod := job.Spec.Template.Spec
	c := pod.Containers[0]

	// GCS authenticates from a mounted key FILE; GOOGLE_APPLICATION_CREDENTIALS
	// must be the mount PATH (a literal value), never a secretKeyRef.
	ev := findEnv(c.Env, envGoogleAppCredentials)
	if ev == nil {
		t.Fatalf("missing env %s", envGoogleAppCredentials)
	}
	if ev.Value != gcsKeyPath {
		t.Errorf("%s = %q, want %q", envGoogleAppCredentials, ev.Value, gcsKeyPath)
	}
	if ev.ValueFrom != nil {
		t.Errorf("%s must be a literal path, not a valueFrom", envGoogleAppCredentials)
	}

	if len(pod.Volumes) != 1 || pod.Volumes[0].Secret == nil || pod.Volumes[0].Secret.SecretName != "gcs-creds" {
		t.Fatalf("expected a secret volume for gcs-creds, got %+v", pod.Volumes)
	}
	if len(c.VolumeMounts) != 1 || c.VolumeMounts[0].MountPath != gcsKeyDir {
		t.Fatalf("expected a volume mount at %s, got %+v", gcsKeyDir, c.VolumeMounts)
	}
}

func TestBuildExportJobFilesystemNoCredentials(t *testing.T) {
	dest := backupv1alpha1.BackupDestination{Driver: testDriverFilesystem, Bucket: "/mnt/backups"}
	job := buildExportJob(testBackup(dest), testCluster(), testOperatorImage)
	c := job.Spec.Template.Spec.Containers[0]
	if len(c.Env) != 0 {
		t.Errorf("filesystem driver should wire no credential env, got %+v", c.Env)
	}
	if len(job.Spec.Template.Spec.Volumes) != 0 {
		t.Errorf("filesystem driver should mount no credential volume")
	}
}

func findEnv(env []corev1.EnvVar, name string) *corev1.EnvVar {
	for i := range env {
		if env[i].Name == name {
			return &env[i]
		}
	}
	return nil
}
