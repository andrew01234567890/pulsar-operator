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
	"maps"
	"testing"

	corev1 "k8s.io/api/core/v1"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
)

const (
	testOffloadBucket = "offload-bucket"
	testOffloadRegion = "us-west-2"
	testGCSBucket     = "gcs-bucket"
	testGCSCreds      = "gcs-creds"
)

func TestWithBrokerOffloadDefaults(t *testing.T) {
	t.Run("nil offload leaves cfg untouched", func(t *testing.T) {
		cfg := map[string]string{"unrelated": "x"}
		got := withBrokerOffloadDefaults(cfg, nil)
		if !maps.Equal(got, cfg) {
			t.Errorf("withBrokerOffloadDefaults(nil) = %v, want %v", got, cfg)
		}
	})

	t.Run("aws-s3 sets driver, bucket, region, and endpoint", func(t *testing.T) {
		offload := &clusterv1alpha1.OffloadSpec{
			Driver:   offloadDriverAWSS3,
			Bucket:   testOffloadBucket,
			Region:   testOffloadRegion,
			Endpoint: "https://s3.example.com",
		}
		got := withBrokerOffloadDefaults(nil, offload)
		want := map[string]string{
			confKeyManagedLedgerOffloadDriver: offloadDriverAWSS3,
			confKeyS3OffloadBucket:            testOffloadBucket,
			confKeyS3OffloadRegion:            testOffloadRegion,
			confKeyS3OffloadServiceEndpoint:   "https://s3.example.com",
		}
		if !maps.Equal(got, want) {
			t.Errorf("withBrokerOffloadDefaults(s3) = %v, want %v", got, want)
		}
	})

	t.Run("aws-s3 without endpoint omits the endpoint key", func(t *testing.T) {
		offload := &clusterv1alpha1.OffloadSpec{Driver: offloadDriverAWSS3, Bucket: testOffloadBucket, Region: testOffloadRegion}
		got := withBrokerOffloadDefaults(nil, offload)
		if _, present := got[confKeyS3OffloadServiceEndpoint]; present {
			t.Errorf("got %v, want no %s key when Endpoint is unset", got, confKeyS3OffloadServiceEndpoint)
		}
	})

	t.Run("google-cloud-storage sets driver, bucket, and region (no endpoint key)", func(t *testing.T) {
		offload := &clusterv1alpha1.OffloadSpec{Driver: offloadDriverGCS, Bucket: testGCSBucket, Region: "us-central1"}
		got := withBrokerOffloadDefaults(nil, offload)
		want := map[string]string{
			confKeyManagedLedgerOffloadDriver: offloadDriverGCS,
			confKeyGCSOffloadBucket:           testGCSBucket,
			confKeyGCSOffloadRegion:           "us-central1",
		}
		if !maps.Equal(got, want) {
			t.Errorf("withBrokerOffloadDefaults(gcs) = %v, want %v", got, want)
		}
	})

	t.Run("google-cloud-storage with credentials sets the service-account key file path", func(t *testing.T) {
		offload := &clusterv1alpha1.OffloadSpec{
			Driver:               offloadDriverGCS,
			Bucket:               testGCSBucket,
			CredentialsSecretRef: &corev1.LocalObjectReference{Name: testGCSCreds},
		}
		got := withBrokerOffloadDefaults(nil, offload)
		if got[confKeyGCSOffloadServiceAccountKeyFile] != gcsOffloadKeyPath {
			t.Errorf("%s = %q, want %q", confKeyGCSOffloadServiceAccountKeyFile, got[confKeyGCSOffloadServiceAccountKeyFile], gcsOffloadKeyPath)
		}
	})

	t.Run("google-cloud-storage without credentials omits the key file path", func(t *testing.T) {
		offload := &clusterv1alpha1.OffloadSpec{Driver: offloadDriverGCS, Bucket: testGCSBucket}
		got := withBrokerOffloadDefaults(nil, offload)
		if _, present := got[confKeyGCSOffloadServiceAccountKeyFile]; present {
			t.Errorf("got %v, want no %s key when no credentials secret is set", got, confKeyGCSOffloadServiceAccountKeyFile)
		}
	})

	t.Run("azureblob sets driver and the generic bucket key", func(t *testing.T) {
		offload := &clusterv1alpha1.OffloadSpec{Driver: offloadDriverAzureBlob, Bucket: "container"}
		got := withBrokerOffloadDefaults(nil, offload)
		want := map[string]string{
			confKeyManagedLedgerOffloadDriver: offloadDriverAzureBlob,
			confKeyAzureOffloadBucket:         "container",
		}
		if !maps.Equal(got, want) {
			t.Errorf("withBrokerOffloadDefaults(azureblob) = %v, want %v", got, want)
		}
	})

	t.Run("filesystem sets only the driver, bucket is not used", func(t *testing.T) {
		offload := &clusterv1alpha1.OffloadSpec{Driver: offloadDriverFilesystem, Bucket: "ignored"}
		got := withBrokerOffloadDefaults(nil, offload)
		want := map[string]string{confKeyManagedLedgerOffloadDriver: offloadDriverFilesystem}
		if !maps.Equal(got, want) {
			t.Errorf("withBrokerOffloadDefaults(filesystem) = %v, want %v", got, want)
		}
	})

	t.Run("threshold is set when configured", func(t *testing.T) {
		threshold := int64(1073741824)
		offload := &clusterv1alpha1.OffloadSpec{Driver: offloadDriverFilesystem, OffloadThresholdBytes: &threshold}
		got := withBrokerOffloadDefaults(nil, offload)
		if got[confKeyManagedLedgerOffloadThresholdBytes] != "1073741824" {
			t.Errorf("threshold = %q, want %q", got[confKeyManagedLedgerOffloadThresholdBytes], "1073741824")
		}
	})

	t.Run("threshold key is absent when unset", func(t *testing.T) {
		offload := &clusterv1alpha1.OffloadSpec{Driver: offloadDriverFilesystem}
		got := withBrokerOffloadDefaults(nil, offload)
		if _, present := got[confKeyManagedLedgerOffloadThresholdBytes]; present {
			t.Errorf("got %v, want no threshold key", got)
		}
	})

	t.Run("user-set keys are never overwritten", func(t *testing.T) {
		cfg := map[string]string{
			confKeyManagedLedgerOffloadDriver: "user-custom-driver",
			confKeyS3OffloadBucket:            "user-bucket",
		}
		offload := &clusterv1alpha1.OffloadSpec{Driver: offloadDriverAWSS3, Bucket: "operator-bucket", Region: testOffloadRegion}
		got := withBrokerOffloadDefaults(cfg, offload)
		if got[confKeyManagedLedgerOffloadDriver] != "user-custom-driver" {
			t.Errorf("driver = %q, want user value preserved", got[confKeyManagedLedgerOffloadDriver])
		}
		if got[confKeyS3OffloadBucket] != "user-bucket" {
			t.Errorf("bucket = %q, want user value preserved", got[confKeyS3OffloadBucket])
		}
		if got[confKeyS3OffloadRegion] != testOffloadRegion {
			t.Errorf("region = %q, want operator default since unset by user", got[confKeyS3OffloadRegion])
		}
	})
}

func TestPulsarAllImage(t *testing.T) {
	cases := []struct {
		name    string
		version string
		want    string
	}{
		{"empty version yields empty image", "", ""},
		{"version builds the pulsar-all image", testPulsarVersion, "apachepulsar/pulsar-all:" + testPulsarVersion},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pulsarAllImage(tc.version); got != tc.want {
				t.Errorf("pulsarAllImage(%q) = %q, want %q", tc.version, got, tc.want)
			}
		})
	}
}

func TestOffloadCredentialEnv(t *testing.T) {
	const secretName = "offload-creds"
	secretRef := &corev1.LocalObjectReference{Name: secretName}

	t.Run("nil offload yields no env vars", func(t *testing.T) {
		if got := offloadCredentialEnv(nil); got != nil {
			t.Errorf("offloadCredentialEnv(nil) = %v, want nil", got)
		}
	})

	t.Run("unset credentialsSecretRef yields no env vars", func(t *testing.T) {
		offload := &clusterv1alpha1.OffloadSpec{Driver: offloadDriverAWSS3}
		if got := offloadCredentialEnv(offload); got != nil {
			t.Errorf("offloadCredentialEnv() = %v, want nil", got)
		}
	})

	t.Run("aws-s3 wires the AWS credential pair", func(t *testing.T) {
		offload := &clusterv1alpha1.OffloadSpec{Driver: offloadDriverAWSS3, CredentialsSecretRef: secretRef}
		got := offloadCredentialEnv(offload)
		assertSecretEnvVars(t, got, secretName, envAWSAccessKeyID, envAWSSecretAccessKey)
	})

	t.Run("google-cloud-storage does NOT wire an env var (it mounts a key file instead)", func(t *testing.T) {
		// Regression: GOOGLE_APPLICATION_CREDENTIALS is a filesystem PATH to a
		// service-account JSON key file, not the credential content, so wiring
		// it from the secret value fails GCS auth at runtime. GCS credentials
		// must be mounted as a file (offloadCredentialVolumes) instead.
		offload := &clusterv1alpha1.OffloadSpec{Driver: offloadDriverGCS, CredentialsSecretRef: secretRef}
		if got := offloadCredentialEnv(offload); got != nil {
			t.Errorf("offloadCredentialEnv(gcs) = %v, want nil (GCS uses a mounted key file, not an env var)", got)
		}
	})

	t.Run("azureblob wires the Azure storage account pair", func(t *testing.T) {
		offload := &clusterv1alpha1.OffloadSpec{Driver: offloadDriverAzureBlob, CredentialsSecretRef: secretRef}
		got := offloadCredentialEnv(offload)
		assertSecretEnvVars(t, got, secretName, envAzureStorageAccount, envAzureStorageAccessKey)
	})

	t.Run("filesystem needs no credentials", func(t *testing.T) {
		offload := &clusterv1alpha1.OffloadSpec{Driver: offloadDriverFilesystem, CredentialsSecretRef: secretRef}
		if got := offloadCredentialEnv(offload); got != nil {
			t.Errorf("offloadCredentialEnv(filesystem) = %v, want nil", got)
		}
	})
}

func TestOffloadCredentialVolumes(t *testing.T) {
	const secretName = testGCSCreds
	secretRef := &corev1.LocalObjectReference{Name: secretName}

	t.Run("google-cloud-storage mounts the key file secret volume", func(t *testing.T) {
		offload := &clusterv1alpha1.OffloadSpec{Driver: offloadDriverGCS, CredentialsSecretRef: secretRef}

		vols := offloadCredentialVolumes(offload)
		if len(vols) != 1 {
			t.Fatalf("got %d volumes, want 1: %+v", len(vols), vols)
		}
		vol := vols[0]
		if vol.Name != offloadCredentialVolumeName {
			t.Errorf("volume.Name = %q, want %q", vol.Name, offloadCredentialVolumeName)
		}
		if vol.Secret == nil {
			t.Fatal("volume.Secret is nil, want a secret volume source")
		}
		if vol.Secret.SecretName != secretName {
			t.Errorf("volume.Secret.SecretName = %q, want %q", vol.Secret.SecretName, secretName)
		}
		if len(vol.Secret.Items) != 1 || vol.Secret.Items[0].Key != gcsOffloadKeySecretKey || vol.Secret.Items[0].Path != gcsOffloadKeySecretKey {
			t.Errorf("volume.Secret.Items = %+v, want a single %q->%q projection", vol.Secret.Items, gcsOffloadKeySecretKey, gcsOffloadKeySecretKey)
		}

		mounts := offloadCredentialVolumeMounts(offload)
		if len(mounts) != 1 {
			t.Fatalf("got %d volume mounts, want 1: %+v", len(mounts), mounts)
		}
		mount := mounts[0]
		if mount.Name != offloadCredentialVolumeName {
			t.Errorf("mount.Name = %q, want %q", mount.Name, offloadCredentialVolumeName)
		}
		if mount.MountPath != gcsOffloadKeyDir {
			t.Errorf("mount.MountPath = %q, want %q", mount.MountPath, gcsOffloadKeyDir)
		}
		if !mount.ReadOnly {
			t.Error("mount.ReadOnly = false, want true")
		}
	})

	t.Run("google-cloud-storage without credentials mounts nothing", func(t *testing.T) {
		offload := &clusterv1alpha1.OffloadSpec{Driver: offloadDriverGCS}
		if got := offloadCredentialVolumes(offload); got != nil {
			t.Errorf("offloadCredentialVolumes() = %v, want nil", got)
		}
		if got := offloadCredentialVolumeMounts(offload); got != nil {
			t.Errorf("offloadCredentialVolumeMounts() = %v, want nil", got)
		}
	})

	t.Run("non-GCS drivers mount nothing", func(t *testing.T) {
		for _, driver := range []string{offloadDriverAWSS3, offloadDriverAzureBlob, offloadDriverFilesystem} {
			offload := &clusterv1alpha1.OffloadSpec{Driver: driver, CredentialsSecretRef: secretRef}
			if got := offloadCredentialVolumes(offload); got != nil {
				t.Errorf("offloadCredentialVolumes(%q) = %v, want nil", driver, got)
			}
			if got := offloadCredentialVolumeMounts(offload); got != nil {
				t.Errorf("offloadCredentialVolumeMounts(%q) = %v, want nil", driver, got)
			}
		}
	})

	t.Run("nil offload mounts nothing", func(t *testing.T) {
		if got := offloadCredentialVolumes(nil); got != nil {
			t.Errorf("offloadCredentialVolumes(nil) = %v, want nil", got)
		}
		if got := offloadCredentialVolumeMounts(nil); got != nil {
			t.Errorf("offloadCredentialVolumeMounts(nil) = %v, want nil", got)
		}
	})
}

func assertSecretEnvVars(t *testing.T, got []corev1.EnvVar, wantSecretName string, wantKeys ...string) {
	t.Helper()
	if len(got) != len(wantKeys) {
		t.Fatalf("got %d env vars, want %d: %+v", len(got), len(wantKeys), got)
	}
	for i, wantKey := range wantKeys {
		ev := got[i]
		if ev.Name != wantKey {
			t.Errorf("env[%d].Name = %q, want %q", i, ev.Name, wantKey)
		}
		if ev.ValueFrom == nil || ev.ValueFrom.SecretKeyRef == nil {
			t.Fatalf("env[%d].ValueFrom.SecretKeyRef is nil, want a secretKeyRef", i)
		}
		if ev.ValueFrom.SecretKeyRef.Name != wantSecretName {
			t.Errorf("env[%d].ValueFrom.SecretKeyRef.Name = %q, want %q", i, ev.ValueFrom.SecretKeyRef.Name, wantSecretName)
		}
		if ev.ValueFrom.SecretKeyRef.Key != wantKey {
			t.Errorf("env[%d].ValueFrom.SecretKeyRef.Key = %q, want %q", i, ev.ValueFrom.SecretKeyRef.Key, wantKey)
		}
	}
}

func TestBuildBrokerSpec_Offload(t *testing.T) {
	t.Run("offload set with no explicit image anywhere selects pulsar-all", func(t *testing.T) {
		spec := clusterv1alpha1.PulsarClusterSpec{
			PulsarVersion: testPulsarVersion,
			Broker:        &clusterv1alpha1.BrokerSpec{Replicas: ptr(int32(3))},
			Offload:       &clusterv1alpha1.OffloadSpec{Driver: offloadDriverAWSS3, Bucket: "b"},
		}
		got := buildBrokerSpec(spec)
		want := "apachepulsar/pulsar-all:" + testPulsarVersion
		if got.Image != want {
			t.Errorf("Image = %q, want %q", got.Image, want)
		}
	})

	t.Run("offload set but cluster-wide image explicitly set is left untouched", func(t *testing.T) {
		spec := clusterv1alpha1.PulsarClusterSpec{
			Image:         testClusterImage,
			PulsarVersion: testPulsarVersion,
			Broker:        &clusterv1alpha1.BrokerSpec{},
			Offload:       &clusterv1alpha1.OffloadSpec{Driver: offloadDriverAWSS3, Bucket: "b"},
		}
		got := buildBrokerSpec(spec)
		if got.Image != testClusterImage {
			t.Errorf("Image = %q, want cluster-wide image %q preserved", got.Image, testClusterImage)
		}
	})

	t.Run("offload set but broker-level image explicitly set is left untouched", func(t *testing.T) {
		const brokerImg = "broker/pulsar:custom"
		spec := clusterv1alpha1.PulsarClusterSpec{
			PulsarVersion: testPulsarVersion,
			Broker:        &clusterv1alpha1.BrokerSpec{Image: brokerImg},
			Offload:       &clusterv1alpha1.OffloadSpec{Driver: offloadDriverAWSS3, Bucket: "b"},
		}
		got := buildBrokerSpec(spec)
		if got.Image != brokerImg {
			t.Errorf("Image = %q, want broker image %q preserved", got.Image, brokerImg)
		}
	})

	t.Run("no offload configured never selects pulsar-all", func(t *testing.T) {
		spec := clusterv1alpha1.PulsarClusterSpec{
			PulsarVersion: testPulsarVersion,
			Broker:        &clusterv1alpha1.BrokerSpec{},
		}
		got := buildBrokerSpec(spec)
		want := "apachepulsar/pulsar:" + testPulsarVersion
		if got.Image != want {
			t.Errorf("Image = %q, want %q", got.Image, want)
		}
	})

	t.Run("aws-s3 credentialsSecretRef is wired into broker Env, not Volumes", func(t *testing.T) {
		spec := clusterv1alpha1.PulsarClusterSpec{
			PulsarVersion: testPulsarVersion,
			Broker:        &clusterv1alpha1.BrokerSpec{},
			Offload: &clusterv1alpha1.OffloadSpec{
				Driver:               offloadDriverAWSS3,
				Bucket:               "b",
				CredentialsSecretRef: &corev1.LocalObjectReference{Name: "creds"},
			},
		}
		got := buildBrokerSpec(spec)
		assertSecretEnvVars(t, got.Env, "creds", envAWSAccessKeyID, envAWSSecretAccessKey)
		if got.Volumes != nil || got.VolumeMounts != nil {
			t.Errorf("aws-s3 must use env vars, not a mounted file: Volumes=%v VolumeMounts=%v", got.Volumes, got.VolumeMounts)
		}
	})

	t.Run("gcs credentialsSecretRef is wired into broker Volumes/VolumeMounts, not Env", func(t *testing.T) {
		spec := clusterv1alpha1.PulsarClusterSpec{
			PulsarVersion: testPulsarVersion,
			Broker:        &clusterv1alpha1.BrokerSpec{},
			Offload: &clusterv1alpha1.OffloadSpec{
				Driver:               offloadDriverGCS,
				Bucket:               "b",
				CredentialsSecretRef: &corev1.LocalObjectReference{Name: testGCSCreds},
			},
		}
		got := buildBrokerSpec(spec)
		if got.Env != nil {
			t.Errorf("gcs must NOT wire an env var: Env=%v", got.Env)
		}
		if len(got.Volumes) != 1 || got.Volumes[0].Secret == nil || got.Volumes[0].Secret.SecretName != testGCSCreds {
			t.Errorf("Volumes = %+v, want a single gcs-creds secret volume", got.Volumes)
		}
		if len(got.VolumeMounts) != 1 || got.VolumeMounts[0].MountPath != gcsOffloadKeyDir {
			t.Errorf("VolumeMounts = %+v, want a single mount at %q", got.VolumeMounts, gcsOffloadKeyDir)
		}
	})

	t.Run("no offload configured leaves Env/Volumes untouched", func(t *testing.T) {
		spec := clusterv1alpha1.PulsarClusterSpec{Broker: &clusterv1alpha1.BrokerSpec{}}
		got := buildBrokerSpec(spec)
		if got.Env != nil {
			t.Errorf("Env = %v, want nil", got.Env)
		}
		if got.Volumes != nil || got.VolumeMounts != nil {
			t.Errorf("Volumes/VolumeMounts = %v/%v, want nil", got.Volumes, got.VolumeMounts)
		}
	})
}
