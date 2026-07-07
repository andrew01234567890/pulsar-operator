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
		offload := &clusterv1alpha1.OffloadSpec{Driver: offloadDriverGCS, Bucket: "gcs-bucket", Region: "us-central1"}
		got := withBrokerOffloadDefaults(nil, offload)
		want := map[string]string{
			confKeyManagedLedgerOffloadDriver: offloadDriverGCS,
			confKeyGCSOffloadBucket:           "gcs-bucket",
			confKeyGCSOffloadRegion:           "us-central1",
		}
		if !maps.Equal(got, want) {
			t.Errorf("withBrokerOffloadDefaults(gcs) = %v, want %v", got, want)
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

	t.Run("google-cloud-storage wires the service-account key file var", func(t *testing.T) {
		offload := &clusterv1alpha1.OffloadSpec{Driver: offloadDriverGCS, CredentialsSecretRef: secretRef}
		got := offloadCredentialEnv(offload)
		assertSecretEnvVars(t, got, secretName, envGoogleApplicationCredentials)
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

	t.Run("credentialsSecretRef is wired into broker Env", func(t *testing.T) {
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
	})

	t.Run("no offload configured leaves Env untouched", func(t *testing.T) {
		spec := clusterv1alpha1.PulsarClusterSpec{Broker: &clusterv1alpha1.BrokerSpec{}}
		got := buildBrokerSpec(spec)
		if got.Env != nil {
			t.Errorf("Env = %v, want nil", got.Env)
		}
	})
}
