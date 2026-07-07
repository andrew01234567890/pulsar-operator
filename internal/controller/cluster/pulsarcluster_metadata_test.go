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
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
)

const (
	testMetadataClusterName = "test-cluster"

	// testWantOxiaURL is the oxia:// metadataStoreUrl/configurationMetadataStoreUrl
	// the umbrella reconciler injects for testMetadataClusterName's Oxia child.
	testWantOxiaURL = "oxia://test-cluster-oxia-oxia:6648/default"

	testUserStoreURL       = "oxia://user-store:6648/default"
	testUserConfigStoreURL = "oxia://user-config-store:6648/default"
)

func TestOxiaPublicServiceName(t *testing.T) {
	// "-oxia" is appended twice: once by the umbrella's own childName
	// convention naming the OxiaCluster child ("<cluster>-oxia"), once more
	// by the OxiaCluster reconciler's own public-Service naming convention
	// (oxiaurl.PublicServiceName). Both are pre-existing conventions this
	// helper must replicate exactly, not invent.
	want := "test-cluster-oxia-oxia"
	if got := oxiaPublicServiceName(testMetadataClusterName); got != want {
		t.Errorf("oxiaPublicServiceName(%q) = %q, want %q", testMetadataClusterName, got, want)
	}
}

func TestSetConfigDefault(t *testing.T) {
	cases := []struct {
		name  string
		cfg   map[string]string
		key   string
		value string
		want  map[string]string
	}{
		{
			name:  "nil map is allocated",
			cfg:   nil,
			key:   "k",
			value: "v",
			want:  map[string]string{"k": "v"},
		},
		{
			name:  "missing key gets the default",
			cfg:   map[string]string{"other": "x"},
			key:   "k",
			value: "v",
			want:  map[string]string{"other": "x", "k": "v"},
		},
		{
			name:  "user-set value is preserved",
			cfg:   map[string]string{"k": "user-value"},
			key:   "k",
			value: "v",
			want:  map[string]string{"k": "user-value"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := setConfigDefault(tc.cfg, tc.key, tc.value)
			if len(got) != len(tc.want) {
				t.Fatalf("setConfigDefault() = %v, want %v", got, tc.want)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Errorf("setConfigDefault()[%q] = %q, want %q", k, got[k], v)
				}
			}
		})
	}
}

func TestWithBrokerProxyMetadataDefaults(t *testing.T) {
	t.Run("unset keys get the cluster's oxia:// URL", func(t *testing.T) {
		got := withBrokerProxyMetadataDefaults(nil, testMetadataClusterName)
		if got[configKeyMetadataStoreURL] != testWantOxiaURL {
			t.Errorf("metadataStoreUrl = %q, want %q", got[configKeyMetadataStoreURL], testWantOxiaURL)
		}
		if got[configKeyConfigurationMetadataStoreURL] != testWantOxiaURL {
			t.Errorf("configurationMetadataStoreUrl = %q, want %q", got[configKeyConfigurationMetadataStoreURL], testWantOxiaURL)
		}
	})

	t.Run("user-set values are preserved", func(t *testing.T) {
		cfg := map[string]string{
			configKeyMetadataStoreURL:              testUserStoreURL,
			configKeyConfigurationMetadataStoreURL: testUserConfigStoreURL,
		}
		got := withBrokerProxyMetadataDefaults(cfg, testMetadataClusterName)
		if got[configKeyMetadataStoreURL] != testUserStoreURL {
			t.Errorf("metadataStoreUrl = %q, want user value preserved", got[configKeyMetadataStoreURL])
		}
		if got[configKeyConfigurationMetadataStoreURL] != testUserConfigStoreURL {
			t.Errorf("configurationMetadataStoreUrl = %q, want user value preserved", got[configKeyConfigurationMetadataStoreURL])
		}
	})

	t.Run("only the unset key of the pair is defaulted", func(t *testing.T) {
		cfg := map[string]string{configKeyMetadataStoreURL: testUserStoreURL}
		got := withBrokerProxyMetadataDefaults(cfg, testMetadataClusterName)
		if got[configKeyMetadataStoreURL] != testUserStoreURL {
			t.Errorf("metadataStoreUrl = %q, want user value preserved", got[configKeyMetadataStoreURL])
		}
		if got[configKeyConfigurationMetadataStoreURL] != testWantOxiaURL {
			t.Errorf("configurationMetadataStoreUrl = %q, want %q", got[configKeyConfigurationMetadataStoreURL], testWantOxiaURL)
		}
	})
}

func TestWithBookKeeperMetadataDefault(t *testing.T) {
	const (
		wantURI = "metadata-store:oxia://test-cluster-oxia-oxia:6648/bookkeeper"
		userURI = "metadata-store:oxia://user-store:6648/bookkeeper"
	)

	if got := withBookKeeperMetadataDefault(nil, testMetadataClusterName); got[configKeyMetadataServiceURI] != wantURI {
		t.Errorf("metadataServiceUri = %q, want %q", got[configKeyMetadataServiceURI], wantURI)
	}

	cfg := map[string]string{configKeyMetadataServiceURI: userURI}
	got := withBookKeeperMetadataDefault(cfg, testMetadataClusterName)
	if got[configKeyMetadataServiceURI] != userURI {
		t.Errorf("metadataServiceUri = %q, want user value preserved", got[configKeyMetadataServiceURI])
	}
}

func TestWithFunctionsWorkerMetadataDefault(t *testing.T) {
	if got := withFunctionsWorkerMetadataDefault(nil, testMetadataClusterName); got[configKeyConfigurationMetadataStoreURL] != testWantOxiaURL {
		t.Errorf("configurationMetadataStoreUrl = %q, want %q", got[configKeyConfigurationMetadataStoreURL], testWantOxiaURL)
	}

	cfg := map[string]string{configKeyConfigurationMetadataStoreURL: testUserStoreURL}
	got := withFunctionsWorkerMetadataDefault(cfg, testMetadataClusterName)
	if got[configKeyConfigurationMetadataStoreURL] != testUserStoreURL {
		t.Errorf("configurationMetadataStoreUrl = %q, want user value preserved", got[configKeyConfigurationMetadataStoreURL])
	}
}

func TestMetadataInitJobName(t *testing.T) {
	if got, want := metadataInitJobName(testMetadataClusterName), "test-cluster-metadata-init"; got != want {
		t.Errorf("metadataInitJobName() = %q, want %q", got, want)
	}
}

func TestBuildMetadataInitJob(t *testing.T) {
	cluster := &clusterv1alpha1.PulsarCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testMetadataClusterName, Namespace: "pulsar-ns"},
		Spec:       clusterv1alpha1.PulsarClusterSpec{Image: testClusterImage},
	}

	job := buildMetadataInitJob(cluster)

	if job.Name != "test-cluster-metadata-init" {
		t.Errorf("Name = %q, want %q", job.Name, "test-cluster-metadata-init")
	}
	if job.Namespace != "pulsar-ns" {
		t.Errorf("Namespace = %q, want %q", job.Namespace, "pulsar-ns")
	}
	if job.Spec.Template.Spec.RestartPolicy != corev1.RestartPolicyOnFailure {
		t.Errorf("RestartPolicy = %q, want %q", job.Spec.Template.Spec.RestartPolicy, corev1.RestartPolicyOnFailure)
	}
	if len(job.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("Containers = %d, want 1", len(job.Spec.Template.Spec.Containers))
	}

	container := job.Spec.Template.Spec.Containers[0]
	if container.Image != testClusterImage {
		t.Errorf("Image = %q, want %q", container.Image, testClusterImage)
	}
	if len(container.Command) != 1 || container.Command[0] != cmdBinPulsar {
		t.Errorf("Command = %v, want [%q]", container.Command, cmdBinPulsar)
	}

	wantArgs := []string{
		"initialize-cluster-metadata",
		"--cluster", testMetadataClusterName,
		"--metadata-store", testWantOxiaURL,
		"--configuration-store", testWantOxiaURL,
		"--web-service-url", "http://test-cluster-broker:8080",
		"--broker-service-url", "pulsar://test-cluster-broker:6650",
	}
	if len(container.Args) != len(wantArgs) {
		t.Fatalf("Args = %v, want %v", container.Args, wantArgs)
	}
	for i, want := range wantArgs {
		if container.Args[i] != want {
			t.Errorf("Args[%d] = %q, want %q", i, container.Args[i], want)
		}
	}
}

func TestMetadataInitializedCondition(t *testing.T) {
	const generation = int64(3)

	t.Run("waiting for oxia", func(t *testing.T) {
		got := metadataInitializedCondition(generation, false, nil)
		if got.Status != metav1.ConditionFalse || got.Reason != reasonMetadataInitWaitingForOxia {
			t.Errorf("got %+v, want False/%s", got, reasonMetadataInitWaitingForOxia)
		}
	})

	t.Run("oxia ready but job not created yet", func(t *testing.T) {
		got := metadataInitializedCondition(generation, true, nil)
		if got.Status != metav1.ConditionFalse || got.Reason != reasonMetadataInitJobRunning {
			t.Errorf("got %+v, want False/%s", got, reasonMetadataInitJobRunning)
		}
	})

	t.Run("job running", func(t *testing.T) {
		job := &batchv1.Job{}
		got := metadataInitializedCondition(generation, true, job)
		if got.Status != metav1.ConditionFalse || got.Reason != reasonMetadataInitJobRunning {
			t.Errorf("got %+v, want False/%s", got, reasonMetadataInitJobRunning)
		}
	})

	t.Run("job succeeded via status count", func(t *testing.T) {
		job := &batchv1.Job{Status: batchv1.JobStatus{Succeeded: 1}}
		got := metadataInitializedCondition(generation, true, job)
		if got.Status != metav1.ConditionTrue || got.Reason != reasonMetadataInitJobSucceeded {
			t.Errorf("got %+v, want True/%s", got, reasonMetadataInitJobSucceeded)
		}
	})

	t.Run("job succeeded via Complete condition", func(t *testing.T) {
		job := &batchv1.Job{Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}},
		}}
		got := metadataInitializedCondition(generation, true, job)
		if got.Status != metav1.ConditionTrue || got.Reason != reasonMetadataInitJobSucceeded {
			t.Errorf("got %+v, want True/%s", got, reasonMetadataInitJobSucceeded)
		}
	})

	t.Run("job failed permanently", func(t *testing.T) {
		job := &batchv1.Job{Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}},
		}}
		got := metadataInitializedCondition(generation, true, job)
		if got.Status != metav1.ConditionFalse || got.Reason != reasonMetadataInitJobFailed {
			t.Errorf("got %+v, want False/%s", got, reasonMetadataInitJobFailed)
		}
	})

	if got := metadataInitializedCondition(generation, true, nil).Type; got != conditionTypeMetadataInitialized {
		t.Errorf("Type = %q, want %q", got, conditionTypeMetadataInitialized)
	}
}
