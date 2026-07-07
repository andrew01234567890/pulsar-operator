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
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
)

const (
	testMetadataClusterName = "test-cluster"
	testMetadataNamespace   = "pulsar-ns"

	// testMetadataInitName is metadataInitJobName(testMetadataClusterName),
	// shared by the Job and ConfigMap this file's tests assert on (both are
	// named identically - see buildMetadataInitConfigMap/buildMetadataInitJob).
	testMetadataInitName = "test-cluster-metadata-init"

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

func TestWithClusterNameDefault(t *testing.T) {
	if got := withClusterNameDefault(nil, testMetadataClusterName); got[configKeyClusterName] != testMetadataClusterName {
		t.Errorf("clusterName = %q, want %q", got[configKeyClusterName], testMetadataClusterName)
	}

	const userClusterName = "user-set-cluster"
	cfg := map[string]string{configKeyClusterName: userClusterName}
	got := withClusterNameDefault(cfg, testMetadataClusterName)
	if got[configKeyClusterName] != userClusterName {
		t.Errorf("clusterName = %q, want user value preserved", got[configKeyClusterName])
	}
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

func TestWithBrokerBookkeeperMetadataDefault(t *testing.T) {
	const (
		wantURI = "metadata-store:oxia://test-cluster-oxia-oxia:6648/bookkeeper"
		userURI = "metadata-store:oxia://user-store:6648/bookkeeper"
	)

	// The broker's bookkeeperMetadataServiceUri MUST equal the bookies' own
	// metadataServiceUri (same "bookkeeper" Oxia namespace), or the broker
	// can't find the bookies and every ledger create fails rc=-6.
	if got := withBrokerBookkeeperMetadataDefault(nil, testMetadataClusterName)[configKeyBookkeeperMetadataServiceURI]; got != wantURI {
		t.Errorf("bookkeeperMetadataServiceUri = %q, want %q", got, wantURI)
	}
	if got := withBookKeeperMetadataDefault(nil, testMetadataClusterName)[configKeyMetadataServiceURI]; got != wantURI {
		t.Errorf("broker bookkeeperMetadataServiceUri must match the bookie metadataServiceUri, bookie got %q", got)
	}

	cfg := map[string]string{configKeyBookkeeperMetadataServiceURI: userURI}
	if got := withBrokerBookkeeperMetadataDefault(cfg, testMetadataClusterName)[configKeyBookkeeperMetadataServiceURI]; got != userURI {
		t.Errorf("bookkeeperMetadataServiceUri = %q, want user value preserved", got)
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
	if got, want := metadataInitJobName(testMetadataClusterName), testMetadataInitName; got != want {
		t.Errorf("metadataInitJobName() = %q, want %q", got, want)
	}
}

func TestBuildMetadataInitJob(t *testing.T) {
	cluster := &clusterv1alpha1.PulsarCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testMetadataClusterName, Namespace: testMetadataNamespace},
		Spec:       clusterv1alpha1.PulsarClusterSpec{Image: testClusterImage},
	}

	job := buildMetadataInitJob(cluster)

	if job.Name != testMetadataInitName {
		t.Errorf("Name = %q, want %q", job.Name, testMetadataInitName)
	}
	if job.Namespace != testMetadataNamespace {
		t.Errorf("Namespace = %q, want %q", job.Namespace, testMetadataNamespace)
	}
	if job.Spec.Template.Spec.RestartPolicy != corev1.RestartPolicyOnFailure {
		t.Errorf("RestartPolicy = %q, want %q", job.Spec.Template.Spec.RestartPolicy, corev1.RestartPolicyOnFailure)
	}
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != metadataInitBackoffLimit {
		t.Errorf("BackoffLimit = %v, want %d", job.Spec.BackoffLimit, metadataInitBackoffLimit)
	}
	if len(job.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("Containers = %d, want 1", len(job.Spec.Template.Spec.Containers))
	}

	container := job.Spec.Template.Spec.Containers[0]
	if container.Image != testClusterImage {
		t.Errorf("Image = %q, want %q", container.Image, testClusterImage)
	}
	if len(container.Command) != 2 || container.Command[0] != "sh" || container.Command[1] != "-c" {
		t.Errorf("Command = %v, want [sh -c]", container.Command)
	}
	if len(container.Args) != 1 {
		t.Fatalf("Args = %d, want 1 (single script)", len(container.Args))
	}

	script := container.Args[0]
	wantSubstrings := []string{
		"bin/bookkeeper shell whatisinstanceid",
		"bin/bookkeeper shell initnewcluster",
		`--cluster "` + testMetadataClusterName + `"`,
		`--metadata-store "` + testWantOxiaURL + `"`,
		`--configuration-store "` + testWantOxiaURL + `"`,
		`--web-service-url "http://test-cluster-broker:8080"`,
		`--broker-service-url "pulsar://test-cluster-broker:6650"`,
	}
	for _, want := range wantSubstrings {
		if !strings.Contains(script, want) {
			t.Errorf("script does not contain %q\nscript:\n%s", want, script)
		}
	}

	// Regression (Bug A): bookies abort with "BookKeeper cluster not
	// initialized" unless BookKeeper's own cluster metadata is bootstrapped
	// before Pulsar's initialize-cluster-metadata runs.
	if strings.Index(script, "initnewcluster") > strings.Index(script, "initialize-cluster-metadata") {
		t.Errorf("bookkeeper initnewcluster must run before pulsar initialize-cluster-metadata:\n%s", script)
	}

	if len(container.VolumeMounts) != 1 || container.VolumeMounts[0].MountPath != bookieConfMountPath {
		t.Errorf("VolumeMounts = %+v, want a single mount at %q", container.VolumeMounts, bookieConfMountPath)
	}

	wantCMName := testMetadataInitName
	if len(job.Spec.Template.Spec.Volumes) != 1 || job.Spec.Template.Spec.Volumes[0].ConfigMap == nil ||
		job.Spec.Template.Spec.Volumes[0].ConfigMap.Name != wantCMName {
		t.Errorf("Volumes = %+v, want a configMap volume named %q", job.Spec.Template.Spec.Volumes, wantCMName)
	}
}

// Regression: initialize-cluster-metadata PERSISTS the advertised broker/web
// URLs in Oxia, so the Job must build them from the broker's real (possibly
// overridden) ports - the same brokerServicePort/webServicePort the Broker
// reconciler binds - not the hardcoded defaults.
func TestBuildMetadataInitJobHonorsBrokerPortOverrides(t *testing.T) {
	cluster := &clusterv1alpha1.PulsarCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testMetadataClusterName, Namespace: testMetadataNamespace},
		Spec: clusterv1alpha1.PulsarClusterSpec{
			Image: testClusterImage,
			Broker: &clusterv1alpha1.BrokerSpec{
				Config: map[string]string{
					confKeyBrokerServicePort: "7650",
					confKeyWebServicePort:    "9090",
				},
			},
		},
	}

	script := buildMetadataInitJob(cluster).Spec.Template.Spec.Containers[0].Args[0]

	if !strings.Contains(script, `--web-service-url "http://test-cluster-broker:9090"`) {
		t.Errorf("script does not honor overridden web service port:\n%s", script)
	}
	if !strings.Contains(script, `--broker-service-url "pulsar://test-cluster-broker:7650"`) {
		t.Errorf("script does not honor overridden broker service port:\n%s", script)
	}
}

func TestBuildMetadataInitConfigMap(t *testing.T) {
	cluster := &clusterv1alpha1.PulsarCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testMetadataClusterName, Namespace: testMetadataNamespace},
		Spec:       clusterv1alpha1.PulsarClusterSpec{Image: testClusterImage},
	}

	cm := buildMetadataInitConfigMap(cluster)

	if cm.Name != testMetadataInitName {
		t.Errorf("Name = %q, want %q", cm.Name, testMetadataInitName)
	}
	if cm.Namespace != testMetadataNamespace {
		t.Errorf("Namespace = %q, want %q", cm.Namespace, testMetadataNamespace)
	}

	const wantURI = "metadataServiceUri=metadata-store:oxia://test-cluster-oxia-oxia:6648/bookkeeper\n"
	if got := cm.Data[configMapKey]; got != wantURI {
		t.Errorf("Data[%q] = %q, want %q", configMapKey, got, wantURI)
	}
}

func TestMetadataInitializedCondition(t *testing.T) {
	const generation = int64(3)

	t.Run("waiting for oxia", func(t *testing.T) {
		got := metadataInitializedCondition(generation, false, nil, false)
		if got.Status != metav1.ConditionFalse || got.Reason != reasonMetadataInitWaitingForOxia {
			t.Errorf("got %+v, want False/%s", got, reasonMetadataInitWaitingForOxia)
		}
	})

	t.Run("oxia ready but job not created yet", func(t *testing.T) {
		got := metadataInitializedCondition(generation, true, nil, false)
		if got.Status != metav1.ConditionFalse || got.Reason != reasonMetadataInitJobRunning {
			t.Errorf("got %+v, want False/%s", got, reasonMetadataInitJobRunning)
		}
	})

	t.Run("job running", func(t *testing.T) {
		job := &batchv1.Job{}
		got := metadataInitializedCondition(generation, true, job, false)
		if got.Status != metav1.ConditionFalse || got.Reason != reasonMetadataInitJobRunning {
			t.Errorf("got %+v, want False/%s", got, reasonMetadataInitJobRunning)
		}
	})

	t.Run("job succeeded via status count", func(t *testing.T) {
		job := &batchv1.Job{Status: batchv1.JobStatus{Succeeded: 1}}
		got := metadataInitializedCondition(generation, true, job, false)
		if got.Status != metav1.ConditionTrue || got.Reason != reasonMetadataInitJobSucceeded {
			t.Errorf("got %+v, want True/%s", got, reasonMetadataInitJobSucceeded)
		}
	})

	t.Run("job succeeded via Complete condition", func(t *testing.T) {
		job := &batchv1.Job{Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}},
		}}
		got := metadataInitializedCondition(generation, true, job, false)
		if got.Status != metav1.ConditionTrue || got.Reason != reasonMetadataInitJobSucceeded {
			t.Errorf("got %+v, want True/%s", got, reasonMetadataInitJobSucceeded)
		}
	})

	t.Run("job failed permanently", func(t *testing.T) {
		job := &batchv1.Job{Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}},
		}}
		got := metadataInitializedCondition(generation, true, job, false)
		if got.Status != metav1.ConditionFalse || got.Reason != reasonMetadataInitJobFailed {
			t.Errorf("got %+v, want False/%s", got, reasonMetadataInitJobFailed)
		}
	})

	t.Run("succeeded job stays True even when oxia is not Ready (monotonic)", func(t *testing.T) {
		// Regression: cluster-metadata bootstrap is a permanent one-time fact.
		// A transient Oxia readiness blip (oxiaReady=false) must NOT flip a
		// previously-succeeded MetadataInitialized back to False/WaitingForOxia.
		job := &batchv1.Job{Status: batchv1.JobStatus{Succeeded: 1}}
		got := metadataInitializedCondition(generation, false, job, false)
		if got.Status != metav1.ConditionTrue || got.Reason != reasonMetadataInitJobSucceeded {
			t.Errorf("got %+v, want True/%s", got, reasonMetadataInitJobSucceeded)
		}
	})

	t.Run("already-initialized stays True when the Job object is absent (deleted succeeded Job)", func(t *testing.T) {
		// Regression: a deleted succeeded Job (kubectl delete job, finished-Job
		// TTL/cleanup) must not regress a previously-True MetadataInitialized
		// to False/JobRunning or JobFailed - bootstrap is a permanent fact.
		got := metadataInitializedCondition(generation, true, nil, true)
		if got.Status != metav1.ConditionTrue || got.Reason != reasonMetadataInitJobSucceeded {
			t.Errorf("got %+v, want True/%s", got, reasonMetadataInitJobSucceeded)
		}
	})

	t.Run("already-initialized stays True even when oxia is not Ready and the Job is absent", func(t *testing.T) {
		got := metadataInitializedCondition(generation, false, nil, true)
		if got.Status != metav1.ConditionTrue || got.Reason != reasonMetadataInitJobSucceeded {
			t.Errorf("got %+v, want True/%s", got, reasonMetadataInitJobSucceeded)
		}
	})

	if got := metadataInitializedCondition(generation, true, nil, false).Type; got != conditionTypeMetadataInitialized {
		t.Errorf("Type = %q, want %q", got, conditionTypeMetadataInitialized)
	}
}
