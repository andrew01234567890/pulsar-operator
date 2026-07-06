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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	"github.com/andrew01234567890/pulsar-operator/internal/builder"
)

const testCustomJournalDir = "/custom/journal"

func int32Ptr(v int32) *int32 { return &v }

func TestMergeConfig_OperatorDefaults(t *testing.T) {
	merged, rendered := mergeConfig(clusterv1alpha1.BookKeeperSpec{})

	for _, key := range []string{keyJournalDirectories, keyLedgerDirectories, keyIndexDirectories, "bookiePort", "httpServerEnabled", "httpServerPort"} {
		if _, ok := merged[key]; !ok {
			t.Errorf("mergeConfig() defaults missing key %q, got %v", key, merged)
		}
	}

	if _, ok := merged["metadataServiceUri"]; ok {
		t.Errorf("mergeConfig() must not hardcode metadataServiceUri, got %q", merged["metadataServiceUri"])
	}
	if strings.Contains(rendered, "metadataServiceUri") {
		t.Errorf("rendered config must not mention metadataServiceUri unless the user set it, got %q", rendered)
	}
}

func TestMergeConfig_UserOverridesWin(t *testing.T) {
	spec := clusterv1alpha1.BookKeeperSpec{
		Config: map[string]string{
			"metadataServiceUri":  "metadata-store:oxia://my-oxia:6648/bookkeeper",
			keyJournalDirectories: testCustomJournalDir,
		},
	}

	merged, rendered := mergeConfig(spec)

	if got := merged["metadataServiceUri"]; got != "metadata-store:oxia://my-oxia:6648/bookkeeper" {
		t.Errorf("merged[metadataServiceUri] = %q, want user override", got)
	}
	if got := merged[keyJournalDirectories]; got != testCustomJournalDir {
		t.Errorf("merged[%s] = %q, want user override to win over operator default", keyJournalDirectories, got)
	}
	if !strings.Contains(rendered, "journalDirectories="+testCustomJournalDir) {
		t.Errorf("rendered config does not reflect overridden journalDirectories: %q", rendered)
	}
}

// TestMergeConfig_Regression pins the exact rendered bookkeeper.conf for the
// zero-value spec. If operator defaults ever change, every existing bookie
// pod-template checksum flips and forces a cluster-wide rolling restart, so
// this must only change deliberately.
func TestMergeConfig_Regression(t *testing.T) {
	const want = "bookiePort=3181\n" +
		"httpServerEnabled=true\n" +
		"httpServerPort=8000\n" +
		"indexDirectories=/pulsar/data/bookkeeper/index\n" +
		"journalDirectories=/pulsar/data/bookkeeper/journal\n" +
		"ledgerDirectories=/pulsar/data/bookkeeper/ledgers\n"

	_, rendered := mergeConfig(clusterv1alpha1.BookKeeperSpec{})
	if rendered != want {
		t.Errorf("mergeConfig() rendered = %q, want %q (confirm this drift is intentional)", rendered, want)
	}
}

func TestResolveReplicas(t *testing.T) {
	tests := []struct {
		name string
		spec clusterv1alpha1.BookKeeperSpec
		want int32
	}{
		{name: "unset falls back to HA default", spec: clusterv1alpha1.BookKeeperSpec{}, want: defaultBookKeeperReplicas},
		{name: "explicit value wins", spec: clusterv1alpha1.BookKeeperSpec{Replicas: int32Ptr(6)}, want: 6},
		{name: "explicit zero is honored, not treated as unset", spec: clusterv1alpha1.BookKeeperSpec{Replicas: int32Ptr(0)}, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveReplicas(tt.spec); got != tt.want {
				t.Errorf("resolveReplicas(%+v) = %d, want %d", tt.spec, got, tt.want)
			}
		})
	}
}

func TestResolveImage(t *testing.T) {
	tests := []struct {
		name string
		spec clusterv1alpha1.BookKeeperSpec
		want string
	}{
		{name: "unset falls back to default image", spec: clusterv1alpha1.BookKeeperSpec{}, want: defaultBookieImage},
		{name: "explicit image wins", spec: clusterv1alpha1.BookKeeperSpec{Image: "my-registry/pulsar:custom"}, want: "my-registry/pulsar:custom"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveImage(tt.spec); got != tt.want {
				t.Errorf("resolveImage(%+v) = %q, want %q", tt.spec, got, tt.want)
			}
		})
	}
}

func TestResolvePodManagementPolicy(t *testing.T) {
	tests := []struct {
		name string
		spec clusterv1alpha1.BookKeeperSpec
		want string
	}{
		{name: "unset defaults to Parallel", spec: clusterv1alpha1.BookKeeperSpec{}, want: "Parallel"},
		{name: "explicit OrderedReady is honored", spec: clusterv1alpha1.BookKeeperSpec{PodManagementPolicy: "OrderedReady"}, want: "OrderedReady"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := string(resolvePodManagementPolicy(tt.spec)); got != tt.want {
				t.Errorf("resolvePodManagementPolicy(%+v) = %q, want %q", tt.spec, got, tt.want)
			}
		})
	}
}

func TestResolvePDBMaxUnavailable(t *testing.T) {
	tests := []struct {
		name               string
		replicas           int32
		writeQuorum        int32
		wantMaxUnavailable int32
	}{
		{name: "typical ensemble tolerates replicas-writeQuorum", replicas: 4, writeQuorum: 2, wantMaxUnavailable: 2},
		{name: "replicas equal to writeQuorum tolerates none", replicas: 2, writeQuorum: 2, wantMaxUnavailable: 0},
		{name: "replicas below writeQuorum never goes negative", replicas: 1, writeQuorum: 2, wantMaxUnavailable: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolvePDBMaxUnavailable(tt.replicas, tt.writeQuorum)
			want := intstr.FromInt32(tt.wantMaxUnavailable)
			if got != want {
				t.Errorf("resolvePDBMaxUnavailable(%d, %d) = %v, want %v", tt.replicas, tt.writeQuorum, got, want)
			}
		})
	}
}

func TestBuildVolumeClaimTemplates(t *testing.T) {
	labels := map[string]string{"app.kubernetes.io/component": bookkeeperComponent}

	t.Run("defaults when spec.volumes is unset", func(t *testing.T) {
		templates := buildVolumeClaimTemplates(clusterv1alpha1.BookKeeperSpec{}, labels)

		if len(templates) != 3 {
			t.Fatalf("got %d volumeClaimTemplates, want 3 (journal, ledgers, index)", len(templates))
		}

		wantNames := []string{volumeNameJournal, volumeNameLedgers, volumeNameIndex}
		wantSizes := []resource.Quantity{defaultJournalSize, defaultLedgerSize, defaultIndexSize}
		for i, tmpl := range templates {
			if tmpl.Name != wantNames[i] {
				t.Errorf("templates[%d].Name = %q, want %q", i, tmpl.Name, wantNames[i])
			}
			got := tmpl.Spec.Resources.Requests[corev1.ResourceStorage]
			if got.Cmp(wantSizes[i]) != 0 {
				t.Errorf("templates[%d] storage request = %v, want %v", i, got, wantSizes[i])
			}
			if tmpl.Spec.StorageClassName != nil {
				t.Errorf("templates[%d].StorageClassName = %v, want nil (let cluster default apply)", i, tmpl.Spec.StorageClassName)
			}
			if len(tmpl.Spec.AccessModes) != 1 || tmpl.Spec.AccessModes[0] != corev1.ReadWriteOnce {
				t.Errorf("templates[%d].AccessModes = %v, want [ReadWriteOnce]", i, tmpl.Spec.AccessModes)
			}
		}
	})

	t.Run("spec overrides size and storage class per disk role", func(t *testing.T) {
		sc := "fast-ssd"
		size := resource.MustParse("200Gi")
		spec := clusterv1alpha1.BookKeeperSpec{
			Volumes: &clusterv1alpha1.BookKeeperVolumes{
				Ledgers: &clusterv1alpha1.VolumeSpec{StorageClassName: &sc, Size: size},
			},
		}

		templates := buildVolumeClaimTemplates(spec, labels)

		ledgerTmpl := templates[1]
		if ledgerTmpl.Name != volumeNameLedgers {
			t.Fatalf("templates[1].Name = %q, want %q", ledgerTmpl.Name, volumeNameLedgers)
		}
		got := ledgerTmpl.Spec.Resources.Requests[corev1.ResourceStorage]
		if got.Cmp(size) != 0 {
			t.Errorf("ledgers storage request = %v, want %v", got, size)
		}
		if ledgerTmpl.Spec.StorageClassName == nil || *ledgerTmpl.Spec.StorageClassName != sc {
			t.Errorf("ledgers StorageClassName = %v, want %q", ledgerTmpl.Spec.StorageClassName, sc)
		}

		// journal/index were not overridden and must keep operator defaults.
		journalGot := templates[0].Spec.Resources.Requests[corev1.ResourceStorage]
		if journalGot.Cmp(defaultJournalSize) != 0 {
			t.Errorf("journal storage request = %v, want default %v", journalGot, defaultJournalSize)
		}
	})
}

func TestBuildBookieContainer_VolumeMountsMatchRenderedDirectories(t *testing.T) {
	spec := clusterv1alpha1.BookKeeperSpec{
		Config: map[string]string{keyJournalDirectories: testCustomJournalDir},
	}
	merged, _ := mergeConfig(spec)

	container := buildBookieContainer(defaultBookieImage, merged)

	mounts := map[string]string{}
	for _, m := range container.VolumeMounts {
		mounts[m.Name] = m.MountPath
	}

	if mounts[volumeNameJournal] != testCustomJournalDir {
		t.Errorf("journal volumeMount path = %q, want %q (must track the rendered journalDirectories value)", mounts[volumeNameJournal], testCustomJournalDir)
	}
	if mounts[volumeNameLedgers] != defaultLedgerDir {
		t.Errorf("ledgers volumeMount path = %q, want %q", mounts[volumeNameLedgers], defaultLedgerDir)
	}
	if mounts[volumeNameIndex] != defaultIndexDir {
		t.Errorf("index volumeMount path = %q, want %q", mounts[volumeNameIndex], defaultIndexDir)
	}
}

func TestComputeReadyCondition(t *testing.T) {
	tests := []struct {
		name            string
		desiredReplicas int32
		readyReplicas   int32
		wantStatus      metav1.ConditionStatus
		wantReason      string
	}{
		{name: "all replicas ready", desiredReplicas: 4, readyReplicas: 4, wantStatus: metav1.ConditionTrue, wantReason: reasonAllReady},
		{name: "partially ready", desiredReplicas: 4, readyReplicas: 2, wantStatus: metav1.ConditionFalse, wantReason: reasonProgressing},
		{name: "none ready yet", desiredReplicas: 4, readyReplicas: 0, wantStatus: metav1.ConditionFalse, wantReason: reasonProgressing},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cond := computeReadyCondition(1, tt.desiredReplicas, tt.readyReplicas)
			if cond.Status != tt.wantStatus {
				t.Errorf("computeReadyCondition(...).Status = %v, want %v", cond.Status, tt.wantStatus)
			}
			if cond.Reason != tt.wantReason {
				t.Errorf("computeReadyCondition(...).Reason = %q, want %q", cond.Reason, tt.wantReason)
			}
			if cond.Type != conditionTypeReady {
				t.Errorf("computeReadyCondition(...).Type = %q, want %q", cond.Type, conditionTypeReady)
			}
		})
	}
}

// TestComputeReadyCondition_ZeroDesiredIsNotVacuouslyReady guards against a
// freshly created StatefulSet (whose Status is all zeros before any pod
// exists) reading as Ready on the very first reconcile just because
// 0 == 0.
func TestComputeReadyCondition_ZeroDesiredIsNotVacuouslyReady(t *testing.T) {
	cond := computeReadyCondition(1, 0, 0)
	if cond.Status != metav1.ConditionFalse {
		t.Errorf("computeReadyCondition(1, 0, 0).Status = %v, want %v (zero desired replicas must not read as trivially Ready)", cond.Status, metav1.ConditionFalse)
	}
	if cond.Reason != reasonNoReplicas {
		t.Errorf("computeReadyCondition(1, 0, 0).Reason = %q, want %q", cond.Reason, reasonNoReplicas)
	}
}

func TestDesiredStatefulSet_HasThreeVolumeClaimTemplatesAndChecksumAnnotation(t *testing.T) {
	bk := &clusterv1alpha1.BookKeeper{
		ObjectMeta: metav1.ObjectMeta{Name: "test-bk", Namespace: "default"},
		Spec:       clusterv1alpha1.BookKeeperSpec{Replicas: int32Ptr(3)},
	}
	merged, rendered := mergeConfig(bk.Spec)

	sts := desiredStatefulSet(bk, defaultBookieImage, merged, rendered)

	if len(sts.Spec.VolumeClaimTemplates) != 3 {
		t.Fatalf("got %d volumeClaimTemplates, want 3", len(sts.Spec.VolumeClaimTemplates))
	}
	if sts.Spec.PersistentVolumeClaimRetentionPolicy != nil {
		t.Errorf("PersistentVolumeClaimRetentionPolicy = %+v, want nil (must not opt into PVC deletion yet)", sts.Spec.PersistentVolumeClaimRetentionPolicy)
	}
	if sts.Spec.ServiceName != bk.Name {
		t.Errorf("ServiceName = %q, want %q", sts.Spec.ServiceName, bk.Name)
	}
	if *sts.Spec.Replicas != 3 {
		t.Errorf("Replicas = %d, want 3", *sts.Spec.Replicas)
	}

	annotations := sts.Spec.Template.Annotations
	checksum, ok := annotations[builder.ConfigChecksumAnnotation]
	if !ok || checksum == "" {
		t.Errorf("pod template annotations = %v, want a non-empty %q", annotations, builder.ConfigChecksumAnnotation)
	}
}
