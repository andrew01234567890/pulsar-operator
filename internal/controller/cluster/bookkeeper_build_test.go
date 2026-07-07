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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	"github.com/andrew01234567890/pulsar-operator/internal/builder"
)

const (
	keyMetadataServiceURI = "metadataServiceUri"

	testNamespaceDefault = "default"
	testStorageClass     = "fast-ssd"
	testRevision1        = "rev-1"
	testRevision2        = "rev-2"
	testBookieName       = "test-bk"
)

func int32Ptr(v int32) *int32 { return &v }

func TestMergeConfig_OperatorDefaults(t *testing.T) {
	merged, rendered := mergeConfig(clusterv1alpha1.BookKeeperSpec{})

	for _, key := range []string{keyJournalDirectories, keyLedgerDirectories, keyIndexDirectories, keyBookiePort, keyHTTPServerEnabled, keyHTTPServerPort} {
		if _, ok := merged[key]; !ok {
			t.Errorf("mergeConfig() defaults missing key %q, got %v", key, merged)
		}
	}

	if _, ok := merged[keyMetadataServiceURI]; ok {
		t.Errorf("mergeConfig() must not hardcode metadataServiceUri, got %q", merged[keyMetadataServiceURI])
	}
	if strings.Contains(rendered, keyMetadataServiceURI) {
		t.Errorf("rendered config must not mention metadataServiceUri unless the user set it, got %q", rendered)
	}
}

func TestMergeConfig_UserOverridesWinForNonManagedKeys(t *testing.T) {
	spec := clusterv1alpha1.BookKeeperSpec{
		Config: map[string]string{
			keyMetadataServiceURI: "metadata-store:oxia://my-oxia:6648/bookkeeper",
			"journalMaxBackups":   "5",
		},
	}

	merged, rendered := mergeConfig(spec)

	if got := merged[keyMetadataServiceURI]; got != "metadata-store:oxia://my-oxia:6648/bookkeeper" {
		t.Errorf("merged[metadataServiceUri] = %q, want user override", got)
	}
	if got := merged["journalMaxBackups"]; got != "5" {
		t.Errorf("merged[journalMaxBackups] = %q, want user override to be applied", got)
	}
	if !strings.Contains(rendered, "journalMaxBackups=5") {
		t.Errorf("rendered config does not reflect overridden journalMaxBackups: %q", rendered)
	}
}

// TestMergeConfig_OperatorManagedKeysAreNotUserOverridable proves the root fix:
// a user override of any structural/wiring key is discarded in favor of the
// operator's value, so the rendered config can never desync from the generated
// Service/probes/mounts. Non-managed keys still pass through.
func TestMergeConfig_OperatorManagedKeysAreNotUserOverridable(t *testing.T) {
	spec := clusterv1alpha1.BookKeeperSpec{
		Config: map[string]string{
			keyBookiePort:         "9999",
			keyHTTPServerPort:     "1234",
			keyHTTPServerEnabled:  "false",
			keyJournalDirectories: "/tmp/a,/tmp/b",
			keyLedgerDirectories:  "",
			keyIndexDirectories:   "/somewhere/else",
			"customKey":           "userValue",
		},
	}

	merged, _ := mergeConfig(spec)

	managed := map[string]string{
		keyBookiePort:         "3181",
		keyHTTPServerPort:     "8000",
		keyHTTPServerEnabled:  "true",
		keyJournalDirectories: journalMountPath,
		keyLedgerDirectories:  ledgerMountPath,
		keyIndexDirectories:   indexMountPath,
	}
	for key, want := range managed {
		if got := merged[key]; got != want {
			t.Errorf("merged[%s] = %q, want operator-managed value %q (user override must be discarded)", key, got, want)
		}
	}

	if got := merged["customKey"]; got != "userValue" {
		t.Errorf("merged[customKey] = %q, want non-managed user value to pass through", got)
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

func TestResolveWriteQuorum(t *testing.T) {
	tests := []struct {
		name string
		spec clusterv1alpha1.BookKeeperSpec
		want int32
	}{
		{name: "unset ensemble falls back to default", spec: clusterv1alpha1.BookKeeperSpec{}, want: defaultWriteQuorum},
		{
			name: "explicit writeQuorum wins",
			spec: clusterv1alpha1.BookKeeperSpec{Ensemble: &clusterv1alpha1.BookKeeperEnsembleSpec{WriteQuorum: int32Ptr(3)}},
			want: 3,
		},
		{
			name: "ensemble present but writeQuorum nil falls back to default",
			spec: clusterv1alpha1.BookKeeperSpec{Ensemble: &clusterv1alpha1.BookKeeperEnsembleSpec{EnsembleSize: int32Ptr(4)}},
			want: defaultWriteQuorum,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveWriteQuorum(tt.spec); got != tt.want {
				t.Errorf("resolveWriteQuorum(%+v) = %d, want %d", tt.spec, got, tt.want)
			}
		})
	}
}

// TestResolvePDBMaxUnavailable is the regression test for the bookie quorum
// PDB math: maxUnavailable = max(writeQuorum-ackQuorum, 1), clamped to at
// most replicas-1. It fails without the ackQuorum-aware formula - e.g. the
// old replicas-writeQuorum formula returned 2 for the "3-AZ ensemble" case
// below instead of the ack-quorum-safe 1.
func TestResolvePDBMaxUnavailable(t *testing.T) {
	tests := []struct {
		name               string
		replicas           int32
		writeQuorum        int32
		ackQuorum          int32
		wantMaxUnavailable int32
	}{
		{name: "prod default 2/2/2 ensemble has zero ack-quorum slack, clamped to 1", replicas: 4, writeQuorum: 2, ackQuorum: 2, wantMaxUnavailable: 1},
		{name: "3-AZ recommended 3/3/2 ensemble has one bookie of ack-quorum slack", replicas: 4, writeQuorum: 3, ackQuorum: 2, wantMaxUnavailable: 1},
		{name: "wide write quorum widens the safe slack", replicas: 6, writeQuorum: 5, ackQuorum: 2, wantMaxUnavailable: 3},
		{name: "slack is capped at replicas-1, never evicting every bookie", replicas: 2, writeQuorum: 2, ackQuorum: 2, wantMaxUnavailable: 1},
		{name: "single bookie blocks all voluntary disruption", replicas: 1, writeQuorum: 2, ackQuorum: 2, wantMaxUnavailable: 0},
		{name: "zero replicas never goes negative", replicas: 0, writeQuorum: 2, ackQuorum: 2, wantMaxUnavailable: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolvePDBMaxUnavailable(tt.replicas, tt.writeQuorum, tt.ackQuorum)
			want := intstr.FromInt32(tt.wantMaxUnavailable)
			if got != want {
				t.Errorf("resolvePDBMaxUnavailable(%d, %d, %d) = %v, want %v", tt.replicas, tt.writeQuorum, tt.ackQuorum, got, want)
			}
		})
	}
}

// TestDesiredPDB_MaxUnavailable pins the quorum-derived maxUnavailable for the
// default (unset) spec: default write/ack quorum are both 2, so the ack-quorum
// slack is 0, clamped up to the practical floor of 1.
func TestDesiredPDB_MaxUnavailable(t *testing.T) {
	bk := &clusterv1alpha1.BookKeeper{
		ObjectMeta: metav1.ObjectMeta{Name: testBookieName, Namespace: testNamespaceDefault},
	}

	pdb := desiredPDB(bk)

	if pdb.Spec.MaxUnavailable == nil {
		t.Fatalf("desiredPDB().Spec.MaxUnavailable = nil, want set")
	}
	if got := *pdb.Spec.MaxUnavailable; got != intstr.FromInt32(1) {
		t.Errorf("desiredPDB() maxUnavailable = %v, want 1 (defaultWriteQuorum 2 - defaultAckQuorum 2 clamped to 1)", got)
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
		sc := testStorageClass
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

func TestBuildBookieContainer_VolumeMountsUseOperatorMountPaths(t *testing.T) {
	container := buildBookieContainer(defaultBookieImage)

	mounts := map[string]string{}
	for _, m := range container.VolumeMounts {
		mounts[m.Name] = m.MountPath
	}

	want := map[string]string{
		volumeNameJournal: journalMountPath,
		volumeNameLedgers: ledgerMountPath,
		volumeNameIndex:   indexMountPath,
	}
	for name, wantPath := range want {
		if mounts[name] != wantPath {
			t.Errorf("%s volumeMount path = %q, want operator mount path %q", name, mounts[name], wantPath)
		}
	}
}

// TestOperatorManagedKeysKeepServiceProbesMountsInSync is the wiring-desync
// regression: even when the user tries to override bookiePort/httpServerPort/
// ledgerDirectories in spec.config, the generated headless Service ports,
// container probes, and volume mounts follow the operator-managed values that
// the rendered bookkeeper.conf also carries — the two can never disagree.
func TestOperatorManagedKeysKeepServiceProbesMountsInSync(t *testing.T) {
	spec := clusterv1alpha1.BookKeeperSpec{
		Config: map[string]string{
			keyBookiePort:        "9999",
			keyHTTPServerPort:    "1234",
			keyLedgerDirectories: "/tmp/x,/tmp/y",
		},
	}
	bk := &clusterv1alpha1.BookKeeper{
		ObjectMeta: metav1.ObjectMeta{Name: testBookieName, Namespace: testNamespaceDefault},
		Spec:       spec,
	}
	merged, _ := mergeConfig(spec)

	wantBookiePort := merged[keyBookiePort]
	wantHTTPPort := merged[keyHTTPServerPort]
	wantLedgerDir := merged[keyLedgerDirectories]

	svc := desiredHeadlessService(bk)
	svcPorts := map[string]corev1.ServicePort{}
	for _, p := range svc.Spec.Ports {
		svcPorts[p.Name] = p
	}
	if got := strconv.Itoa(int(svcPorts[bookieContainerName].Port)); got != wantBookiePort {
		t.Errorf("headless Service bookie port = %s, want config value %s", got, wantBookiePort)
	}
	if svcPorts[bookieContainerName].TargetPort != intstr.FromInt32(bookiePort) {
		t.Errorf("headless Service bookie targetPort = %v, want %d", svcPorts[bookieContainerName].TargetPort, bookiePort)
	}
	if got := strconv.Itoa(int(svcPorts["http"].Port)); got != wantHTTPPort {
		t.Errorf("headless Service http port = %s, want config value %s", got, wantHTTPPort)
	}

	container := buildBookieContainer(defaultBookieImage)

	if got := container.LivenessProbe.TCPSocket.Port; got != intstr.FromInt32(bookiePort) {
		t.Errorf("liveness TCP probe port = %v, want bookie port %d", got, bookiePort)
	}
	if got := strconv.Itoa(container.LivenessProbe.TCPSocket.Port.IntValue()); got != wantBookiePort {
		t.Errorf("liveness TCP probe port = %s, want config value %s", got, wantBookiePort)
	}
	if got := container.ReadinessProbe.HTTPGet.Port; got != intstr.FromInt32(bookieAdminPort) {
		t.Errorf("readiness HTTP probe port = %v, want admin port %d", got, bookieAdminPort)
	}
	if got := strconv.Itoa(container.ReadinessProbe.HTTPGet.Port.IntValue()); got != wantHTTPPort {
		t.Errorf("readiness HTTP probe port = %s, want config value %s", got, wantHTTPPort)
	}

	mounts := map[string]string{}
	for _, m := range container.VolumeMounts {
		mounts[m.Name] = m.MountPath
	}
	if mounts[volumeNameLedgers] != wantLedgerDir {
		t.Errorf("ledgers volumeMount = %q, want config value %q (mount must not follow the user's multi-dir override)", mounts[volumeNameLedgers], wantLedgerDir)
	}
}

// rolledOut is a rolloutState with every pod ready, every pod updated, and no
// revision split — the only shape that should read as Ready.
func rolledOut(ready, updated int32) rolloutState {
	return rolloutState{readyReplicas: ready, updatedReplicas: updated, currentRevision: testRevision1, updateRevision: testRevision1}
}

func TestComputeReadyCondition(t *testing.T) {
	tests := []struct {
		name       string
		desired    int32
		state      rolloutState
		wantStatus metav1.ConditionStatus
		wantReason string
	}{
		{name: "all ready and fully rolled out", desired: 4, state: rolledOut(4, 4), wantStatus: metav1.ConditionTrue, wantReason: reasonAllReady},
		{name: "partially ready", desired: 4, state: rolledOut(2, 4), wantStatus: metav1.ConditionFalse, wantReason: reasonProgressing},
		{name: "none ready yet", desired: 4, state: rolledOut(0, 0), wantStatus: metav1.ConditionFalse, wantReason: reasonProgressing},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cond := computeReadyCondition(1, tt.desired, tt.state)
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

// TestComputeReadyCondition_RolloutAware asserts that a StatefulSet whose pods
// are all Ready but not all on the latest revision (mid config-checksum rolling
// restart) reports NOT Ready, so status can't flash Ready while stale pods run.
func TestComputeReadyCondition_RolloutAware(t *testing.T) {
	tests := []struct {
		name  string
		state rolloutState
	}{
		{
			name:  "revision split: all ready but current != update revision",
			state: rolloutState{readyReplicas: 4, updatedReplicas: 4, currentRevision: testRevision1, updateRevision: testRevision2},
		},
		{
			name:  "not all pods updated to the latest revision yet",
			state: rolloutState{readyReplicas: 4, updatedReplicas: 2, currentRevision: testRevision2, updateRevision: testRevision2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cond := computeReadyCondition(1, 4, tt.state)
			if cond.Status != metav1.ConditionFalse {
				t.Errorf("computeReadyCondition(...).Status = %v, want False (rollout not complete)", cond.Status)
			}
			if cond.Reason != reasonProgressing {
				t.Errorf("computeReadyCondition(...).Reason = %q, want %q", cond.Reason, reasonProgressing)
			}
		})
	}
}

// TestComputeReadyCondition_ZeroDesiredIsNotVacuouslyReady guards against a
// freshly created StatefulSet (whose Status is all zeros before any pod
// exists) reading as Ready on the very first reconcile just because
// 0 == 0.
func TestComputeReadyCondition_ZeroDesiredIsNotVacuouslyReady(t *testing.T) {
	cond := computeReadyCondition(1, 0, rolloutState{})
	if cond.Status != metav1.ConditionFalse {
		t.Errorf("computeReadyCondition(1, 0, {}).Status = %v, want %v (zero desired replicas must not read as trivially Ready)", cond.Status, metav1.ConditionFalse)
	}
	if cond.Reason != reasonNoReplicas {
		t.Errorf("computeReadyCondition(1, 0, {}).Reason = %q, want %q", cond.Reason, reasonNoReplicas)
	}
}

func TestDesiredStatefulSet_HasThreeVolumeClaimTemplatesAndChecksumAnnotation(t *testing.T) {
	bk := &clusterv1alpha1.BookKeeper{
		ObjectMeta: metav1.ObjectMeta{Name: testBookieName, Namespace: testNamespaceDefault},
		Spec:       clusterv1alpha1.BookKeeperSpec{Replicas: int32Ptr(3)},
	}
	_, rendered := mergeConfig(bk.Spec)

	sts := desiredStatefulSet(bk, defaultBookieImage, rendered)

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
