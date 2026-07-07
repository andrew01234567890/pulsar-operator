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
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	bkadmin "github.com/andrew01234567890/pulsar-operator/internal/autoscaler/bookkeeper"
)

// --- mock AdminClient ---

// setReadOnlyCall records one SetReadOnly invocation so tests can assert not
// just whether a bookie ended up read-only, but the exact sequence of calls
// (e.g. that a revert really issued readOnly=false, not just that the final
// state happens to be writable).
type setReadOnlyCall struct {
	podName  string
	readOnly bool
}

// mockAdmin is a hand-written mock of bkadmin.AdminClient: every phase and
// guard of the decommission state machine is driven purely through this
// mock, never a real bookie/pod-exec, per the "injectable exec/admin
// interface so tests can mock it" requirement.
type mockAdmin struct {
	mu sync.Mutex

	writable                 map[string]bool
	diskBelowLWM             map[string]bool
	hasLedgers               map[string]bool
	noUnderReplicatedLedgers bool

	setReadOnlyCalls []setReadOnlyCall
	triggerCalls     []string
	renameCalls      []string

	triggerErr           error
	renameErr            error
	setReadOnlyRevertErr error // returned only when SetReadOnly is called with readOnly=false
}

func newMockAdmin() *mockAdmin {
	return &mockAdmin{
		writable:                 map[string]bool{},
		diskBelowLWM:             map[string]bool{},
		hasLedgers:               map[string]bool{},
		noUnderReplicatedLedgers: true,
	}
}

func (m *mockAdmin) IsWritable(_ context.Context, podName string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.writable[podName], nil
}

func (m *mockAdmin) LedgerDiskUsageBelow(_ context.Context, podName string, _ float64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.diskBelowLWM[podName], nil
}

func (m *mockAdmin) SetReadOnly(_ context.Context, podName string, readOnly bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setReadOnlyCalls = append(m.setReadOnlyCalls, setReadOnlyCall{podName: podName, readOnly: readOnly})
	if !readOnly && m.setReadOnlyRevertErr != nil {
		return m.setReadOnlyRevertErr
	}
	m.writable[podName] = !readOnly
	return nil
}

func (m *mockAdmin) TriggerDecommission(_ context.Context, podName, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.triggerCalls = append(m.triggerCalls, podName)
	return m.triggerErr
}

func (m *mockAdmin) HasLedgers(_ context.Context, podName, _ string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hasLedgers[podName], nil
}

func (m *mockAdmin) NoUnderReplicatedLedgers(_ context.Context, _ string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.noUnderReplicatedLedgers, nil
}

func (m *mockAdmin) RenameCookie(_ context.Context, podName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.renameCalls = append(m.renameCalls, podName)
	return m.renameErr
}

var _ bkadmin.AdminClient = (*mockAdmin)(nil)

// --- test fixtures ---

const decommTestNamespace = "default"

func decommTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme(clientgoscheme): %v", err)
	}
	if err := clusterv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme(clusterv1alpha1): %v", err)
	}
	return scheme
}

// newDecommTestBookKeeper builds a BookKeeper with the guard fully enabled
// and readyReplicas/replicas already converged, so tests opt in to the
// guarded state machine without needing to also fabricate an unrelated
// rollout-in-progress scenario.
func newDecommTestBookKeeper(name string, replicas int32) *clusterv1alpha1.BookKeeper {
	trueVal := true
	return &clusterv1alpha1.BookKeeper{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: decommTestNamespace},
		Spec: clusterv1alpha1.BookKeeperSpec{
			Replicas: &replicas,
			Autoscaler: &clusterv1alpha1.BookKeeperAutoscalerSpec{
				Enabled:                    true,
				ScaleDownEnabled:           &trueVal,
				StabilizationWindowSeconds: int32Ptr(0),
			},
		},
		Status: clusterv1alpha1.BookKeeperStatus{
			Replicas:      replicas,
			ReadyReplicas: replicas,
		},
	}
}

func readyPodObjs(bk *clusterv1alpha1.BookKeeper, replicas int32, readySince time.Time) []client.Object {
	objs := make([]client.Object, 0, replicas)
	for ord := range replicas {
		objs = append(objs, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: bookiePodName(bk, ord), Namespace: bk.Namespace},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(readySince)},
				},
			},
		})
	}
	return objs
}

func pvcObjs(bk *clusterv1alpha1.BookKeeper, ordinal int32) []client.Object {
	podName := bookiePodName(bk, ordinal)
	objs := make([]client.Object, 0, 3)
	for _, vol := range []string{volumeNameJournal, volumeNameLedgers, volumeNameIndex} {
		objs = append(objs, &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-%s", vol, podName), Namespace: bk.Namespace},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			},
		})
	}
	return objs
}

func newDecommTestReconciler(t *testing.T, admin bkadmin.AdminClient, now time.Time, objs ...client.Object) (*BookKeeperDecommissionReconciler, client.Client) {
	t.Helper()
	scheme := decommTestScheme(t)
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&clusterv1alpha1.BookKeeper{}).
		WithObjects(objs...).
		Build()

	r := &BookKeeperDecommissionReconciler{
		Client:             cl,
		Scheme:             scheme,
		Recorder:           events.NewFakeRecorder(100),
		AdminClientFactory: func(*clusterv1alpha1.BookKeeper) bkadmin.AdminClient { return admin },
		Now:                func() time.Time { return now },
	}
	return r, cl
}

func reconcileOnce(t *testing.T, r *BookKeeperDecommissionReconciler, name string) reconcile.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: decommTestNamespace}})
	if err != nil {
		t.Fatalf("Reconcile() unexpected error: %v", err)
	}
	return res
}

func reconcileExpectErr(t *testing.T, r *BookKeeperDecommissionReconciler, name string) error {
	t.Helper()
	_, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: decommTestNamespace}})
	if err == nil {
		t.Fatalf("Reconcile() expected an error, got nil")
	}
	return err
}

func getBookKeeper(t *testing.T, cl client.Client, name string) *clusterv1alpha1.BookKeeper {
	t.Helper()
	bk := &clusterv1alpha1.BookKeeper{}
	if err := cl.Get(context.Background(), types.NamespacedName{Name: name, Namespace: decommTestNamespace}, bk); err != nil {
		t.Fatalf("Get(%s) failed: %v", name, err)
	}
	return bk
}

// readyPodsOnNodes builds one Ready bookie pod per ordinal, each pinned to a
// distinct node "node-<ord>", plus the matching cluster-scoped Node object
// carrying the given topology zone label. It backs the rack/zone placement
// guard tests, which need real pod->node->zone lookups.
func readyPodsOnNodes(bk *clusterv1alpha1.BookKeeper, zones []string, readySince time.Time) []client.Object {
	objs := make([]client.Object, 0, len(zones)*2)
	for ord, zone := range zones {
		nodeName := fmt.Sprintf("node-%d", ord)
		objs = append(objs, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: bookiePodName(bk, int32(ord)), Namespace: bk.Namespace},
			Spec:       corev1.PodSpec{NodeName: nodeName},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(readySince)},
				},
			},
		})
		objs = append(objs, &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: nodeName, Labels: map[string]string{labelTopologyZone: zone}},
		})
	}
	return objs
}

func decommissioningCond(bk *clusterv1alpha1.BookKeeper) *metav1.Condition {
	return findCondition(bk.Status.Conditions, clusterv1alpha1.BookKeeperConditionTypeDecommissioning)
}

// --- guard: quorum/capacity would break -> abort ---

func TestBookKeeperDecommission_QuorumWouldBreak_Aborts(t *testing.T) {
	const name = "bk-quorum"
	now := time.Now()

	bk := newDecommTestBookKeeper(name, 3)
	// ensembleSize=3, minWritableBookies defaults to ensembleSize=3: removing
	// bookie 2 would leave only 2 writable bookies, below the floor.
	bk.Annotations = map[string]string{clusterv1alpha1.AnnotationDrainBookieOrdinal: "2"}

	pods := readyPodObjs(bk, 3, now.Add(-time.Hour))
	objs := append([]client.Object{bk}, pods...)

	admin := newMockAdmin()
	admin.writable["bk-quorum-0"] = true
	admin.writable["bk-quorum-1"] = true
	admin.writable["bk-quorum-2"] = true

	r, cl := newDecommTestReconciler(t, admin, now, objs...)

	reconcileOnce(t, r, name) // starts (manual), phase=Verifying
	reconcileOnce(t, r, name) // processes Verifying -> aborts

	got := getBookKeeper(t, cl, name)
	if got.Status.Decommission != nil {
		t.Errorf("Status.Decommission = %+v, want nil after an aborted decommission", got.Status.Decommission)
	}
	cond := decommissioningCond(got)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != reasonDecommissionQuorumWouldBreak {
		t.Errorf("Decommissioning condition = %+v, want False/%s", cond, reasonDecommissionQuorumWouldBreak)
	}
	if len(admin.setReadOnlyCalls) != 0 {
		t.Errorf("SetReadOnly calls = %v, want none -- the bookie must never be touched when the pre-flight guard fails", admin.setReadOnlyCalls)
	}
	if *got.Spec.Replicas != 3 {
		t.Errorf("Spec.Replicas = %d, want unchanged 3", *got.Spec.Replicas)
	}
}

// --- guard: under-replication present -> no automatic start ---

func TestBookKeeperDecommission_EvaluateAutoTrigger_Guards(t *testing.T) {
	now := time.Now()

	baseAdmin := func() *mockAdmin {
		m := newMockAdmin()
		for _, pod := range []string{"bk-auto-0", "bk-auto-1", "bk-auto-2", "bk-auto-3"} {
			m.writable[pod] = true
			m.diskBelowLWM[pod] = true
		}
		m.noUnderReplicatedLedgers = true
		return m
	}

	tests := []struct {
		name   string
		mutate func(*mockAdmin)
		wantOK bool
	}{
		{"surplus, all below LWM, no under-replication: should start", func(*mockAdmin) {}, true},
		{"under-replication present: must not start", func(m *mockAdmin) { m.noUnderReplicatedLedgers = false }, false},
		{"a writable bookie at/above LWM: must not start", func(m *mockAdmin) { m.diskBelowLWM["bk-auto-1"] = false }, false},
		{"no surplus writable bookies: must not start", func(m *mockAdmin) { m.writable["bk-auto-3"] = false }, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const name = "bk-auto"
			bk := newDecommTestBookKeeper(name, 4)
			// ensembleSize defaults to 3, minWritableBookies defaults to
			// ensembleSize: 4 writable > floor 3 is the surplus this table
			// exercises against LWM/under-replication/deficit guards.
			pods := readyPodObjs(bk, 4, now.Add(-time.Hour))
			objs := append([]client.Object{bk}, pods...)

			admin := baseAdmin()
			tt.mutate(admin)

			r, _ := newDecommTestReconciler(t, admin, now, objs...)

			should, ordinal, err := r.evaluateAutoTrigger(context.Background(), bk, admin)
			if err != nil {
				t.Fatalf("evaluateAutoTrigger() error = %v", err)
			}
			if should != tt.wantOK {
				t.Errorf("evaluateAutoTrigger() should = %v, want %v", should, tt.wantOK)
			}
			if tt.wantOK && ordinal != 3 {
				t.Errorf("evaluateAutoTrigger() ordinal = %d, want 3 (highest)", ordinal)
			}
		})
	}
}

// --- regression: reaching the trigger decision never bare-decrements replicas ---

func TestBookKeeperDecommission_RegressionNaiveReplicaDecrementNotTaken(t *testing.T) {
	const name = "bk-regression"
	now := time.Now()

	bk := newDecommTestBookKeeper(name, 4)
	pods := readyPodObjs(bk, 4, now.Add(-time.Hour))
	objs := append([]client.Object{bk}, pods...)

	admin := newMockAdmin()
	for _, pod := range []string{"bk-regression-0", "bk-regression-1", "bk-regression-2", "bk-regression-3"} {
		admin.writable[pod] = true
		admin.diskBelowLWM[pod] = true
	}
	admin.noUnderReplicatedLedgers = true

	r, cl := newDecommTestReconciler(t, admin, now, objs...)

	reconcileOnce(t, r, name)

	got := getBookKeeper(t, cl, name)
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 4 {
		t.Fatalf("Spec.Replicas = %v immediately after triggering, want unchanged 4 -- "+
			"a scale-down decision must start the guarded state machine, never bare-decrement replicas", got.Spec.Replicas)
	}
	if got.Status.Decommission == nil || got.Status.Decommission.Phase != clusterv1alpha1.BookKeeperDecommissionPhaseVerifying {
		t.Fatalf("Status.Decommission = %+v, want phase Verifying started", got.Status.Decommission)
	}
	if len(admin.setReadOnlyCalls) != 0 {
		t.Errorf("SetReadOnly calls = %v, want none yet (still in Verifying)", admin.setReadOnlyCalls)
	}
}

// --- manual drain: wrong ordinal rejected ---

func TestBookKeeperDecommission_ManualDrain_WrongOrdinalRejected(t *testing.T) {
	const name = "bk-manual-wrong"
	now := time.Now()

	bk := newDecommTestBookKeeper(name, 3)
	bk.Annotations = map[string]string{clusterv1alpha1.AnnotationDrainBookieOrdinal: "0"} // highest is 2, not 0

	admin := newMockAdmin()
	r, cl := newDecommTestReconciler(t, admin, now, bk)

	reconcileOnce(t, r, name)

	got := getBookKeeper(t, cl, name)
	if got.Status.Decommission != nil {
		t.Errorf("Status.Decommission = %+v, want nil (request rejected)", got.Status.Decommission)
	}
	if _, ok := got.Annotations[clusterv1alpha1.AnnotationDrainBookieOrdinal]; ok {
		t.Errorf("annotation still present, want cleared after a rejected request")
	}
	if len(admin.setReadOnlyCalls) != 0 {
		t.Errorf("SetReadOnly calls = %v, want none", admin.setReadOnlyCalls)
	}
}

// --- happy path: full sequence, cookie renamed (not deleted), PVCs deleted by the operator ---

func TestBookKeeperDecommission_HappyPath_FullSequence(t *testing.T) {
	const (
		name           = "bk-happy"
		happyTargetPod = "bk-happy-2"
	)
	now := time.Now()

	bk := newDecommTestBookKeeper(name, 3)
	bk.Annotations = map[string]string{clusterv1alpha1.AnnotationDrainBookieOrdinal: "2"}
	// Use a smaller ensemble than the CRD default (3) so the pre-flight
	// capacity guard passes with only 2 writable bookies remaining after
	// bookie 2 is removed.
	two := int32(2)
	bk.Spec.Ensemble = &clusterv1alpha1.BookKeeperEnsembleSpec{EnsembleSize: &two, WriteQuorum: &two, AckQuorum: &two}

	pods := readyPodObjs(bk, 3, now.Add(-time.Hour))
	pvcs := pvcObjs(bk, 2)
	objs := append([]client.Object{bk}, pods...)
	objs = append(objs, pvcs...)

	admin := newMockAdmin()
	admin.writable["bk-happy-0"] = true
	admin.writable["bk-happy-1"] = true
	admin.writable[happyTargetPod] = true

	r, cl := newDecommTestReconciler(t, admin, now, objs...)

	// Phase 1: Verifying -> MarkingReadOnly (start + verify = 2 reconciles).
	reconcileOnce(t, r, name) // start
	reconcileOnce(t, r, name) // Verifying -> MarkingReadOnly

	got := getBookKeeper(t, cl, name)
	if got.Status.Decommission.Phase != clusterv1alpha1.BookKeeperDecommissionPhaseMarkingReadOnly {
		t.Fatalf("phase = %s, want MarkingReadOnly", got.Status.Decommission.Phase)
	}
	if *got.Spec.Replicas != 3 {
		t.Fatalf("Spec.Replicas = %d, want still 3 before scale-down phase", *got.Spec.Replicas)
	}

	// Phase 2: MarkingReadOnly -> TriggeringRecovery.
	reconcileOnce(t, r, name)
	got = getBookKeeper(t, cl, name)
	if got.Status.Decommission.Phase != clusterv1alpha1.BookKeeperDecommissionPhaseTriggeringRecovery {
		t.Fatalf("phase = %s, want TriggeringRecovery", got.Status.Decommission.Phase)
	}
	if len(admin.setReadOnlyCalls) != 1 || admin.setReadOnlyCalls[0] != (setReadOnlyCall{happyTargetPod, true}) {
		t.Fatalf("setReadOnlyCalls = %v, want exactly one readOnly=true call for bk-happy-2", admin.setReadOnlyCalls)
	}

	// Phase 3: TriggeringRecovery -> AwaitingReplication.
	reconcileOnce(t, r, name)
	got = getBookKeeper(t, cl, name)
	if got.Status.Decommission.Phase != clusterv1alpha1.BookKeeperDecommissionPhaseAwaitingReplication {
		t.Fatalf("phase = %s, want AwaitingReplication", got.Status.Decommission.Phase)
	}
	if len(admin.triggerCalls) != 1 || admin.triggerCalls[0] != happyTargetPod {
		t.Fatalf("triggerCalls = %v, want exactly one call for bk-happy-2", admin.triggerCalls)
	}

	// Phase 4: AwaitingReplication blocks while the target still has ledgers.
	admin.hasLedgers[happyTargetPod] = true
	res := reconcileOnce(t, r, name)
	if res.RequeueAfter <= 0 {
		t.Fatalf("expected a positive RequeueAfter while still awaiting replication")
	}
	got = getBookKeeper(t, cl, name)
	if got.Status.Decommission.Phase != clusterv1alpha1.BookKeeperDecommissionPhaseAwaitingReplication {
		t.Fatalf("phase = %s, want still AwaitingReplication while the bookie still has ledgers", got.Status.Decommission.Phase)
	}

	// Replication finishes: zero ledgers on the target, zero under-replication.
	admin.hasLedgers[happyTargetPod] = false
	admin.noUnderReplicatedLedgers = true
	reconcileOnce(t, r, name)
	got = getBookKeeper(t, cl, name)
	if got.Status.Decommission.Phase != clusterv1alpha1.BookKeeperDecommissionPhaseInvalidatingCookie {
		t.Fatalf("phase = %s, want InvalidatingCookie", got.Status.Decommission.Phase)
	}

	// Phase 5: InvalidatingCookie -> ScalingDown. Cookie is RENAMED, never deleted.
	reconcileOnce(t, r, name)
	got = getBookKeeper(t, cl, name)
	if got.Status.Decommission.Phase != clusterv1alpha1.BookKeeperDecommissionPhaseScalingDown {
		t.Fatalf("phase = %s, want ScalingDown", got.Status.Decommission.Phase)
	}
	if len(admin.renameCalls) != 1 || admin.renameCalls[0] != happyTargetPod {
		t.Fatalf("renameCalls = %v, want exactly one rename for bk-happy-2 (never a delete call -- there is no such method)", admin.renameCalls)
	}
	if *got.Spec.Replicas != 3 {
		t.Fatalf("Spec.Replicas = %d, want still 3 immediately before the ScalingDown phase runs", *got.Spec.Replicas)
	}

	// Phase 6: ScalingDown -> DeletingPVCs. Exactly one ordinal removed.
	reconcileOnce(t, r, name)
	got = getBookKeeper(t, cl, name)
	if got.Status.Decommission.Phase != clusterv1alpha1.BookKeeperDecommissionPhaseDeletingPVCs {
		t.Fatalf("phase = %s, want DeletingPVCs", got.Status.Decommission.Phase)
	}
	if *got.Spec.Replicas != 2 {
		t.Fatalf("Spec.Replicas = %d, want exactly 2 (one ordinal removed)", *got.Spec.Replicas)
	}

	// While the target pod still exists, phaseDeletePVCs must wait rather
	// than delete PVCs out from under a pod that might still have them
	// mounted.
	res = reconcileOnce(t, r, name)
	if res.RequeueAfter <= 0 {
		t.Fatalf("expected a positive RequeueAfter while the target pod is still terminating")
	}
	got = getBookKeeper(t, cl, name)
	if got.Status.Decommission.Phase != clusterv1alpha1.BookKeeperDecommissionPhaseDeletingPVCs {
		t.Fatalf("phase = %s, want still DeletingPVCs while the pod hasn't terminated", got.Status.Decommission.Phase)
	}

	// The StatefulSet controller finishes terminating the target ordinal's pod.
	targetPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: happyTargetPod, Namespace: decommTestNamespace}}
	if err := cl.Delete(context.Background(), targetPod); err != nil {
		t.Fatalf("deleting target pod fixture: %v", err)
	}

	reconcileOnce(t, r, name)
	got = getBookKeeper(t, cl, name)
	if got.Status.Decommission != nil {
		t.Fatalf("Status.Decommission = %+v, want nil (complete)", got.Status.Decommission)
	}
	cond := decommissioningCond(got)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != reasonDecommissionComplete {
		t.Fatalf("Decommissioning condition = %+v, want False/%s", cond, reasonDecommissionComplete)
	}
	if _, ok := got.Annotations[clusterv1alpha1.AnnotationDrainBookieOrdinal]; ok {
		t.Errorf("manual drain annotation still present after completion, want cleared")
	}

	for _, vol := range []string{volumeNameJournal, volumeNameLedgers, volumeNameIndex} {
		pvc := &corev1.PersistentVolumeClaim{}
		err := cl.Get(context.Background(), types.NamespacedName{Name: fmt.Sprintf("%s-bk-happy-2", vol), Namespace: decommTestNamespace}, pvc)
		if !apierrors.IsNotFound(err) {
			t.Errorf("PVC %s-bk-happy-2 still exists (err=%v), want deleted by the operator", vol, err)
		}
	}

	// Never reverted: readOnly=false must never have been called.
	for _, call := range admin.setReadOnlyCalls {
		if !call.readOnly {
			t.Errorf("unexpected SetReadOnly(readOnly=false) call in the happy path: %+v", call)
		}
	}
}

// --- timeout during the blocking replication wait: auto-revert to writable ---

func TestBookKeeperDecommission_Timeout_DuringAwaitingReplication_AutoReverts(t *testing.T) {
	const name = "bk-timeout"
	now := time.Now()
	started := metav1.NewTime(now.Add(-2 * time.Hour)) // well past the default 1800s timeout

	bk := newDecommTestBookKeeper(name, 3)
	ordinal := int32(2)
	bk.Status.Decommission = &clusterv1alpha1.BookKeeperDecommissionStatus{
		Phase:          clusterv1alpha1.BookKeeperDecommissionPhaseAwaitingReplication,
		TargetOrdinal:  &ordinal,
		TargetBookieID: bookieIDFor(bk, ordinal),
		StartedAt:      &started,
	}

	admin := newMockAdmin()
	admin.writable["bk-timeout-2"] = false  // currently read-only, mid-decommission
	admin.hasLedgers["bk-timeout-2"] = true // still stuck replicating

	r, cl := newDecommTestReconciler(t, admin, now, bk)

	reconcileOnce(t, r, name)

	got := getBookKeeper(t, cl, name)
	if got.Status.Decommission != nil {
		t.Fatalf("Status.Decommission = %+v, want nil after an auto-revert", got.Status.Decommission)
	}
	cond := decommissioningCond(got)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != reasonDecommissionTimedOut {
		t.Fatalf("Decommissioning condition = %+v, want False/%s", cond, reasonDecommissionTimedOut)
	}
	if len(admin.setReadOnlyCalls) != 1 || admin.setReadOnlyCalls[0] != (setReadOnlyCall{"bk-timeout-2", false}) {
		t.Fatalf("setReadOnlyCalls = %v, want exactly one readOnly=false revert call", admin.setReadOnlyCalls)
	}
	if !admin.writable["bk-timeout-2"] {
		t.Errorf("bookie bk-timeout-2 must be writable again after the revert")
	}
}

// --- timeout during MarkingReadOnly: reverts without ever triggering recovery ---

func TestBookKeeperDecommission_Timeout_DuringMarkingReadOnly_AutoReverts(t *testing.T) {
	const name = "bk-timeout-early"
	now := time.Now()
	started := metav1.NewTime(now.Add(-2 * time.Hour))

	bk := newDecommTestBookKeeper(name, 3)
	ordinal := int32(2)
	bk.Status.Decommission = &clusterv1alpha1.BookKeeperDecommissionStatus{
		Phase:         clusterv1alpha1.BookKeeperDecommissionPhaseMarkingReadOnly,
		TargetOrdinal: &ordinal,
		StartedAt:     &started,
	}

	admin := newMockAdmin()
	r, cl := newDecommTestReconciler(t, admin, now, bk)

	reconcileOnce(t, r, name)

	got := getBookKeeper(t, cl, name)
	if got.Status.Decommission != nil {
		t.Fatalf("Status.Decommission = %+v, want nil after an auto-revert", got.Status.Decommission)
	}
	if len(admin.triggerCalls) != 0 {
		t.Errorf("triggerCalls = %v, want none -- recovery must never be triggered on an already-timed-out decommission", admin.triggerCalls)
	}
	if len(admin.setReadOnlyCalls) != 1 || admin.setReadOnlyCalls[0].readOnly {
		t.Fatalf("setReadOnlyCalls = %v, want exactly one readOnly=false revert call", admin.setReadOnlyCalls)
	}
}

// --- resume: reconciling mid-phase continues, it does not restart ---

func TestBookKeeperDecommission_Resume_ContinuesFromPersistedPhase(t *testing.T) {
	const name = "bk-resume"
	now := time.Now()
	started := metav1.NewTime(now.Add(-time.Minute)) // recent: not timed out

	bk := newDecommTestBookKeeper(name, 3)
	ordinal := int32(2)
	bk.Status.Decommission = &clusterv1alpha1.BookKeeperDecommissionStatus{
		Phase:          clusterv1alpha1.BookKeeperDecommissionPhaseAwaitingReplication,
		TargetOrdinal:  &ordinal,
		TargetBookieID: bookieIDFor(bk, ordinal),
		StartedAt:      &started,
	}

	admin := newMockAdmin()
	admin.writable["bk-resume-2"] = false // already read-only from a "previous life"
	admin.hasLedgers["bk-resume-2"] = false
	admin.noUnderReplicatedLedgers = true

	r, cl := newDecommTestReconciler(t, admin, now, bk)

	reconcileOnce(t, r, name)

	got := getBookKeeper(t, cl, name)
	if got.Status.Decommission == nil || got.Status.Decommission.Phase != clusterv1alpha1.BookKeeperDecommissionPhaseInvalidatingCookie {
		t.Fatalf("phase = %+v, want advanced straight to InvalidatingCookie", got.Status.Decommission)
	}
	// The critical assertion: resuming at AwaitingReplication must not
	// replay MarkingReadOnly or TriggeringRecovery -- an operator-restart
	// resume, not a restart from phase 1.
	if len(admin.setReadOnlyCalls) != 0 {
		t.Errorf("setReadOnlyCalls = %v, want none replayed on resume", admin.setReadOnlyCalls)
	}
	if len(admin.triggerCalls) != 0 {
		t.Errorf("triggerCalls = %v, want none replayed on resume", admin.triggerCalls)
	}
}

// --- regression: ScalingDown must be idempotent across a resumed retry ---

// TestBookKeeperDecommission_ScaleDown_IdempotentAcrossResume guards against
// a specific bug caught in review: computing the ScalingDown phase's target
// replica count as "current spec.replicas - 1" is NOT idempotent, because a
// resumed retry of this same phase (e.g. the spec write landed but the
// phase-advance status write failed before the operator restarted) would see
// spec.replicas already decremented once and decrement it a second time. The
// target must instead be pinned to the persisted target ordinal.
func TestBookKeeperDecommission_ScaleDown_IdempotentAcrossResume(t *testing.T) {
	const name = "bk-scaledown-idempotent"
	now := time.Now()

	bk := newDecommTestBookKeeper(name, 3)
	ordinal := int32(2)
	bk.Status.Decommission = &clusterv1alpha1.BookKeeperDecommissionStatus{
		Phase:         clusterv1alpha1.BookKeeperDecommissionPhaseScalingDown,
		TargetOrdinal: &ordinal,
	}

	admin := newMockAdmin()
	r, cl := newDecommTestReconciler(t, admin, now, bk)

	reconcileOnce(t, r, name)

	got := getBookKeeper(t, cl, name)
	if *got.Spec.Replicas != 2 {
		t.Fatalf("Spec.Replicas = %d after the first ScalingDown pass, want 2", *got.Spec.Replicas)
	}
	if got.Status.Decommission.Phase != clusterv1alpha1.BookKeeperDecommissionPhaseDeletingPVCs {
		t.Fatalf("phase = %s, want DeletingPVCs", got.Status.Decommission.Phase)
	}

	// Simulate resuming this phase a second time (e.g. the phase-advance
	// status write was lost and an operator restart re-entered ScalingDown
	// with the spec change already applied).
	got.Status.Decommission.Phase = clusterv1alpha1.BookKeeperDecommissionPhaseScalingDown
	if err := cl.Status().Update(context.Background(), got); err != nil {
		t.Fatalf("resetting phase back to ScalingDown: %v", err)
	}

	reconcileOnce(t, r, name)

	got = getBookKeeper(t, cl, name)
	if *got.Spec.Replicas != 2 {
		t.Fatalf("Spec.Replicas = %d after a resumed retry of ScalingDown, want still 2 (not decremented again to 1)", *got.Spec.Replicas)
	}
}

// --- CRITICAL regression: concurrent scale-up during a decommission must
// ABORT ScalingDown, never shrink the StatefulSet past the target ordinal ---

// TestBookKeeperDecommission_ScaleDown_ConcurrentScaleUp_Aborts covers the
// highest-severity data-loss bug in this controller: the AwaitingReplication
// wait can run for up to decommissionTimeoutSeconds (~30 min), during which
// the cluster can scale UP (the disk-watermark autoscaler, or a human). If
// that happens, the recorded target ordinal is no longer the StatefulSet's
// top ordinal, so writing spec.replicas = ordinal would make the StatefulSet
// delete EVERY bookie between the target and the new top -- none of which
// were drained, re-replicated, or cookie-invalidated -> permanent data loss.
// ScalingDown must instead abort and leave the StatefulSet untouched.
func TestBookKeeperDecommission_ScaleDown_ConcurrentScaleUp_Aborts(t *testing.T) {
	const name = "bk-topology"
	now := time.Now()

	// This decommission began when the top ordinal was 2 (replicas were 3),
	// but replicas have since grown to 5 (bookies 0..4 now exist).
	bk := newDecommTestBookKeeper(name, 5)
	ordinal := int32(2)
	started := metav1.NewTime(now.Add(-time.Minute))
	bk.Status.Decommission = &clusterv1alpha1.BookKeeperDecommissionStatus{
		Phase:          clusterv1alpha1.BookKeeperDecommissionPhaseScalingDown,
		TargetOrdinal:  &ordinal,
		TargetBookieID: bookieIDFor(bk, ordinal),
		StartedAt:      &started,
	}

	admin := newMockAdmin()
	admin.writable["bk-topology-2"] = false // drained + read-only mid-decommission

	r, cl := newDecommTestReconciler(t, admin, now, bk)

	reconcileOnce(t, r, name)

	got := getBookKeeper(t, cl, name)

	// The load-bearing assertion: the StatefulSet's replica count is NOT
	// shrunk. Bookies 3 and 4 (never drained) are never deleted.
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 5 {
		t.Fatalf("Spec.Replicas = %v, want unchanged 5 -- a concurrent scale-up must abort the decommission, never shrink past the target", got.Spec.Replicas)
	}
	if got.Status.Decommission != nil {
		t.Fatalf("Status.Decommission = %+v, want nil after a topology-change abort", got.Status.Decommission)
	}
	cond := decommissioningCond(got)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != reasonDecommissionAbortedTopologyChanged {
		t.Fatalf("Decommissioning condition = %+v, want False/%s", cond, reasonDecommissionAbortedTopologyChanged)
	}
	// Best-effort revert of the already-drained target to writable.
	if len(admin.setReadOnlyCalls) != 1 || admin.setReadOnlyCalls[0] != (setReadOnlyCall{"bk-topology-2", false}) {
		t.Fatalf("setReadOnlyCalls = %v, want exactly one best-effort readOnly=false revert of the target", admin.setReadOnlyCalls)
	}
}

// TestBookKeeperDecommission_ScaleDown_ReplicasAlreadyBelowTarget_NoScaleUp
// covers the other side of the guard: if replicas are already at or below the
// target ordinal (the scale-down already landed, or another actor shrank
// further), ScalingDown must advance without ever scaling the StatefulSet
// back UP.
func TestBookKeeperDecommission_ScaleDown_ReplicasAlreadyBelowTarget_NoScaleUp(t *testing.T) {
	const name = "bk-belowtarget"
	now := time.Now()

	// target ordinal 2, but replicas are already 2 (target removed).
	bk := newDecommTestBookKeeper(name, 2)
	ordinal := int32(2)
	bk.Status.Decommission = &clusterv1alpha1.BookKeeperDecommissionStatus{
		Phase:         clusterv1alpha1.BookKeeperDecommissionPhaseScalingDown,
		TargetOrdinal: &ordinal,
	}

	admin := newMockAdmin()
	r, cl := newDecommTestReconciler(t, admin, now, bk)

	reconcileOnce(t, r, name)

	got := getBookKeeper(t, cl, name)
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 2 {
		t.Fatalf("Spec.Replicas = %v, want unchanged 2 -- never scale the StatefulSet back up", got.Spec.Replicas)
	}
	if got.Status.Decommission == nil || got.Status.Decommission.Phase != clusterv1alpha1.BookKeeperDecommissionPhaseDeletingPVCs {
		t.Fatalf("phase = %+v, want advanced to DeletingPVCs", got.Status.Decommission)
	}
}

// --- revert-on-failure path: a failed revert must NOT clear state ---

// TestBookKeeperDecommission_Timeout_RevertFails_KeepsStateAndRetries proves
// the safety-critical contract that if the auto-revert-to-writable itself
// fails, the controller surfaces the error and leaves the decommission state
// intact in the SAME phase, so the next reconcile retries the revert rather
// than abandoning a bookie stuck read-only.
func TestBookKeeperDecommission_Timeout_RevertFails_KeepsStateAndRetries(t *testing.T) {
	const name = "bk-revertfail"
	now := time.Now()
	started := metav1.NewTime(now.Add(-2 * time.Hour)) // past the default 1800s timeout

	bk := newDecommTestBookKeeper(name, 3)
	ordinal := int32(2)
	bk.Status.Decommission = &clusterv1alpha1.BookKeeperDecommissionStatus{
		Phase:          clusterv1alpha1.BookKeeperDecommissionPhaseAwaitingReplication,
		TargetOrdinal:  &ordinal,
		TargetBookieID: bookieIDFor(bk, ordinal),
		StartedAt:      &started,
	}

	admin := newMockAdmin()
	admin.writable["bk-revertfail-2"] = false
	admin.hasLedgers["bk-revertfail-2"] = true
	admin.setReadOnlyRevertErr = errors.New("bookie admin API unreachable")

	r, cl := newDecommTestReconciler(t, admin, now, bk)

	err := reconcileExpectErr(t, r, name)
	if !strings.Contains(err.Error(), "reverting bookie") {
		t.Fatalf("error = %v, want it to wrap the revert failure", err)
	}

	got := getBookKeeper(t, cl, name)
	if got.Status.Decommission == nil {
		t.Fatalf("Status.Decommission = nil, want state PRESERVED when the revert fails")
	}
	if got.Status.Decommission.Phase != clusterv1alpha1.BookKeeperDecommissionPhaseAwaitingReplication {
		t.Fatalf("phase = %s, want it unchanged at AwaitingReplication so the next reconcile retries the revert", got.Status.Decommission.Phase)
	}
	if len(admin.setReadOnlyCalls) != 1 || admin.setReadOnlyCalls[0] != (setReadOnlyCall{"bk-revertfail-2", false}) {
		t.Fatalf("setReadOnlyCalls = %v, want exactly one attempted readOnly=false revert", admin.setReadOnlyCalls)
	}

	// The revert now succeeds on the follow-up reconcile: state is cleared.
	admin.setReadOnlyRevertErr = nil
	reconcileOnce(t, r, name)

	got = getBookKeeper(t, cl, name)
	if got.Status.Decommission != nil {
		t.Fatalf("Status.Decommission = %+v, want nil once the retried revert succeeds", got.Status.Decommission)
	}
	if len(admin.setReadOnlyCalls) != 2 {
		t.Fatalf("setReadOnlyCalls = %v, want a second (successful) revert call", admin.setReadOnlyCalls)
	}
	if !admin.writable["bk-revertfail-2"] {
		t.Errorf("bookie must be writable again after the retried revert succeeds")
	}
}

// --- placement (rack/zone) guard at the controller level ---

func TestBookKeeperDecommission_PlacementGuard(t *testing.T) {
	// ensembleSize=2 so removing one of three bookies still satisfies the
	// capacity guard (2 writable remain), leaving zone placement as the sole
	// deciding factor. Target is ordinal 2.
	two := int32(2)
	const zoneA, zoneB, zoneC = "zone-a", "zone-b", "zone-c"

	tests := []struct {
		name      string
		zones     []string // zone label per ordinal 0,1,2 (2 is the target)
		wantAbort bool
	}{
		{"remaining bookies collapse to a single zone -> abort", []string{zoneA, zoneA, zoneB}, true},
		{"remaining bookies span two zones -> proceed", []string{zoneA, zoneB, zoneC}, false},
		// If the target's own zone were (wrongly) counted, {a,b} would be two
		// zones and this would proceed; it must abort, proving the target's
		// zone is excluded from the remaining-zone count.
		{"target's own zone excluded from the count -> abort", []string{zoneA, zoneA, zoneB}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const name = "bk-placement"
			now := time.Now()

			bk := newDecommTestBookKeeper(name, 3)
			bk.Spec.Ensemble = &clusterv1alpha1.BookKeeperEnsembleSpec{EnsembleSize: &two, WriteQuorum: &two, AckQuorum: &two}
			ordinal := int32(2)
			bk.Status.Decommission = &clusterv1alpha1.BookKeeperDecommissionStatus{
				Phase:          clusterv1alpha1.BookKeeperDecommissionPhaseVerifying,
				TargetOrdinal:  &ordinal,
				TargetBookieID: bookieIDFor(bk, ordinal),
				StartedAt:      &metav1.Time{Time: now.Add(-time.Minute)},
			}

			objs := append([]client.Object{bk}, readyPodsOnNodes(bk, tt.zones, now.Add(-time.Hour))...)

			admin := newMockAdmin()
			admin.writable["bk-placement-0"] = true
			admin.writable["bk-placement-1"] = true
			admin.writable["bk-placement-2"] = true

			r, cl := newDecommTestReconciler(t, admin, now, objs...)

			reconcileOnce(t, r, name)

			got := getBookKeeper(t, cl, name)
			if tt.wantAbort {
				if got.Status.Decommission != nil {
					t.Fatalf("Status.Decommission = %+v, want nil (placement guard aborted)", got.Status.Decommission)
				}
				cond := decommissioningCond(got)
				if cond == nil || cond.Reason != reasonDecommissionPlacementUnsatisfiable {
					t.Fatalf("condition = %+v, want reason %s", cond, reasonDecommissionPlacementUnsatisfiable)
				}
				if len(admin.setReadOnlyCalls) != 0 {
					t.Errorf("setReadOnlyCalls = %v, want none -- a pre-flight guard failure must not touch any bookie", admin.setReadOnlyCalls)
				}
			} else {
				if got.Status.Decommission == nil || got.Status.Decommission.Phase != clusterv1alpha1.BookKeeperDecommissionPhaseMarkingReadOnly {
					t.Fatalf("phase = %+v, want advanced to MarkingReadOnly (placement satisfied)", got.Status.Decommission)
				}
			}
		})
	}
}

// --- failure contracts for TriggeringRecovery and InvalidatingCookie ---

// TestBookKeeperDecommission_TriggerRecovery_ErrorRetries proves a failed
// decommissionbookie/recover trigger surfaces the error and stays in
// TriggeringRecovery (retried by the controller's normal backoff), without
// reverting.
func TestBookKeeperDecommission_TriggerRecovery_ErrorRetries(t *testing.T) {
	const name = "bk-triggererr"
	now := time.Now()

	bk := newDecommTestBookKeeper(name, 3)
	ordinal := int32(2)
	bk.Status.Decommission = &clusterv1alpha1.BookKeeperDecommissionStatus{
		Phase:          clusterv1alpha1.BookKeeperDecommissionPhaseTriggeringRecovery,
		TargetOrdinal:  &ordinal,
		TargetBookieID: bookieIDFor(bk, ordinal),
		StartedAt:      &metav1.Time{Time: now.Add(-time.Minute)}, // not timed out
	}

	admin := newMockAdmin()
	admin.writable["bk-triggererr-2"] = false
	admin.triggerErr = errors.New("decommissionbookie and recover both failed")

	r, cl := newDecommTestReconciler(t, admin, now, bk)

	err := reconcileExpectErr(t, r, name)
	if !strings.Contains(err.Error(), "triggering decommission") {
		t.Fatalf("error = %v, want it to wrap the trigger failure", err)
	}

	got := getBookKeeper(t, cl, name)
	if got.Status.Decommission == nil || got.Status.Decommission.Phase != clusterv1alpha1.BookKeeperDecommissionPhaseTriggeringRecovery {
		t.Fatalf("phase = %+v, want unchanged at TriggeringRecovery", got.Status.Decommission)
	}
	if len(admin.triggerCalls) != 1 {
		t.Errorf("triggerCalls = %v, want exactly one attempt", admin.triggerCalls)
	}
	if len(admin.setReadOnlyCalls) != 0 {
		t.Errorf("setReadOnlyCalls = %v, want none -- a trigger failure is retried, not reverted", admin.setReadOnlyCalls)
	}
}

// TestBookKeeperDecommission_InvalidateCookie_ErrorRetriesNeverReverts proves
// the critical post-cookie-rename contract: once the cookie has been
// invalidated, a subsequent failure is retried indefinitely and NEVER
// triggers a revert to writable (which would be unsafe -- the bookie's
// on-disk identity is already gone).
func TestBookKeeperDecommission_InvalidateCookie_ErrorRetriesNeverReverts(t *testing.T) {
	const name = "bk-renameerr"
	now := time.Now()

	bk := newDecommTestBookKeeper(name, 3)
	ordinal := int32(2)
	// Deliberately far past the timeout, to prove that even a timed-out
	// decommission is NOT reverted once past cookie invalidation.
	bk.Status.Decommission = &clusterv1alpha1.BookKeeperDecommissionStatus{
		Phase:          clusterv1alpha1.BookKeeperDecommissionPhaseInvalidatingCookie,
		TargetOrdinal:  &ordinal,
		TargetBookieID: bookieIDFor(bk, ordinal),
		StartedAt:      &metav1.Time{Time: now.Add(-3 * time.Hour)},
	}

	admin := newMockAdmin()
	admin.writable["bk-renameerr-2"] = false
	admin.renameErr = errors.New("pod exec failed renaming cookie")

	r, cl := newDecommTestReconciler(t, admin, now, bk)

	err := reconcileExpectErr(t, r, name)
	if !strings.Contains(err.Error(), "invalidating cookie") {
		t.Fatalf("error = %v, want it to wrap the cookie-rename failure", err)
	}

	got := getBookKeeper(t, cl, name)
	if got.Status.Decommission == nil || got.Status.Decommission.Phase != clusterv1alpha1.BookKeeperDecommissionPhaseInvalidatingCookie {
		t.Fatalf("phase = %+v, want unchanged at InvalidatingCookie", got.Status.Decommission)
	}
	if len(admin.renameCalls) != 1 {
		t.Errorf("renameCalls = %v, want exactly one attempt", admin.renameCalls)
	}
	if len(admin.setReadOnlyCalls) != 0 {
		t.Fatalf("setReadOnlyCalls = %v, want NONE -- failures after the cookie rename must never revert to writable", admin.setReadOnlyCalls)
	}
}

// int32Ptr is defined in bookkeeper_build_test.go (same package).
