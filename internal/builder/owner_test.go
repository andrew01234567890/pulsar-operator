package builder

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestSetControllerOwner(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	owner := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testNameBroker,
			Namespace: testNamespace,
			UID:       "owner-uid",
		},
	}
	controlled := ConfigMap("pulsar-broker-config", testNamespace, nil, nil)

	if err := SetControllerOwner(owner, controlled, scheme); err != nil {
		t.Fatalf("SetControllerOwner: %v", err)
	}

	refs := controlled.GetOwnerReferences()
	if len(refs) != 1 {
		t.Fatalf("got %d owner references, want 1: %v", len(refs), refs)
	}

	ref := refs[0]
	if ref.Name != owner.Name {
		t.Errorf("owner ref Name = %q, want %q", ref.Name, owner.Name)
	}
	if ref.UID != owner.UID {
		t.Errorf("owner ref UID = %q, want %q", ref.UID, owner.UID)
	}
	if ref.Kind != "Service" {
		t.Errorf("owner ref Kind = %q, want %q", ref.Kind, "Service")
	}
	if ref.Controller == nil || !*ref.Controller {
		t.Errorf("owner ref Controller = %v, want pointer to true", ref.Controller)
	}
	if ref.BlockOwnerDeletion == nil || !*ref.BlockOwnerDeletion {
		t.Errorf("owner ref BlockOwnerDeletion = %v, want pointer to true", ref.BlockOwnerDeletion)
	}
}

func TestSetControllerOwnerRejectsSecondController(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	firstOwner := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "first", Namespace: testNamespace, UID: "first-uid"}}
	secondOwner := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "second", Namespace: testNamespace, UID: "second-uid"}}
	controlled := ConfigMap("pulsar-config", testNamespace, nil, nil)

	if err := SetControllerOwner(firstOwner, controlled, scheme); err != nil {
		t.Fatalf("first SetControllerOwner: %v", err)
	}

	if err := SetControllerOwner(secondOwner, controlled, scheme); err == nil {
		t.Error("second SetControllerOwner with a different owner should error, got nil")
	}
}
