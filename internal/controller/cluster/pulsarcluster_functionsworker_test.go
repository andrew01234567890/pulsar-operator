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
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
)

// testPackageStorageS3/testPackageStorageGCS are shared across this file and
// functionsworker_controller_test.go: both exercise the non-FileSystem
// packageStorage values, and reusing one constant each keeps goconst happy
// rather than scattering the same literal.
const (
	testPackageStorageS3  = "S3PackagesStorage"
	testPackageStorageGCS = "GCSPackagesStorage"
)

func TestIsFileSystemPackageStorage(t *testing.T) {
	tests := []struct {
		name           string
		packageStorage string
		want           bool
	}{
		{name: "unset defaults to filesystem", packageStorage: "", want: true},
		{name: "explicit FileSystemPackagesStorage", packageStorage: packageStorageFileSystem, want: true},
		{name: "S3PackagesStorage is not filesystem", packageStorage: testPackageStorageS3, want: false},
		{name: "GCSPackagesStorage is not filesystem", packageStorage: "GCSPackagesStorage", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isFileSystemPackageStorage(tt.packageStorage); got != tt.want {
				t.Errorf("isFileSystemPackageStorage(%q) = %v, want %v", tt.packageStorage, got, tt.want)
			}
		})
	}
}

// TestWithFunctionsWorkerColocatedBrokerDefaults pins the exact broker.conf
// keys Fix A wires in, verified against Pulsar's ServiceConfiguration
// (functionsWorkerEnabled, enablePackagesManagement,
// functionsWorkerEnablePackageManagement, packagesManagementStorageProvider)
// and FileSystemPackagesStorage's STORAGE_PATH generic-properties key - see
// /tmp/pulsar-refs/pulsar's ServiceConfiguration.java and
// FileSystemPackagesStorage.java.
func TestWithFunctionsWorkerColocatedBrokerDefaults(t *testing.T) {
	t.Run("FileSystemPackagesStorage (default)", func(t *testing.T) {
		got := withFunctionsWorkerColocatedBrokerDefaults(nil, clusterv1alpha1.FunctionsWorkerSpec{})
		want := map[string]string{
			confKeyFunctionsWorkerEnabled:            configValTrue,
			confKeyEnablePackagesManagement:          configValTrue,
			"functionsWorkerEnablePackageManagement": configValTrue,
			"packagesManagementStorageProvider":      fileSystemPackagesStorageProviderClass,
			confKeyStoragePath:                       functionsWorkerPackageStorageMountPath,
		}
		for k, v := range want {
			if got[k] != v {
				t.Errorf("%s = %q, want %q", k, got[k], v)
			}
		}
	})

	t.Run("S3PackagesStorage leaves the storage path to the user", func(t *testing.T) {
		got := withFunctionsWorkerColocatedBrokerDefaults(nil, clusterv1alpha1.FunctionsWorkerSpec{PackageStorage: testPackageStorageS3})
		if got[confKeyFunctionsWorkerEnabled] != configValTrue {
			t.Errorf("functionsWorkerEnabled = %q, want true", got[confKeyFunctionsWorkerEnabled])
		}
		if got[confKeyEnablePackagesManagement] != configValTrue {
			t.Errorf("enablePackagesManagement = %q, want true", got[confKeyEnablePackagesManagement])
		}
		if _, ok := got[confKeyStoragePath]; ok {
			t.Errorf("STORAGE_PATH = %q, want absent for a non-filesystem provider", got[confKeyStoragePath])
		}
		if _, ok := got["packagesManagementStorageProvider"]; ok {
			t.Errorf("packagesManagementStorageProvider = %q, want absent (left to spec.config)", got["packagesManagementStorageProvider"])
		}
	})

	t.Run("user overrides win", func(t *testing.T) {
		cfg := map[string]string{confKeyFunctionsWorkerEnabled: testConfigValFalse}
		got := withFunctionsWorkerColocatedBrokerDefaults(cfg, clusterv1alpha1.FunctionsWorkerSpec{})
		if got[confKeyFunctionsWorkerEnabled] != testConfigValFalse {
			t.Errorf("functionsWorkerEnabled = %q, want user override %q preserved", got[confKeyFunctionsWorkerEnabled], testConfigValFalse)
		}
	})
}

func TestFunctionsWorkerPackageStoragePVCName(t *testing.T) {
	if got, want := functionsWorkerPackageStoragePVCName("fw-cluster"), "fw-cluster-functions-package-storage"; got != want {
		t.Errorf("functionsWorkerPackageStoragePVCName(%q) = %q, want %q", "fw-cluster", got, want)
	}
}

func TestFunctionsWorkerPackageStorageVolumeAndMount(t *testing.T) {
	vol := functionsWorkerPackageStorageVolume("my-pvc")
	if vol.Name != functionsWorkerPackageStorageVolumeName {
		t.Errorf("volume name = %q, want %q", vol.Name, functionsWorkerPackageStorageVolumeName)
	}
	if vol.PersistentVolumeClaim == nil || vol.PersistentVolumeClaim.ClaimName != "my-pvc" {
		t.Errorf("volume PVC source = %+v, want claimName %q", vol.PersistentVolumeClaim, "my-pvc")
	}

	mount := functionsWorkerPackageStorageVolumeMount()
	if mount.Name != functionsWorkerPackageStorageVolumeName {
		t.Errorf("mount name = %q, want %q", mount.Name, functionsWorkerPackageStorageVolumeName)
	}
	if mount.MountPath != functionsWorkerPackageStorageMountPath {
		t.Errorf("mount path = %q, want %q", mount.MountPath, functionsWorkerPackageStorageMountPath)
	}
}

// TestWireFunctionsWorkerColocatedNoOp proves wireFunctionsWorkerColocated
// does nothing at all - to broker.Config, FunctionsWorkerConfig, Volumes, or
// VolumeMounts - when FunctionsWorker isn't configured or is in standalone
// mode, so a Broker child is never silently modified for a cluster that
// never asked for colocated functions.
func TestWireFunctionsWorkerColocatedNoOp(t *testing.T) {
	r := &PulsarClusterReconciler{}

	for _, tt := range []struct {
		name string
		fw   *clusterv1alpha1.FunctionsWorkerSpec
	}{
		{name: "no FunctionsWorker configured", fw: nil},
		{name: "standalone mode", fw: &clusterv1alpha1.FunctionsWorkerSpec{Mode: functionsWorkerModeStandalone}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cluster := &clusterv1alpha1.PulsarCluster{
				Spec: clusterv1alpha1.PulsarClusterSpec{FunctionsWorker: tt.fw},
			}
			broker := &clusterv1alpha1.BrokerSpec{}

			if err := r.wireFunctionsWorkerColocated(context.Background(), cluster, broker); err != nil {
				t.Fatalf("wireFunctionsWorkerColocated returned an error: %v", err)
			}

			if len(broker.Config) != 0 {
				t.Errorf("broker.Config = %v, want untouched", broker.Config)
			}
			if broker.FunctionsWorkerConfig != nil {
				t.Errorf("broker.FunctionsWorkerConfig = %v, want nil", broker.FunctionsWorkerConfig)
			}
			if len(broker.Volumes) != 0 || len(broker.VolumeMounts) != 0 {
				t.Errorf("broker.Volumes/VolumeMounts = %v/%v, want untouched", broker.Volumes, broker.VolumeMounts)
			}
		})
	}
}

// TestBrokerMaxReplicas covers the autoscaler-aware max used to decide
// whether the default ReadWriteOnce package-storage PVC is a real
// single-writer hazard: an autoscaled broker starts at 1 and scales up, so
// the max bound - not the current replica count - is what matters.
func TestBrokerMaxReplicas(t *testing.T) {
	autoscaler := func(enabled bool, max *int32) *clusterv1alpha1.BrokerAutoscalerSpec {
		return &clusterv1alpha1.BrokerAutoscalerSpec{Enabled: enabled, Max: max}
	}
	tests := []struct {
		name string
		spec clusterv1alpha1.BrokerSpec
		want int32
	}{
		{name: "nil replicas defaults to 3", spec: clusterv1alpha1.BrokerSpec{}, want: defaultBrokerReplicas},
		{name: "static single broker", spec: clusterv1alpha1.BrokerSpec{Replicas: ptr(int32(1))}, want: 1},
		{name: "static multi broker", spec: clusterv1alpha1.BrokerSpec{Replicas: ptr(int32(4))}, want: 4},
		{
			name: "autoscaler enabled starting at 1 but max 5 -> 5",
			spec: clusterv1alpha1.BrokerSpec{Replicas: ptr(int32(1)), Autoscaler: autoscaler(true, ptr(int32(5)))},
			want: 5,
		},
		{
			name: "autoscaler disabled ignores max",
			spec: clusterv1alpha1.BrokerSpec{Replicas: ptr(int32(1)), Autoscaler: autoscaler(false, ptr(int32(5)))},
			want: 1,
		},
		{
			name: "autoscaler enabled but max below current keeps current",
			spec: clusterv1alpha1.BrokerSpec{Replicas: ptr(int32(3)), Autoscaler: autoscaler(true, ptr(int32(2)))},
			want: 3,
		},
		{
			name: "autoscaler enabled with nil max falls back to current",
			spec: clusterv1alpha1.BrokerSpec{Replicas: ptr(int32(2)), Autoscaler: autoscaler(true, nil)},
			want: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := tt.spec
			if got := brokerMaxReplicas(&spec); got != tt.want {
				t.Errorf("brokerMaxReplicas() = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestEnqueueFunctionsWorkerForBroker covers the cross-CR watch mapping that
// keeps colocated readiness fresh: a Broker readiness change must enqueue its
// sibling FunctionsWorker (same owning PulsarCluster, deterministic child
// name), and nothing else.
func TestEnqueueFunctionsWorkerForBroker(t *testing.T) {
	const (
		brokerName = "cl-broker"
		ns         = "ns"
	)
	broker := func(refs []metav1.OwnerReference) *clusterv1alpha1.Broker {
		return &clusterv1alpha1.Broker{
			ObjectMeta: metav1.ObjectMeta{Name: brokerName, Namespace: ns, OwnerReferences: refs},
		}
	}
	ownerRef := func(kind, name string) []metav1.OwnerReference {
		return []metav1.OwnerReference{{
			APIVersion: clusterv1alpha1.GroupVersion.String(),
			Kind:       kind,
			Name:       name,
			UID:        types.UID("uid"),
			Controller: ptr(true),
		}}
	}

	t.Run("non-Broker object maps to nothing", func(t *testing.T) {
		got := enqueueFunctionsWorkerForBroker(context.Background(), &appsv1.StatefulSet{})
		if len(got) != 0 {
			t.Errorf("got %v, want no requests for a non-Broker object", got)
		}
	})

	t.Run("Broker with no controller owner maps to nothing", func(t *testing.T) {
		got := enqueueFunctionsWorkerForBroker(context.Background(), broker(nil))
		if len(got) != 0 {
			t.Errorf("got %v, want no requests for an unowned Broker", got)
		}
	})

	t.Run("Broker owned by a non-PulsarCluster maps to nothing", func(t *testing.T) {
		got := enqueueFunctionsWorkerForBroker(context.Background(), broker(ownerRef("Deployment", "cl")))
		if len(got) != 0 {
			t.Errorf("got %v, want no requests for a non-PulsarCluster owner", got)
		}
	})

	t.Run("Broker owned by a PulsarCluster maps to its sibling FunctionsWorker", func(t *testing.T) {
		got := enqueueFunctionsWorkerForBroker(context.Background(), broker(ownerRef(pulsarClusterOwnerKind, "cl")))
		want := types.NamespacedName{Name: "cl-functionsworker", Namespace: ns}
		if len(got) != 1 || got[0].NamespacedName != want {
			t.Errorf("got %v, want a single request for %v", got, want)
		}
	})
}
