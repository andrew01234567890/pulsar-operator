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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// BookKeeperVolumes configures the three disk-role volumes bookies use.
type BookKeeperVolumes struct {
	// journal is the write-ahead-log volume.
	// +optional
	Journal *VolumeSpec `json:"journal,omitempty"`

	// ledgers is the ledger-storage volume.
	// +optional
	Ledgers *VolumeSpec `json:"ledgers,omitempty"`

	// index is the ledger-index volume.
	// +optional
	Index *VolumeSpec `json:"index,omitempty"`
}

// BookKeeperEnsembleSpec configures the default ledger ensemble/quorum sizes.
//
// The operator rejects an ensembleSize greater than the bookie replica count.
// For 3-AZ deployments, writeQuorum=3/ackQuorum=2 is recommended over the
// Pulsar-production default of 2/2 so the cluster survives one AZ loss.
// +kubebuilder:validation:XValidation:rule="self.ackQuorum <= self.writeQuorum && self.writeQuorum <= self.ensembleSize",message="ackQuorum <= writeQuorum <= ensembleSize"
type BookKeeperEnsembleSpec struct {
	// ensembleSize is the number of bookies each ledger is striped across.
	// +optional
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	EnsembleSize *int32 `json:"ensembleSize,omitempty"`

	// writeQuorum is the number of bookies each entry is written to.
	// +optional
	// +kubebuilder:default=2
	// +kubebuilder:validation:Minimum=1
	WriteQuorum *int32 `json:"writeQuorum,omitempty"`

	// ackQuorum is the number of acks required before an entry is confirmed.
	// +optional
	// +kubebuilder:default=2
	// +kubebuilder:validation:Minimum=1
	AckQuorum *int32 `json:"ackQuorum,omitempty"`
}

// BookKeeperAutoscalerSpec configures the bookie disk-watermark autoscaler.
//
// Per tick, priority order is: (1) scale up to cover a writable-bookie deficit,
// (2) pre-emptive scale up if any writable bookie is at or above
// diskUsageToleranceHwm, (3) scale down only if every writable bookie is below
// diskUsageToleranceLwm and there is zero cluster-wide under-replication.
// Scale-down is a guarded, opt-in, serialized decommission workflow and is
// OFF by default.
// +kubebuilder:validation:XValidation:rule="self.diskUsageToleranceLwm < self.diskUsageToleranceHwm",message="Lwm must be below Hwm"
type BookKeeperAutoscalerSpec struct {
	// enabled turns on the bookie disk-watermark autoscaler (scale-up).
	// +optional
	// +kubebuilder:default=false
	Enabled bool `json:"enabled,omitempty"`

	// minWritableBookies is the minimum number of writable bookies to maintain.
	// Must be >= the largest configured ensembleSize.
	// +optional
	// +kubebuilder:validation:Minimum=1
	MinWritableBookies *int32 `json:"minWritableBookies,omitempty"`

	// scaleUpBy is the number of bookies to add on each scale-up.
	// +optional
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	ScaleUpBy *int32 `json:"scaleUpBy,omitempty"`

	// scaleDownBy is the number of bookies to remove on each scale-down.
	// +optional
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	ScaleDownBy *int32 `json:"scaleDownBy,omitempty"`

	// scaleUpMaxLimit is the maximum number of bookies. The autoscaler never
	// scales above this.
	// +optional
	// +kubebuilder:validation:Minimum=1
	ScaleUpMaxLimit *int32 `json:"scaleUpMaxLimit,omitempty"`

	// diskUsageToleranceHwm is the high watermark; a writable bookie at or above
	// this disk usage triggers a pre-emptive scale-up. Expressed as a
	// whole-number percent in the range 0-100 (e.g. 92 means 92% disk used);
	// autoscaler controllers divide by 100.
	// +optional
	// +kubebuilder:default=92
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	DiskUsageToleranceHwm *int32 `json:"diskUsageToleranceHwm,omitempty"`

	// diskUsageToleranceLwm is the low watermark; every writable bookie must be
	// below this, with zero under-replicated ledgers cluster-wide, before a
	// scale-down is considered. Expressed as a whole-number percent in the range
	// 0-100 (e.g. 75 means 75% disk used); autoscaler controllers divide by 100.
	// +optional
	// +kubebuilder:default=75
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	DiskUsageToleranceLwm *int32 `json:"diskUsageToleranceLwm,omitempty"`

	// stabilizationWindowSeconds gates scaling decisions until all bookie pods
	// have been continuously Ready for this long, preventing flapping.
	// +optional
	// +kubebuilder:default=300
	// +kubebuilder:validation:Minimum=0
	StabilizationWindowSeconds *int32 `json:"stabilizationWindowSeconds,omitempty"`

	// scaleDownEnabled gates the guarded bookie decommission state machine.
	// Kept OFF by default; enable only after validating the safe-decommission
	// workflow against your metadata store.
	// +optional
	// +kubebuilder:default=false
	ScaleDownEnabled *bool `json:"scaleDownEnabled,omitempty"`

	// periodSeconds is the interval between autoscaler evaluations.
	// +optional
	// +kubebuilder:default=10
	// +kubebuilder:validation:Minimum=1
	PeriodSeconds *int32 `json:"periodSeconds,omitempty"`

	// decommissionTimeoutSeconds bounds how long the guarded scale-down state
	// machine waits, once a bookie is marked read-only, for re-replication to
	// finish (zero ledgers on the target bookie and zero cluster-wide
	// under-replicated ledgers) before auto-reverting the bookie to writable,
	// clearing the decommission state, and raising a Warning condition.
	// +optional
	// +kubebuilder:default=1800
	// +kubebuilder:validation:Minimum=60
	DecommissionTimeoutSeconds *int32 `json:"decommissionTimeoutSeconds,omitempty"`
}

// BookKeeperAutoRackConfig configures the rack-awareness sync daemon, which
// writes each bookie's node zone into metadata as its BookKeeper rack.
type BookKeeperAutoRackConfig struct {
	// enabled turns on the rack-awareness sync daemon.
	// +optional
	// +kubebuilder:default=false
	Enabled bool `json:"enabled,omitempty"`

	// periodSeconds is the interval between rack-metadata sync passes.
	// +optional
	// +kubebuilder:default=60
	// +kubebuilder:validation:Minimum=1
	PeriodSeconds *int32 `json:"periodSeconds,omitempty"`
}

// BookKeeperSpec defines the desired state of BookKeeper.
// +kubebuilder:validation:XValidation:rule="!has(self.ensemble) || !has(self.ensemble.ensembleSize) || !has(self.replicas) || self.ensemble.ensembleSize <= self.replicas",message="ensembleSize cannot exceed replicas"
type BookKeeperSpec struct {
	// replicas is the number of bookie pods.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Replicas *int32 `json:"replicas,omitempty"`

	// image overrides the bookie container image. Falls back to
	// PulsarCluster.spec.image when unset.
	// +optional
	Image string `json:"image,omitempty"`

	// config sets bookkeeper.conf key/value overrides layered on top of operator
	// defaults. Structural/wiring keys the operator owns —
	// journalDirectories, ledgerDirectories, indexDirectories, bookiePort,
	// httpServerEnabled, and httpServerPort — are re-asserted by the operator to
	// keep the rendered config in sync with the generated Service, probes, and
	// volume mounts, so overrides of those specific keys are ignored. Every
	// other key is applied as given.
	// +optional
	Config map[string]string `json:"config,omitempty"`

	// volumes configures the journal/ledgers/index disk-role volumes.
	// +optional
	Volumes *BookKeeperVolumes `json:"volumes,omitempty"`

	// ensemble configures the default ledger ensemble/write/ack quorum sizes.
	// +optional
	Ensemble *BookKeeperEnsembleSpec `json:"ensemble,omitempty"`

	// autoscaler configures the disk-watermark-driven autoscaler.
	// +optional
	Autoscaler *BookKeeperAutoscalerSpec `json:"autoscaler,omitempty"`

	// podManagementPolicy is the StatefulSet pod management policy. Immutable
	// once set.
	// +optional
	// +kubebuilder:default=Parallel
	// +kubebuilder:validation:Enum=OrderedReady;Parallel
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="podManagementPolicy is immutable"
	PodManagementPolicy string `json:"podManagementPolicy,omitempty"`

	// autoRackConfig configures the bookie rack-awareness sync daemon.
	// +optional
	AutoRackConfig *BookKeeperAutoRackConfig `json:"autoRackConfig,omitempty"`
}

// BookKeeperStatus defines the observed state of BookKeeper.
type BookKeeperStatus struct {
	// replicas is the observed number of bookie pods.
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// readyReplicas is the observed number of Ready bookie pods.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// writableBookies is the observed number of writable (non-read-only) bookies,
	// as last polled by the disk-watermark autoscaler.
	// +optional
	WritableBookies int32 `json:"writableBookies,omitempty"`

	// lastScaleTime is when the disk-watermark autoscaler last changed
	// spec.replicas. The autoscaler's stabilization window is measured from
	// this timestamp, not from pod age, so it gates repeated scaling events
	// directly rather than as a side effect of pod restarts.
	// +optional
	LastScaleTime *metav1.Time `json:"lastScaleTime,omitempty"`

	// decommission tracks the guarded, resumable bookie scale-down state
	// machine's progress when one is in flight (triggered automatically or via
	// AnnotationDrainBookieOrdinal). Persisting this on status, rather than
	// holding it only in memory, is what lets the state machine resume after
	// an operator restart instead of starting the decommission over. Absent
	// when no decommission is running.
	// +optional
	Decommission *BookKeeperDecommissionStatus `json:"decommission,omitempty"`

	// bookieRacks is the last-applied bookie-id -> rack mapping written by
	// the rack-awareness sync controller (gated on spec.autoRackConfig).
	// It is the operator's own cache for diffing the desired mapping against
	// what was last applied, not a read-back of BookKeeper's rack-placement
	// metadata, so a sync tick calls the rack setter only for bookies whose
	// desired rack differs from this cache.
	// +optional
	BookieRacks map[string]string `json:"bookieRacks,omitempty"`

	// observedGeneration is the most recent generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// conditions represent the current state of the BookKeeper resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.status.replicas`
// +kubebuilder:printcolumn:name="Writable",type=integer,JSONPath=`.status.writableBookies`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// BookKeeper is the Schema for the bookkeepers API
type BookKeeper struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of BookKeeper
	// +required
	Spec BookKeeperSpec `json:"spec"`

	// status defines the observed state of BookKeeper
	// +optional
	Status BookKeeperStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// BookKeeperList contains a list of BookKeeper
type BookKeeperList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []BookKeeper `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &BookKeeper{}, &BookKeeperList{})
		return nil
	})
}
