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

package metadata

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func availableDeployment(ready bool) *appsv1.Deployment {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return &appsv1.Deployment{
		Status: appsv1.DeploymentStatus{
			Conditions: []appsv1.DeploymentCondition{
				{Type: appsv1.DeploymentAvailable, Status: status},
			},
		},
	}
}

func statefulSetWithReadyReplicas(ready int32) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{Status: appsv1.StatefulSetStatus{ReadyReplicas: ready}}
}

func TestAggregateStatusReadyCondition(t *testing.T) {
	tests := []struct {
		name              string
		coordinatorReady  bool
		serverReadyCount  int32
		desiredReplicas   int32
		wantConditionTrue bool
		wantReason        string
	}{
		{
			name:              "both ready",
			coordinatorReady:  true,
			serverReadyCount:  3,
			desiredReplicas:   3,
			wantConditionTrue: true,
			wantReason:        reasonComponentsReady,
		},
		{
			name:              "coordinator not available",
			coordinatorReady:  false,
			serverReadyCount:  3,
			desiredReplicas:   3,
			wantConditionTrue: false,
			wantReason:        reasonCoordinatorNotReady,
		},
		{
			name:              "server under desired replicas",
			coordinatorReady:  true,
			serverReadyCount:  2,
			desiredReplicas:   3,
			wantConditionTrue: false,
			wantReason:        reasonServerNotReady,
		},
		{
			name:              "server over desired replicas is still not ready",
			coordinatorReady:  true,
			serverReadyCount:  4,
			desiredReplicas:   3,
			wantConditionTrue: false,
			wantReason:        reasonServerNotReady,
		},
		{
			name:              "neither ready",
			coordinatorReady:  false,
			serverReadyCount:  0,
			desiredReplicas:   3,
			wantConditionTrue: false,
			wantReason:        reasonComponentsNotReady,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oxia := newTestOxiaCluster(withServerReplicas(tt.desiredReplicas))
			status := aggregateStatus(oxia, availableDeployment(tt.coordinatorReady), statefulSetWithReadyReplicas(tt.serverReadyCount))

			cond := meta.FindStatusCondition(status.Conditions, ReadyConditionType)
			if cond == nil {
				t.Fatalf("aggregateStatus() produced no %q condition", ReadyConditionType)
			}
			gotTrue := cond.Status == metav1.ConditionTrue
			if gotTrue != tt.wantConditionTrue {
				t.Errorf("Ready condition status = %v, want True=%v", cond.Status, tt.wantConditionTrue)
			}
			if cond.Reason != tt.wantReason {
				t.Errorf("Ready condition reason = %q, want %q", cond.Reason, tt.wantReason)
			}
		})
	}
}

func TestAggregateStatusReplicaCounts(t *testing.T) {
	oxia := newTestOxiaCluster(withServerReplicas(3))
	coordinator := availableDeployment(true)
	coordinator.Status.ReadyReplicas = 2
	server := statefulSetWithReadyReplicas(3)

	status := aggregateStatus(oxia, coordinator, server)

	if status.CoordinatorReplicas != 2 {
		t.Errorf("status.CoordinatorReplicas = %d, want 2", status.CoordinatorReplicas)
	}
	if status.ServerReplicas != 3 {
		t.Errorf("status.ServerReplicas = %d, want 3", status.ServerReplicas)
	}
}

func TestAggregateStatusObservedGeneration(t *testing.T) {
	oxia := newTestOxiaCluster(withServerReplicas(3))
	oxia.Generation = 7

	status := aggregateStatus(oxia, availableDeployment(true), statefulSetWithReadyReplicas(3))

	if status.ObservedGeneration != 7 {
		t.Errorf("status.ObservedGeneration = %d, want 7", status.ObservedGeneration)
	}
	cond := meta.FindStatusCondition(status.Conditions, ReadyConditionType)
	if cond == nil {
		t.Fatalf("aggregateStatus() produced no %q condition", ReadyConditionType)
	}
	if cond.ObservedGeneration != 7 {
		t.Errorf("Ready condition ObservedGeneration = %d, want 7", cond.ObservedGeneration)
	}
}

// TestAggregateStatusNotReadyDuringServerRollout is the regression proof for
// the status half of the CRITICAL requirement: while the server tier is
// mid-rollout after a servers-list change (ReadyReplicas lags the new
// desired count), status must not report Ready — even though the
// coordinator itself may already be Available throughout the rollout.
func TestAggregateStatusNotReadyDuringServerRollout(t *testing.T) {
	oxia := newTestOxiaCluster(withServerReplicas(5))
	status := aggregateStatus(oxia, availableDeployment(true), statefulSetWithReadyReplicas(3))

	cond := meta.FindStatusCondition(status.Conditions, ReadyConditionType)
	if cond == nil {
		t.Fatalf("aggregateStatus() produced no %q condition", ReadyConditionType)
	}
	if cond.Status == metav1.ConditionTrue {
		t.Errorf("Ready condition = True during server rollout (3/5 ready), want False")
	}
}
