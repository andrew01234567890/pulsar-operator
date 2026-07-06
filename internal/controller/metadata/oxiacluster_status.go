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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	metadatav1alpha1 "github.com/andrew01234567890/pulsar-operator/api/metadata/v1alpha1"
)

// ReadyConditionType is the OxiaCluster status condition set once the
// coordinator Deployment is Available and every desired server replica is
// Ready.
const ReadyConditionType = "Ready"

const (
	reasonComponentsReady     = "ComponentsReady"
	reasonCoordinatorNotReady = "CoordinatorNotReady"
	reasonServerNotReady      = "ServerNotReady"
	reasonComponentsNotReady  = "ComponentsNotReady"
)

// aggregateStatus computes OxiaClusterStatus from the coordinator Deployment
// and server StatefulSet's observed state. Ready is True only when the
// coordinator Deployment reports its standard "Available" condition True
// *and* the server StatefulSet's ReadyReplicas equals the desired replica
// count — a partially-scaled server tier (e.g. mid rolling-restart after a
// servers-list change) must not read as Ready.
func aggregateStatus(oxia *metadatav1alpha1.OxiaCluster, coordinator *appsv1.Deployment, server *appsv1.StatefulSet) metadatav1alpha1.OxiaClusterStatus {
	coordinatorReady := deploymentAvailable(coordinator)
	desiredServerReplicas := serverReplicas(oxia)
	serverReady := server.Status.ReadyReplicas == desiredServerReplicas

	status := metadatav1alpha1.OxiaClusterStatus{
		CoordinatorReplicas: coordinator.Status.ReadyReplicas,
		ServerReplicas:      server.Status.ReadyReplicas,
		ObservedGeneration:  oxia.Generation,
		Conditions:          oxia.Status.Conditions,
	}

	condStatus, reason, message := readyCondition(coordinatorReady, serverReady)
	meta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:               ReadyConditionType,
		Status:             condStatus,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: oxia.Generation,
	})

	return status
}

func readyCondition(coordinatorReady, serverReady bool) (metav1.ConditionStatus, string, string) {
	switch {
	case coordinatorReady && serverReady:
		return metav1.ConditionTrue, reasonComponentsReady, "coordinator is Available and all server replicas are Ready"
	case !coordinatorReady && !serverReady:
		return metav1.ConditionFalse, reasonComponentsNotReady, "waiting for coordinator and server to become ready"
	case !coordinatorReady:
		return metav1.ConditionFalse, reasonCoordinatorNotReady, "waiting for coordinator to become Available"
	default:
		return metav1.ConditionFalse, reasonServerNotReady, "waiting for server replicas to become Ready"
	}
}

func deploymentAvailable(d *appsv1.Deployment) bool {
	for _, c := range d.Status.Conditions {
		if c.Type == appsv1.DeploymentAvailable {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}
