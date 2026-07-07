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
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// conditionTypeReady ("Ready") and reasonProgressing ("Progressing") are
// declared once for this package (in pulsarcluster_controller.go and
// bookkeeper_controller.go respectively) and reused here. The two reasons
// below are specific to the stateless/optional tiers this helper serves and
// deliberately differ from BookKeeper's (which treats zero replicas as an
// error, not a parked tier).
const (
	// reasonReplicasReady marks a fully-converged workload: the controller
	// has observed the current spec and every desired pod is updated to the
	// latest template and Ready.
	reasonReplicasReady = "ReplicasReady"

	// reasonScaledToZero marks a tier deliberately parked at 0 replicas. Kept
	// Ready=True (non-blocking for umbrella rollups and `kubectl wait`) but
	// with a distinct reason so it is never mistaken for a serving tier.
	reasonScaledToZero = "ScaledToZero"
)

// rolloutStatus captures the workload-status fields needed to decide whether a
// rolling update has fully converged, abstracting over StatefulSet and
// Deployment, whose relevant Status fields share names and meaning.
type rolloutStatus struct {
	// generation is the workload's metadata.generation - the spec the API
	// server currently stores. A template edit (e.g. a new config-checksum
	// annotation) bumps it.
	generation int64
	// observedGeneration is the generation the workload controller has last
	// acted on. While it lags generation, the controller hasn't yet rolled
	// the new template, so readyReplicas still reflects the old revision and
	// must not be read as "the new spec is ready".
	observedGeneration int64
	// updatedReplicas is the number of pods already running the latest
	// template.
	updatedReplicas int32
	// readyReplicas is the number of Ready pods.
	readyReplicas int32
}

func statefulSetRollout(sts *appsv1.StatefulSet) rolloutStatus {
	return rolloutStatus{
		generation:         sts.Generation,
		observedGeneration: sts.Status.ObservedGeneration,
		updatedReplicas:    sts.Status.UpdatedReplicas,
		readyReplicas:      sts.Status.ReadyReplicas,
	}
}

func deploymentRollout(deploy *appsv1.Deployment) rolloutStatus {
	return rolloutStatus{
		generation:         deploy.Generation,
		observedGeneration: deploy.Status.ObservedGeneration,
		updatedReplicas:    deploy.Status.UpdatedReplicas,
		readyReplicas:      deploy.Status.ReadyReplicas,
	}
}

// workloadReadyCondition computes a component's Ready condition from the
// desired replica count and the observed rollout status of its workload.
//
// Ready is True only when the rollout has fully converged: the workload
// controller has observed the current spec and every desired pod is both
// updated to the latest template and Ready. It deliberately stays False
// (reason Progressing) during a rolling restart - e.g. one triggered by a
// config-checksum or image change - even while a stale Status still reports
// the old revision as fully ready. A tier scaled to zero is a separate,
// non-error state reported Ready with reason ScaledToZero.
//
// noun names the component in the human-readable message (e.g. "proxy").
func workloadReadyCondition(generation int64, desired int32, rollout rolloutStatus, noun string) metav1.Condition {
	base := metav1.Condition{Type: conditionTypeReady, ObservedGeneration: generation}

	switch {
	case desired == 0:
		base.Status = metav1.ConditionTrue
		base.Reason = reasonScaledToZero
		base.Message = fmt.Sprintf("%s scaled to 0 replicas (not serving)", noun)
	case rollout.observedGeneration == rollout.generation &&
		rollout.updatedReplicas == desired &&
		rollout.readyReplicas == desired:
		base.Status = metav1.ConditionTrue
		base.Reason = reasonReplicasReady
		base.Message = fmt.Sprintf("%d/%d %s replicas ready", rollout.readyReplicas, desired, noun)
	default:
		base.Status = metav1.ConditionFalse
		base.Reason = reasonProgressing
		base.Message = fmt.Sprintf("%s rollout in progress: %d/%d updated, %d/%d ready",
			noun, rollout.updatedReplicas, desired, rollout.readyReplicas, desired)
	}

	return base
}
