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

// Package broker implements the broker CPU autoscaler's pure decision logic
// and the pluggable client that reports each broker's CPU utilization. The
// decision engine (Decide) takes no Kubernetes dependency at all, so its
// unanimous-vote scaling rule is fully covered by table-driven unit tests;
// the controller in internal/controller/cluster wires it to a real
// LoadClient and to the Broker/Pod API objects.
package broker

import (
	"context"

	corev1 "k8s.io/api/core/v1"
)

// LoadClient reports each broker pod's CPU utilization as a whole-number
// percent (0-100, matching BrokerAutoscalerSpec's threshold fields).
// Implementations are injected into the controller so tests can substitute
// a canned/mock source instead of talking to a real broker or
// metrics-server; CPUPercentByBroker is the entire seam.
type LoadClient interface {
	// CPUPercentByBroker returns a CPU percent for every pod in pods, keyed
	// by pod name. An implementation that cannot determine a value for some
	// pod should omit that pod's key rather than guess - the caller treats a
	// missing broker as "unknown" and refuses to act unanimously.
	CPUPercentByBroker(ctx context.Context, pods []corev1.Pod) (map[string]int32, error)
}
