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
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

// oxiaHealthProbe builds an exec probe around the "oxia health" CLI
// subcommand against oxiaInternalPort (both coordinator and server probes
// target their internal port, never the server's public port), matching the
// real coordinator/server probes (pulsar-helm-chart _oxia.tpl:
// oxia-cluster.probe / oxia-cluster.readiness-probe). readiness adds
// "--service=oxia-readiness", which is how oxia's health check distinguishes
// "process is up" from "ready to serve" for the data server.
func oxiaHealthProbe(readiness bool, initialDelaySeconds int32) *corev1.Probe {
	command := []string{oxiaBinary, "health", fmt.Sprintf("--port=%d", oxiaInternalPort)}
	if readiness {
		command = append(command, "--service=oxia-readiness")
	}
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{Command: command},
		},
		InitialDelaySeconds: initialDelaySeconds,
		TimeoutSeconds:      10,
	}
}
