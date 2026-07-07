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

// Package rackawareness implements applying BookKeeper rack-placement
// metadata. The RackSetter interface is the seam
// internal/controller/cluster's BookKeeperRackReconciler applies its
// computed bookie->rack mapping through, so tests can inject a mock instead
// of a live cluster; PodExecRackSetter is the default, cluster-facing
// implementation.
package rackawareness

import (
	"bytes"
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

// RackSetter applies BookKeeper rack-placement metadata for a single
// bookie. The rack-sync controller tracks each bookie's last-applied rack
// itself (BookKeeper.status.bookieRacks) and only calls SetBookieRack when
// the desired rack differs from that cache, so a steady-state cluster
// issues zero calls per tick.
type RackSetter interface {
	// SetBookieRack sets bookieID's BookKeeper rack-placement metadata to
	// rack.
	SetBookieRack(ctx context.Context, bookieID, rack string) error
}

// pulsarAdminBin is the pulsar-admin CLI bundled in every official Apache
// Pulsar image, on bookie pods and broker pods alike.
const pulsarAdminBin = "bin/pulsar-admin"

// PodExecRackSetter is the default RackSetter. BookKeeper rack-placement
// metadata is mutable only through the Pulsar admin API/CLI - never by
// writing to the metadata store directly, since the operator must work
// identically whether the cluster is metadata-store-Oxia or ZooKeeper-backed
// - so this pod-execs `bin/pulsar-admin bookies set-bookie-rack` into a
// running bookie or broker pod, exactly like `kubectl exec`.
type PodExecRackSetter struct {
	RESTConfig *rest.Config
	ClientSet  kubernetes.Interface

	// Namespace/PodName/Container name the pod pulsar-admin is exec'd into.
	Namespace string
	PodName   string
	Container string

	// AdminURL is passed as pulsar-admin's --admin-url flag when non-empty.
	// Left empty, pulsar-admin falls back to the target pod's bundled
	// conf/client.conf, which resolves out of the box when the exec target
	// is a broker pod (its own webServicePort, normally 8080).
	AdminURL string

	// execFn runs command inside the target pod and returns its combined
	// stdout/stderr. Nil uses execInPod (a real SPDY exec against
	// RESTConfig/ClientSet); overridden in this package's tests so the
	// command-construction and error-wrapping logic can be exercised
	// without a live cluster.
	execFn func(ctx context.Context, command []string) (output string, err error)
}

// SetBookieRack implements RackSetter.
func (p *PodExecRackSetter) SetBookieRack(ctx context.Context, bookieID, rack string) error {
	command := p.adminCommand("bookies", "set-bookie-rack",
		"--bookie", bookieID, "--rack", rack, "--hostname", bookieID)

	output, err := p.exec(ctx, command)
	if err != nil {
		return fmt.Errorf("set-bookie-rack --bookie %s --rack %s: %w (%s)", bookieID, rack, err, output)
	}
	return nil
}

func (p *PodExecRackSetter) adminCommand(args ...string) []string {
	command := []string{pulsarAdminBin}
	if p.AdminURL != "" {
		command = append(command, "--admin-url", p.AdminURL)
	}
	return append(command, args...)
}

func (p *PodExecRackSetter) exec(ctx context.Context, command []string) (string, error) {
	if p.execFn != nil {
		return p.execFn(ctx, command)
	}
	return p.execInPod(ctx, command)
}

// execInPod runs command inside p.PodName over a SPDY exec stream, the same
// transport `kubectl exec` uses.
func (p *PodExecRackSetter) execInPod(ctx context.Context, command []string) (string, error) {
	req := p.ClientSet.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(p.PodName).
		Namespace(p.Namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: p.Container,
			Command:   command,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(p.RESTConfig, "POST", req.URL())
	if err != nil {
		return "", fmt.Errorf("build pod exec for %s/%s: %w", p.Namespace, p.PodName, err)
	}

	var out bytes.Buffer
	err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &out, Stderr: &out})
	return out.String(), err
}
