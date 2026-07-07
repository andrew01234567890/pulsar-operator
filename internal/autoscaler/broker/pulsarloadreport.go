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

package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// defaultLoadReportTimeout bounds a single broker's load-report request so
// one unreachable pod can't stall an entire autoscaler tick.
const defaultLoadReportTimeout = 10 * time.Second

// PulsarLoadReportClient is the ResourcesUsageSource=PulsarLBReport default
// LoadClient: it reads each broker's own load-manager report - the same
// "cpu": {"usage", "limit"} pair Pulsar's ThresholdShedder/LoadManager
// consult - rather than raw kubelet cgroup CPU, so the autoscaler agrees
// with what Pulsar itself considers "hot".
type PulsarLoadReportClient struct {
	// HTTPClient defaults to an *http.Client with defaultLoadReportTimeout
	// when nil.
	HTTPClient *http.Client
	// HTTPPort is the broker web service port to query (the merged
	// broker.conf's webServicePort, not necessarily 8080).
	HTTPPort int32
}

type loadReportResponse struct {
	CPU struct {
		Usage float64 `json:"usage"`
		Limit float64 `json:"limit"`
	} `json:"cpu"`
}

// CPUPercentByBroker fetches /admin/v2/broker-stats/load-report/ from every
// pod's IP directly (the operator runs in-cluster, so unlike KAAP's
// exec-into-pod-and-curl workaround it can reach the pod network without a
// shell). A pod that fails to respond is omitted from the result map rather
// than failing the whole call; the caller decides whether a partial result
// is still safe to act on.
func (c *PulsarLoadReportClient) CPUPercentByBroker(ctx context.Context, pods []corev1.Pod) (map[string]int32, error) {
	client := c.httpClient()

	result := make(map[string]int32, len(pods))
	var errs []error
	for _, pod := range pods {
		if pod.Status.PodIP == "" {
			errs = append(errs, fmt.Errorf("broker pod %s: no PodIP assigned yet", pod.Name))
			continue
		}

		percent, err := c.fetchOne(ctx, client, pod.Status.PodIP)
		if err != nil {
			errs = append(errs, fmt.Errorf("broker pod %s: %w", pod.Name, err))
			continue
		}
		result[pod.Name] = percent
	}

	return result, errors.Join(errs...)
}

func (c *PulsarLoadReportClient) fetchOne(ctx context.Context, client *http.Client, podIP string) (int32, error) {
	url := fmt.Sprintf("http://%s:%d/admin/v2/broker-stats/load-report/", podIP, c.HTTPPort)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, fmt.Errorf("building load-report request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("requesting load-report: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("load-report returned status %d", resp.StatusCode)
	}

	var report loadReportResponse
	if err := json.NewDecoder(resp.Body).Decode(&report); err != nil {
		return 0, fmt.Errorf("decoding load-report: %w", err)
	}

	return cpuPercentFromUsageLimit(report.CPU.Usage, report.CPU.Limit), nil
}

func (c *PulsarLoadReportClient) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return &http.Client{Timeout: defaultLoadReportTimeout}
}

// cpuPercentFromUsageLimit converts the load-report's raw usage/limit pair
// into a clamped [0, 100] whole-number percent. A non-positive limit (a
// broker that hasn't reported real numbers yet) reads as 0% rather than
// dividing by zero.
func cpuPercentFromUsageLimit(usage, limit float64) int32 {
	if limit <= 0 {
		return 0
	}

	percent := math.Round(usage / limit * 100)
	switch {
	case percent < 0:
		return 0
	case percent > 100:
		return 100
	default:
		return int32(percent)
	}
}
