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
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestCpuPercentFromUsageLimit(t *testing.T) {
	tests := []struct {
		name  string
		usage float64
		limit float64
		want  int32
	}{
		{name: "half utilized", usage: 50, limit: 100, want: 50},
		{name: "fully utilized", usage: 100, limit: 100, want: 100},
		{name: "zero limit avoids divide by zero", usage: 50, limit: 0, want: 0},
		{name: "negative limit avoids divide by zero", usage: 50, limit: -1, want: 0},
		{name: "usage above limit clamps to 100", usage: 150, limit: 100, want: 100},
		{name: "rounds to nearest whole percent", usage: 33.7, limit: 100, want: 34},
		{name: "zero usage", usage: 0, limit: 100, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cpuPercentFromUsageLimit(tt.usage, tt.limit); got != tt.want {
				t.Errorf("cpuPercentFromUsageLimit(%v, %v) = %d, want %d", tt.usage, tt.limit, got, tt.want)
			}
		})
	}
}

func newLoadReportServer(t *testing.T, body string, status int) (podIP string, port int32) {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/admin/v2/broker-stats/load-report") {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)

	u, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parsing test server URL: %v", err)
	}
	p, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatalf("parsing test server port: %v", err)
	}
	return u.Hostname(), int32(p)
}

func TestPulsarLoadReportClient_CPUPercentByBroker(t *testing.T) {
	t.Run("reads and converts the load-report CPU usage/limit pair", func(t *testing.T) {
		podIP, port := newLoadReportServer(t, `{"cpu":{"usage":45.0,"limit":100.0}}`, http.StatusOK)
		client := &PulsarLoadReportClient{HTTPPort: port}

		pods := []corev1.Pod{brokerPod(testBroker0, podIP)}
		got, err := client.CPUPercentByBroker(context.Background(), pods)
		if err != nil {
			t.Fatalf("CPUPercentByBroker() error = %v", err)
		}
		if got[testBroker0] != 45 {
			t.Errorf("CPUPercentByBroker()[broker-0] = %d, want 45", got[testBroker0])
		}
	})

	t.Run("a pod with no PodIP is omitted and reported as an error", func(t *testing.T) {
		client := &PulsarLoadReportClient{HTTPPort: 8080}
		pods := []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: testBroker0}}}

		got, err := client.CPUPercentByBroker(context.Background(), pods)
		if err == nil {
			t.Fatal("CPUPercentByBroker() error = nil, want an error for the pod with no PodIP")
		}
		if _, ok := got[testBroker0]; ok {
			t.Error("CPUPercentByBroker() should not report a percent for a pod with no PodIP")
		}
	})

	t.Run("an unreachable pod is omitted and reported as an error without failing the whole batch", func(t *testing.T) {
		podIP, port := newLoadReportServer(t, `{"cpu":{"usage":10.0,"limit":100.0}}`, http.StatusOK)
		client := &PulsarLoadReportClient{HTTPPort: port}

		// 127.0.0.99 is loopback (always routable) but nothing listens
		// there, so the connection is refused immediately rather than
		// hanging - a fast, deterministic "unreachable broker" simulation.
		pods := []corev1.Pod{
			brokerPod(testBroker0, podIP),
			brokerPod("broker-1", "127.0.0.99"),
		}

		got, err := client.CPUPercentByBroker(context.Background(), pods)
		if err == nil {
			t.Fatal("CPUPercentByBroker() error = nil, want an error for the unreachable broker")
		}
		if got[testBroker0] != 10 {
			t.Errorf("CPUPercentByBroker()[broker-0] = %d, want 10", got[testBroker0])
		}
		if _, ok := got["broker-1"]; ok {
			t.Error("CPUPercentByBroker() should not report a percent for the unreachable broker")
		}
	})

	t.Run("a non-200 response is an error", func(t *testing.T) {
		podIP, port := newLoadReportServer(t, `internal error`, http.StatusInternalServerError)
		client := &PulsarLoadReportClient{HTTPPort: port}

		_, err := client.CPUPercentByBroker(context.Background(), []corev1.Pod{brokerPod(testBroker0, podIP)})
		if err == nil {
			t.Fatal("CPUPercentByBroker() error = nil, want an error for a non-200 response")
		}
	})

	t.Run("malformed JSON is an error", func(t *testing.T) {
		podIP, port := newLoadReportServer(t, `not json`, http.StatusOK)
		client := &PulsarLoadReportClient{HTTPPort: port}

		_, err := client.CPUPercentByBroker(context.Background(), []corev1.Pod{brokerPod(testBroker0, podIP)})
		if err == nil {
			t.Fatal("CPUPercentByBroker() error = nil, want an error for malformed JSON")
		}
	})
}

func brokerPod(name, podIP string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status:     corev1.PodStatus{PodIP: podIP},
	}
}
