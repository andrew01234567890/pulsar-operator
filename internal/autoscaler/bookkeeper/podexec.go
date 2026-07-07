package bookkeeper

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

// execTimeout bounds a single pod-exec round trip (curl call or bookkeeper
// shell command), independent of the overall decommission timeout the state
// machine enforces across the whole blocking-replication phase.
const execTimeout = 2 * time.Minute

// PodExecAdminClient implements AdminClient by exec-ing curl calls against
// the bookie admin REST API and `bin/bookkeeper shell`/`mv` commands inside
// each bookie's own container, mirroring KAAP's PodExecBookieAdminClient:
// every operation goes through the API server's pod-exec subresource (the
// same path `kubectl exec` uses), so it works regardless of whether the
// operator's pod network has a direct route to the bookie's, and regardless
// of NetworkPolicies that would otherwise block a direct HTTP call from the
// operator to the bookie.
type PodExecAdminClient struct {
	// RESTConfig authenticates the exec subresource requests.
	RESTConfig *rest.Config
	// Clientset issues the exec subresource requests.
	Clientset kubernetes.Interface
	// Namespace is the bookie pods' namespace.
	Namespace string
	// ContainerName is the bookie container to exec into.
	ContainerName string
	// AdminPort is the bookie admin HTTP port (matches bookieAdminPort /
	// httpServerPort in bookkeeper_controller.go's operator-managed config).
	AdminPort int32
	// JournalDir is the journal volume's mount path (matches
	// journalMountPath in bookkeeper_controller.go), used to locate the
	// on-disk cookie VERSION file under <JournalDir>/current/.
	JournalDir string
}

var _ AdminClient = (*PodExecAdminClient)(nil)

func (c *PodExecAdminClient) adminBaseURL() string {
	return fmt.Sprintf("http://localhost:%d", c.AdminPort)
}

func (c *PodExecAdminClient) IsWritable(ctx context.Context, podName string) (bool, error) {
	out, err := c.exec(ctx, podName, fmt.Sprintf("curl -s %s/api/v1/bookie/state", c.adminBaseURL()))
	if err != nil {
		return false, fmt.Errorf("querying bookie state for %s: %w", podName, err)
	}

	var state struct {
		Running      bool `json:"running"`
		ReadOnly     bool `json:"readOnly"`
		ShuttingDown bool `json:"shuttingDown"`
	}
	if err := json.Unmarshal([]byte(out), &state); err != nil {
		return false, fmt.Errorf("parsing bookie state for %s: %w (output: %s)", podName, err, out)
	}
	return state.Running && !state.ReadOnly && !state.ShuttingDown, nil
}

func (c *PodExecAdminClient) LedgerDiskUsageBelow(ctx context.Context, podName string, tolerance float64) (bool, error) {
	out, err := c.exec(ctx, podName, fmt.Sprintf("curl -s %s/api/v1/bookie/info", c.adminBaseURL()))
	if err != nil {
		return false, fmt.Errorf("querying bookie info for %s: %w", podName, err)
	}

	var info struct {
		FreeSpace  int64 `json:"freeSpace"`
		TotalSpace int64 `json:"totalSpace"`
	}
	if err := json.Unmarshal([]byte(out), &info); err != nil {
		return false, fmt.Errorf("parsing bookie info for %s: %w (output: %s)", podName, err, out)
	}
	if info.TotalSpace <= 0 {
		return false, fmt.Errorf("bookie %s reported a non-positive totalSpace (%d)", podName, info.TotalSpace)
	}

	used := float64(info.TotalSpace-info.FreeSpace) / float64(info.TotalSpace)
	return used < tolerance, nil
}

func (c *PodExecAdminClient) SetReadOnly(ctx context.Context, podName string, readOnly bool) error {
	cmd := fmt.Sprintf(
		`curl -s -X PUT -H "Content-Type: application/json" -d '{"readOnly":%t}' %s/api/v1/bookie/state/readonly`,
		readOnly, c.adminBaseURL())
	if _, err := c.exec(ctx, podName, cmd); err != nil {
		return fmt.Errorf("setting read-only=%t on bookie %s: %w", readOnly, podName, err)
	}
	return nil
}

func (c *PodExecAdminClient) TriggerDecommission(ctx context.Context, podName, bookieID string) error {
	_, err := c.exec(ctx, podName, fmt.Sprintf("bin/bookkeeper shell decommissionbookie -bookieid %s", bookieID))
	if err == nil {
		return nil
	}

	// Fallback: `recover -f` re-replicates the bookie's ledgers the same way
	// decommissionbookie does internally, for BookKeeper versions/configs
	// where decommissionbookie itself is unavailable or fails outright.
	if _, fallbackErr := c.exec(ctx, podName, fmt.Sprintf("bin/bookkeeper shell recover -f -bookieid %s", bookieID)); fallbackErr != nil {
		return fmt.Errorf("decommissionbookie failed (%w) and recover fallback also failed: %w", err, fallbackErr)
	}
	return nil
}

func (c *PodExecAdminClient) HasLedgers(ctx context.Context, podName, bookieID string) (bool, error) {
	out, err := c.exec(ctx, podName, fmt.Sprintf("bin/bookkeeper shell listledgers -meta -bookieid %s", bookieID))
	if err != nil {
		return false, fmt.Errorf("listing ledgers for bookie %s: %w", podName, err)
	}

	// An inconclusive result must never be read as "safe to proceed" -- err
	// on the side of "still has ledgers" so the state machine keeps waiting
	// (or eventually times out and reverts) instead of deleting data.
	if strings.Contains(out, "Unable to read the ledger") ||
		strings.Contains(out, "Received error return value while processing ledgers") ||
		strings.Contains(out, "Received Exception while processing ledgers") {
		return true, nil
	}
	return strings.Contains(out, "ledgerID: "), nil
}

func (c *PodExecAdminClient) NoUnderReplicatedLedgers(ctx context.Context, podName string) (bool, error) {
	out, err := c.exec(ctx, podName, fmt.Sprintf("curl -s %s/api/v1/autorecovery/list_under_replicated_ledger/", c.adminBaseURL()))
	if err != nil {
		return false, fmt.Errorf("listing under-replicated ledgers via bookie %s: %w", podName, err)
	}
	return strings.Contains(out, "No under replicated ledgers found"), nil
}

func (c *PodExecAdminClient) RenameCookie(ctx context.Context, podName string) error {
	dir := c.JournalDir + "/current"
	// Idempotent by construction: if a prior attempt's mv actually succeeded
	// but the exec response was lost (the ambiguity KAAP's own
	// implementation calls out as an open risk), the renamed file already
	// exists and this becomes a no-op success instead of failing forever on
	// a missing source file.
	cmd := fmt.Sprintf("test -f %[1]s/VERSION.decommissioned || mv %[1]s/VERSION %[1]s/VERSION.decommissioned", dir)
	if _, err := c.exec(ctx, podName, cmd); err != nil {
		return fmt.Errorf("renaming cookie for bookie %s: %w", podName, err)
	}
	return nil
}

func (c *PodExecAdminClient) exec(ctx context.Context, podName, command string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, execTimeout)
	defer cancel()

	req := c.Clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(c.Namespace).
		Name(podName).
		SubResource("exec")
	req.VersionedParams(&corev1.PodExecOptions{
		Container: c.ContainerName,
		Command:   []string{"bash", "-c", command},
		Stdout:    true,
		Stderr:    true,
	}, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(c.RESTConfig, "POST", req.URL())
	if err != nil {
		return "", fmt.Errorf("building exec stream for pod %s: %w", podName, err)
	}

	var stdout, stderr bytes.Buffer
	if err := executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr}); err != nil {
		return stdout.String(), fmt.Errorf("exec %q on pod %s: %w (stderr: %s)", command, podName, err, stderr.String())
	}
	return stdout.String(), nil
}
