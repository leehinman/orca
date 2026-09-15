package steps

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/elastic/orca/harness"
)

type kibanaClient struct {
	base string
	user string
	pass string
	http *http.Client
}

type fleetUpload struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	ActionID string `json:"actionId"`
	Status   string `json:"status"`
	Error    string `json:"error"`
}

func newKibanaClient(state *harness.State, cfg harness.KibanaConfig) (*kibanaClient, error) {
	base, err := state.Resolve(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("resolve Kibana URL: %w", err)
	}
	if base == "" {
		return nil, fmt.Errorf("kibana.url is not configured")
	}
	user, _ := state.Resolve(cfg.Username)
	pass, _ := state.Resolve(cfg.Password)

	c := &kibanaClient{base: strings.TrimRight(base, "/"), user: user, pass: pass, http: &http.Client{}}
	if cfg.CAPath != "" {
		transport, err := buildTLSTransport(state, cfg.CAPath)
		if err != nil {
			return nil, err
		}
		c.http.Transport = transport
	}
	return c, nil
}

func (c *kibanaClient) newRequest(ctx context.Context, method, path string, body any) (*http.Request, error) {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rdr)
	if err != nil {
		return nil, err
	}
	req.Header.Set("kbn-xsrf", "true")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.user != "" {
		req.SetBasicAuth(c.user, c.pass)
	}
	return req, nil
}

func (c *kibanaClient) doJSON(ctx context.Context, method, path string, body, out any) error {
	req, err := c.newRequest(ctx, method, path, body)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s: HTTP %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("%s %s: decode response: %w", method, path, err)
	}
	return nil
}

func (c *kibanaClient) findAgentByHostname(ctx context.Context, hostname string) (string, error) {
	kuery := fmt.Sprintf(`local_metadata.host.hostname:"%s"`, strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(hostname))
	q := url.Values{"kuery": {kuery}, "perPage": {"10"}}
	var out struct {
		Items []struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"items"`
	}
	if err := c.doJSON(ctx, http.MethodGet, "/api/fleet/agents?"+q.Encode(), nil, &out); err != nil {
		return "", err
	}
	switch len(out.Items) {
	case 0:
		return "", fmt.Errorf("no active agent with hostname %q", hostname)
	case 1:
		return out.Items[0].ID, nil
	default:
		ids := make([]string, len(out.Items))
		for i, it := range out.Items {
			ids[i] = it.ID
		}
		return "", fmt.Errorf("%d active agents match hostname %q: %s", len(ids), hostname, strings.Join(ids, ", "))
	}
}

func (c *kibanaClient) requestDiagnostics(ctx context.Context, agentID string, cpuProfile bool) (string, error) {
	body := map[string]any{}
	if cpuProfile {
		body["additional_metrics"] = []string{"CPU"}
	}
	var out struct {
		ActionID string `json:"actionId"`
	}
	path := "/api/fleet/agents/" + url.PathEscape(agentID) + "/request_diagnostics"
	if err := c.doJSON(ctx, http.MethodPost, path, body, &out); err != nil {
		return "", err
	}
	if out.ActionID == "" {
		return "", fmt.Errorf("request_diagnostics for %s returned no actionId", agentID)
	}
	return out.ActionID, nil
}

func (c *kibanaClient) listUploads(ctx context.Context, agentID string) ([]fleetUpload, error) {
	var out struct {
		Items []fleetUpload `json:"items"`
	}
	path := "/api/fleet/agents/" + url.PathEscape(agentID) + "/uploads"
	if err := c.doJSON(ctx, http.MethodGet, path, nil, &out); err != nil {
		return nil, err
	}
	return out.Items, nil
}

func (c *kibanaClient) downloadFile(ctx context.Context, fileID, fileName, dest string) (int64, error) {
	path := "/api/fleet/agents/files/" + url.PathEscape(fileID) + "/" + url.PathEscape(fileName)
	req, err := c.newRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return 0, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return 0, fmt.Errorf("GET %s: HTTP %d: %s", path, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	return writeBody(resp.Body, dest)
}

func (c *kibanaClient) version(ctx context.Context) (string, error) {
	var out struct {
		Version struct {
			Number string `json:"number"`
		} `json:"version"`
	}
	if err := c.doJSON(ctx, http.MethodGet, "/api/status", nil, &out); err != nil {
		return "", err
	}
	if out.Version.Number == "" {
		return "", fmt.Errorf("kibana /api/status returned no version")
	}
	return out.Version.Number, nil
}

func (c *kibanaClient) generateReport(ctx context.Context, exportType, jobParams string) (string, error) {
	q := url.Values{"jobParams": {jobParams}}
	var out struct {
		Path string `json:"path"`
	}
	path := "/api/reporting/generate/" + exportType + "?" + q.Encode()
	if err := c.doJSON(ctx, http.MethodPost, path, nil, &out); err != nil {
		return "", err
	}
	if out.Path == "" {
		return "", fmt.Errorf("reporting generate %s returned no download path", exportType)
	}
	return out.Path, nil
}

// waitReport polls a Reporting download path until the job completes. Transport
// errors (for example Kibana restarting after an OOM-killed Chromium) are
// retried until ctx expires, since the job itself survives a Kibana restart.
func (c *kibanaClient) waitReport(ctx context.Context, downloadPath, dest string, poll time.Duration) (int64, error) {
	var lastErr error
	for {
		req, err := c.newRequest(ctx, http.MethodGet, downloadPath, nil)
		if err != nil {
			return 0, err
		}
		resp, err := c.http.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				if lastErr != nil {
					return 0, fmt.Errorf("%w (last transport error: %v)", ctx.Err(), lastErr)
				}
				return 0, err
			}
			if lastErr == nil || lastErr.Error() != err.Error() {
				fmt.Fprintf(os.Stderr, "[kibana] %s: %v; retrying\n", downloadPath, err)
			}
			lastErr = err
			select {
			case <-ctx.Done():
				return 0, fmt.Errorf("%w (last transport error: %v)", ctx.Err(), lastErr)
			case <-time.After(poll):
			}
			continue
		}
		switch resp.StatusCode {
		case http.StatusOK:
			n, err := writeBody(resp.Body, dest)
			resp.Body.Close()
			return n, err
		case http.StatusServiceUnavailable:
			resp.Body.Close()
		default:
			data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			return 0, fmt.Errorf("GET %s: HTTP %d: %s", downloadPath, resp.StatusCode, strings.TrimSpace(string(data)))
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(poll):
		}
	}
}

func writeBody(body io.Reader, dest string) (int64, error) {
	f, err := os.Create(dest)
	if err != nil {
		return 0, fmt.Errorf("create %s: %w", dest, err)
	}
	n, err := io.Copy(f, body)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(dest) //nolint:errcheck
		return 0, fmt.Errorf("write %s: %w", dest, err)
	}
	return n, nil
}
