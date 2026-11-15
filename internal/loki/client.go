// HTTP client for the Loki query API.
//
// Every function talks to the server named by a `loki` DuckDB secret: Grafana
// Loki (single binary or microservices behind the query frontend), Grafana
// Cloud Logs (basic auth: instance id + access-policy token), or anything
// that serves the Loki HTTP API behind a bearer token. The secret carries the
// base URL, a bearer token or basic-auth pair, an optional tenant id (sent as
// X-Scope-OrgID) and extra headers. Credentials are read from the secret on
// every call and never kept in table-function state, which travels back to
// DuckDB over the HTTP transport.

package loki

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

const (
	// SecretType is the DuckDB secret type every function reads.
	SecretType = "loki"
	// MaxAttempts bounds retries of 429/502/503/504 responses and network errors.
	MaxAttempts = 4
	// Timeout bounds every request.
	Timeout = 120 * time.Second
	// maxGetURL is the longest URL sent as GET; longer requests are POSTed as a form.
	maxGetURL = 4000
	// encodingFlagsHeader asks Loki 3.x to return stream labels, structured
	// metadata and parsed labels as separate categories instead of merging
	// them into one label set.
	encodingFlagsHeader = "X-Loki-Response-Encoding-Flags"
)

// userAgent identifies the worker to the API.
const userAgent = "vgi-loki-worker/" + Version

func secretField(name, doc string, redact bool) arrow.Field {
	keys := []string{"vgi_doc"}
	vals := []string{doc}
	if redact {
		keys = append(keys, "redact")
		vals = append(vals, "true")
	}
	return arrow.Field{Name: name, Type: arrow.BinaryTypes.String, Nullable: true,
		Metadata: arrow.NewMetadata(keys, vals)}
}

// APISecretType is registered with DuckDB so users can run
// CREATE SECRET (TYPE loki, BASE_URL '...', BEARER_TOKEN '...').
var APISecretType = vgi.SecretTypeSpec{
	Name:        SecretType,
	Description: "Connection to a Grafana Loki HTTP API (self-hosted Loki, Grafana Cloud Logs, or a Loki-compatible gateway).",
	Schema: arrow.NewSchema([]arrow.Field{
		secretField("base_url", "Server root, e.g. 'http://loki:3100' or 'https://logs-prod-012.grafana.net'. /loki/api/v1/... is appended.", false),
		secretField("bearer_token", "Token sent as 'Authorization: Bearer <token>'.", true),
		secretField("username", "Basic-auth user name (Grafana Cloud Logs: the numeric instance id).", false),
		secretField("password", "Basic-auth password (Grafana Cloud: an access-policy token with logs:read).", true),
		secretField("tenant_id", "Tenant sent as the X-Scope-OrgID header (multi-tenant Loki).", false),
		secretField("extra_headers", "Additional request headers as a JSON object, e.g. '{\"X-Custom\": \"v\"}'.", true),
	}, &secretSchemaMetadata),
}

// secretSchemaMetadata links the secret type to its documentation.
var secretSchemaMetadata = arrow.NewMetadata(
	[]string{"vgi_doc_url"},
	[]string{SourceURL + "#secrets-and-configuration"})

// secretRequirements is declared by every server function: resolve the
// session's loki secret(s) and pass them to the worker.
var secretRequirements = []vgi.SecretRequirement{{SecretType: SecretType}}

// bindWithSecret binds a table function's schema. When secret_name names a
// secret, the first bind asks the extension to resolve that secret by name
// (two-phase bind); otherwise only the default secret of the type is sent.
func bindWithSecret[A secretNamed](params *vgi.BindParams, schema *arrow.Schema) (*vgi.BindResponse, error) {
	var a A
	if err := vgi.BindArgs(params.Args, &a); err != nil {
		return nil, err
	}
	return bindNamedSecret(params, a.secret(), schema)
}

// bindNamedSecret is bindWithSecret for functions that decode their own arguments.
func bindNamedSecret(params *vgi.BindParams, name string, schema *arrow.Schema) (*vgi.BindResponse, error) {
	if name != "" && !params.ResolvedSecretsProvided {
		return &vgi.BindResponse{SecretScopeRequest: []vgi.SecretLookup{{SecretType: SecretType, SecretName: name}}}, nil
	}
	return vgi.BindSchema(schema)
}

// conn is a resolved connection. It is built per call from the secret and is
// never stored in scan state.
type conn struct {
	BaseURL string
	Headers http.Header
}

// resolveConn picks the secret (the named one when name is non-empty) and
// turns it into a connection.
func resolveConn(secrets vgi.Secrets, name string) (*conn, error) {
	var fields map[string]interface{}
	var ok bool
	if name != "" {
		fields, ok = secrets.LookupNamed(SecretType, name)
		if !ok {
			return nil, fmt.Errorf("no loki secret named %q: create it with CREATE SECRET %s (TYPE loki, BASE_URL '...')", name, name)
		}
	} else {
		fields, ok = secrets.Lookup(SecretType)
		if !ok {
			return nil, errors.New("no loki secret found: create one with CREATE SECRET (TYPE loki, BASE_URL 'http://localhost:3100') and add BEARER_TOKEN, USERNAME/PASSWORD or TENANT_ID as your server needs")
		}
	}
	get := func(k string) string { return strings.TrimSpace(vgi.RenderSecretValue(fields[k])) }
	base := strings.TrimRight(get("base_url"), "/")
	if base == "" {
		return nil, errors.New("the loki secret has no BASE_URL")
	}
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("the loki secret's BASE_URL must be an http(s) URL, got %q", base)
	}
	h := http.Header{}
	h.Set("Accept", "application/json")
	h.Set("User-Agent", userAgent)
	if extra := get("extra_headers"); extra != "" {
		var m map[string]string
		if err := json.Unmarshal([]byte(extra), &m); err != nil {
			return nil, errors.New("the loki secret's EXTRA_HEADERS must be a JSON object of strings, e.g. '{\"X-Custom\": \"value\"}'")
		}
		for k, v := range m {
			h.Set(k, v)
		}
	}
	if t := get("bearer_token"); t != "" {
		h.Set("Authorization", "Bearer "+t)
	} else if user := get("username"); user != "" {
		req := &http.Request{Header: http.Header{}}
		req.SetBasicAuth(user, vgi.RenderSecretValue(fields["password"]))
		h.Set("Authorization", req.Header.Get("Authorization"))
	}
	if t := get("tenant_id"); t != "" {
		h.Set("X-Scope-OrgID", t)
	}
	h.Set(encodingFlagsHeader, "categorize-labels")
	return &conn{BaseURL: base, Headers: h}, nil
}

// apiError is a failed API call. The message names the HTTP status and the
// server's error text so users can tell a bad query from an outage.
type apiError struct {
	Status  int
	Message string
	Path    string
}

func (e *apiError) Error() string {
	msg := fmt.Sprintf("loki %s: HTTP %d", e.Path, e.Status)
	if e.Status == http.StatusNotFound {
		msg += " (endpoint not found: this server or gateway does not serve it)"
	}
	if e.Message != "" {
		msg += ": " + e.Message
	}
	return msg
}

// envelope is the {"status": ..., "data": ...} wrapper most endpoints use.
type envelope struct {
	Status   string          `json:"status"`
	Data     json.RawMessage `json:"data"`
	Error    string          `json:"error"`
	Warnings []string        `json:"warnings"`
}

// client is shared so connections are reused across pages.
var client = &http.Client{Timeout: Timeout}

// sleep is replaced in tests so retries don't wait.
var sleep = func(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// retryDelay honours a Retry-After header (seconds or HTTP date), else backs off exponentially.
func retryDelay(header string, attempt int) time.Duration {
	header = strings.TrimSpace(header)
	if s, err := strconv.Atoi(header); err == nil && s >= 0 {
		return min(time.Duration(s)*time.Second, 30*time.Second)
	}
	if t, err := http.ParseTime(header); err == nil {
		return min(max(time.Until(t), 0), 30*time.Second)
	}
	return time.Duration(250<<attempt) * time.Millisecond
}

func retryable(status int) bool {
	return status == http.StatusTooManyRequests || status == http.StatusBadGateway ||
		status == http.StatusServiceUnavailable || status == http.StatusGatewayTimeout
}

// encodeParams sorts keys so request URLs are deterministic (recorded
// fixtures key on them).
func encodeParams(params url.Values) string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		for _, v := range params[k] {
			if b.Len() > 0 {
				b.WriteByte('&')
			}
			b.WriteString(url.QueryEscape(k))
			b.WriteByte('=')
			b.WriteString(url.QueryEscape(v))
		}
	}
	return b.String()
}

// errorText extracts a readable message from an error body: Loki answers
// with plain text ("parse error at line 1 ...") or a JSON object with an
// "error" or "message" field.
func errorText(body []byte) string {
	var m struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	if json.Unmarshal(body, &m) == nil {
		if m.Error != "" {
			return m.Error
		}
		if m.Message != "" {
			return m.Message
		}
	}
	s := strings.TrimSpace(string(body))
	if len(s) > 300 {
		s = s[:300]
	}
	return s
}

// raw calls GET <base>/<path>?params (POSTing a form when the URL would be
// too long and the endpoint accepts POST), retrying rate-limited and
// unavailable responses, and returns the body of a 2xx response.
func (c *conn) raw(ctx context.Context, path string, params url.Values, postOK bool) ([]byte, error) {
	endpoint := c.BaseURL + "/" + path
	query := encodeParams(params)
	usePost := postOK && len(endpoint)+len(query)+1 > maxGetURL
	for attempt := 1; ; attempt++ {
		var req *http.Request
		var err error
		if usePost {
			req, err = http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader([]byte(query)))
			if err == nil {
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			}
		} else {
			target := endpoint
			if query != "" {
				target += "?" + query
			}
			req, err = http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		}
		if err != nil {
			return nil, err
		}
		for k, vs := range c.Headers {
			for _, v := range vs {
				req.Header.Add(k, v)
			}
		}
		resp, err := client.Do(req)
		if err != nil {
			if attempt >= MaxAttempts || errors.Is(err, context.Canceled) {
				return nil, fmt.Errorf("loki /%s: %w", path, err)
			}
			sleep(ctx, retryDelay("", attempt))
			continue
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<30))
		_ = resp.Body.Close()
		if retryable(resp.StatusCode) && attempt < MaxAttempts {
			slog.Debug("loki: retrying", "path", path, "status", resp.StatusCode, "attempt", attempt)
			sleep(ctx, retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if readErr != nil {
			return nil, fmt.Errorf("loki /%s: reading response: %w", path, readErr)
		}
		if resp.StatusCode/100 != 2 {
			return nil, &apiError{Status: resp.StatusCode, Path: "/" + path, Message: errorText(body)}
		}
		return body, nil
	}
}

// data calls an enveloped endpoint and returns its "data" member.
func (c *conn) data(ctx context.Context, path string, params url.Values, postOK bool) (json.RawMessage, error) {
	body, err := c.raw(ctx, path, params, postOK)
	if err != nil {
		return nil, err
	}
	var env envelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, &apiError{Status: 200, Path: "/" + path, Message: "response is not JSON: " + errorText(body)}
	}
	if env.Status != "success" {
		return nil, &apiError{Status: 200, Path: "/" + path, Message: env.Error}
	}
	for _, w := range env.Warnings {
		slog.Warn("loki warning", "path", path, "warning", w)
	}
	return env.Data, nil
}

// apiPath is the Loki query API prefix.
const apiPath = "loki/api/v1/"

// nsParam renders a Unix-nanosecond time for start/end/time parameters.
func nsParam(ns int64) string { return strconv.FormatInt(ns, 10) }

// formatStep renders a step in milliseconds as seconds for the API.
func formatStep(ms int64) string {
	sec, frac := ms/1000, ms%1000
	if frac == 0 {
		return strconv.FormatInt(sec, 10)
	}
	return fmt.Sprintf("%d.%03d", sec, frac)
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
