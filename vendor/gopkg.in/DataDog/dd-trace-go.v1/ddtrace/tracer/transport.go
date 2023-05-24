// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

package tracer

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	traceinternal "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/internal"
	"gopkg.in/DataDog/dd-trace-go.v1/internal"
	"gopkg.in/DataDog/dd-trace-go.v1/internal/version"

	"github.com/tinylib/msgp/msgp"
)

const (
	// headerComputedTopLevel specifies that the client has marked top-level spans, when set.
	// Any non-empty value will mean 'yes'.
	headerComputedTopLevel = "Datadog-Client-Computed-Top-Level"
)

var defaultDialer = &net.Dialer{
	Timeout:   30 * time.Second,
	KeepAlive: 30 * time.Second,
	DualStack: true,
}

var defaultClient = &http.Client{
	// We copy the transport to avoid using the default one, as it might be
	// augmented with tracing and we don't want these calls to be recorded.
	// See https://golang.org/pkg/net/http/#DefaultTransport .
	Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           defaultDialer.DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	},
	Timeout: defaultHTTPTimeout,
}

const (
	defaultHostname    = "localhost"
	defaultPort        = "8126"
	defaultAddress     = defaultHostname + ":" + defaultPort
	defaultHTTPTimeout = 2 * time.Second         // defines the current timeout before giving up with the send process
	traceCountHeader   = "X-Datadog-Trace-Count" // header containing the number of traces in the payload
)

// transport is an interface for communicating data to the agent.
type transport interface {
	// send sends the payload p to the agent using the transport set up.
	// It returns a non-nil response body when no error occurred.
	send(p *payload) (body io.ReadCloser, err error)
	// sendStats sends the given stats payload to the agent.
	sendStats(s *statsPayload) error
	// endpoint returns the URL to which the transport will send traces.
	endpoint() string
}

type httpTransport struct {
	traceURL string            // the delivery URL for traces
	statsURL string            // the delivery URL for stats
	client   *http.Client      // the HTTP client used in the POST
	headers  map[string]string // the Transport headers
}

// newTransport returns a new Transport implementation that sends traces to a
// trace agent running on the given hostname and port, using a given
// *http.Client. If the zero values for hostname and port are provided,
// the default values will be used ("localhost" for hostname, and "8126" for
// port). If client is nil, a default is used.
//
// In general, using this method is only necessary if you have a trace agent
// running on a non-default port, if it's located on another machine, or when
// otherwise needing to customize the transport layer, for instance when using
// a unix domain socket.
func newHTTPTransport(addr string, client *http.Client) *httpTransport {
	// initialize the default EncoderPool with Encoder headers
	defaultHeaders := map[string]string{
		"Datadog-Meta-Lang":             "go",
		"Datadog-Meta-Lang-Version":     strings.TrimPrefix(runtime.Version(), "go"),
		"Datadog-Meta-Lang-Interpreter": runtime.Compiler + "-" + runtime.GOARCH + "-" + runtime.GOOS,
		"Datadog-Meta-Tracer-Version":   version.Tag,
		"Content-Type":                  "application/msgpack",
	}
	if cid := internal.ContainerID(); cid != "" {
		defaultHeaders["Datadog-Container-ID"] = cid
	}
	return &httpTransport{
		traceURL: fmt.Sprintf("http://%s/v0.4/traces", resolveAgentAddr(addr)),
		statsURL: fmt.Sprintf("http://%s/v0.6/stats", resolveAgentAddr(addr)),
		client:   client,
		headers:  defaultHeaders,
	}
}

func (t *httpTransport) sendStats(p *statsPayload) error {
	var buf bytes.Buffer
	if err := msgp.Encode(&buf, p); err != nil {
		return err
	}
	req, err := http.NewRequest("POST", t.statsURL, &buf)
	if err != nil {
		return err
	}
	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	if code := resp.StatusCode; code >= 400 {
		// error, check the body for context information and
		// return a nice error.
		msg := make([]byte, 1000)
		n, _ := resp.Body.Read(msg)
		resp.Body.Close()
		txt := http.StatusText(code)
		if n > 0 {
			return fmt.Errorf("%s (Status: %s)", msg[:n], txt)
		}
		return fmt.Errorf("%s", txt)
	}
	return nil
}

func (t *httpTransport) send(p *payload) (body io.ReadCloser, err error) {
	req, err := http.NewRequest("POST", t.traceURL, p)
	if err != nil {
		return nil, fmt.Errorf("cannot create http request: %v", err)
	}
	for header, value := range t.headers {
		req.Header.Set(header, value)
	}
	req.Header.Set(traceCountHeader, strconv.Itoa(p.itemCount()))
	req.Header.Set("Content-Length", strconv.Itoa(p.size()))
	req.Header.Set(headerComputedTopLevel, "yes")
	if t, ok := traceinternal.GetGlobalTracer().(*tracer); ok {
		if t.config.canComputeStats() {
			req.Header.Set("Datadog-Client-Computed-Stats", "yes")
		}
		droppedTraces := int(atomic.SwapUint64(&t.droppedP0Traces, 0))
		droppedSpans := int(atomic.SwapUint64(&t.droppedP0Spans, 0))
		if stats := t.config.statsd; stats != nil {
			stats.Count("datadog.tracer.dropped_p0_traces", int64(droppedTraces), nil, 1)
			stats.Count("datadog.tracer.dropped_p0_spans", int64(droppedSpans), nil, 1)
		}
		req.Header.Set("Datadog-Client-Dropped-P0-Traces", strconv.Itoa(droppedTraces))
		req.Header.Set("Datadog-Client-Dropped-P0-Spans", strconv.Itoa(droppedSpans))
	}
	response, err := t.client.Do(req)
	if err != nil {
		return nil, err
	}
	if code := response.StatusCode; code >= 400 {
		// error, check the body for context information and
		// return a nice error.
		msg := make([]byte, 1000)
		n, _ := response.Body.Read(msg)
		response.Body.Close()
		txt := http.StatusText(code)
		if n > 0 {
			return nil, fmt.Errorf("%s (Status: %s)", msg[:n], txt)
		}
		return nil, fmt.Errorf("%s", txt)
	}
	return response.Body, nil
}

func (t *httpTransport) endpoint() string {
	return t.traceURL
}

// resolveAgentAddr resolves the given agent address and fills in any missing host
// and port using the defaults. Some environment variable settings will
// take precedence over configuration.
func resolveAgentAddr(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		// no port in addr
		host = addr
	}
	if host == "" {
		host = defaultHostname
	}
	if port == "" {
		port = defaultPort
	}
	if v := os.Getenv("DD_AGENT_HOST"); v != "" {
		host = v
	}
	if v := os.Getenv("DD_TRACE_AGENT_PORT"); v != "" {
		port = v
	}
	return fmt.Sprintf("%s:%s", host, port)
}
