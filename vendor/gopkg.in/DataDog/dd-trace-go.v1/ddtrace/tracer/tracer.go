// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

package tracer

import (
	gocontext "context"
	"os"
	"runtime/pprof"
	"strconv"
	"sync"
	"time"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/internal"
	"gopkg.in/DataDog/dd-trace-go.v1/internal/appsec"
	"gopkg.in/DataDog/dd-trace-go.v1/internal/log"
	"gopkg.in/DataDog/dd-trace-go.v1/internal/traceprof"

	"github.com/DataDog/datadog-agent/pkg/obfuscate"
)

var _ ddtrace.Tracer = (*tracer)(nil)

// tracer creates, buffers and submits Spans which are used to time blocks of
// computation. They are accumulated and streamed into an internal payload,
// which is flushed to the agent whenever its size exceeds a specific threshold
// or when a certain interval of time has passed, whichever happens first.
//
// tracer operates based on a worker loop which responds to various request
// channels. It additionally holds two buffers which accumulates error and trace
// queues to be processed by the payload encoder.
type tracer struct {
	config *config

	// stats specifies the concentrator used to compute statistics, when client-side
	// stats are enabled.
	stats *concentrator

	// traceWriter is responsible for sending finished traces to their
	// destination, such as the Trace Agent or Datadog Forwarder.
	traceWriter traceWriter

	// out receives traces to be added to the payload.
	out chan []*span

	// flush receives a channel onto which it will confirm after a flush has been
	// triggered and completed.
	flush chan chan<- struct{}

	// stop causes the tracer to shut down when closed.
	stop chan struct{}

	// stopOnce ensures the tracer is stopped exactly once.
	stopOnce sync.Once

	// wg waits for all goroutines to exit when stopping.
	wg sync.WaitGroup

	// prioritySampling holds an instance of the priority sampler.
	prioritySampling *prioritySampler

	// pid of the process
	pid string

	// These integers track metrics about spans and traces as they are started,
	// finished, and dropped
	spansStarted, spansFinished, tracesDropped int64

	// Records the number of dropped P0 traces and spans.
	droppedP0Traces, droppedP0Spans uint64

	// rulesSampling holds an instance of the rules sampler. These are user-defined
	// rules for applying a sampling rate to spans that match the designated service
	// or operation name.
	rulesSampling *rulesSampler

	// obfuscator holds the obfuscator used to obfuscate resources in aggregated stats.
	// obfuscator may be nil if disabled.
	obfuscator *obfuscate.Obfuscator
}

const (
	// flushInterval is the interval at which the payload contents will be flushed
	// to the transport.
	flushInterval = 2 * time.Second

	// payloadMaxLimit is the maximum payload size allowed and should indicate the
	// maximum size of the package that the agent can receive.
	payloadMaxLimit = 9.5 * 1024 * 1024 // 9.5 MB

	// payloadSizeLimit specifies the maximum allowed size of the payload before
	// it will trigger a flush to the transport.
	payloadSizeLimit = payloadMaxLimit / 2

	// concurrentConnectionLimit specifies the maximum number of concurrent outgoing
	// connections allowed.
	concurrentConnectionLimit = 100
)

// statsInterval is the interval at which health metrics will be sent with the
// statsd client; replaced in tests.
var statsInterval = 10 * time.Second

// Start starts the tracer with the given set of options. It will stop and replace
// any running tracer, meaning that calling it several times will result in a restart
// of the tracer by replacing the current instance with a new one.
func Start(opts ...StartOption) {
	if internal.Testing {
		return // mock tracer active
	}
	t := newTracer(opts...)
	if !t.config.enabled {
		return
	}
	internal.SetGlobalTracer(t)
	if t.config.logStartup {
		logStartup(t)
	}
}

// Stop stops the started tracer. Subsequent calls are valid but become no-op.
func Stop() {
	internal.SetGlobalTracer(&internal.NoopTracer{})
	log.Flush()
}

// Span is an alias for ddtrace.Span. It is here to allow godoc to group methods returning
// ddtrace.Span. It is recommended and is considered more correct to refer to this type as
// ddtrace.Span instead.
type Span = ddtrace.Span

// StartSpan starts a new span with the given operation name and set of options.
// If the tracer is not started, calling this function is a no-op.
func StartSpan(operationName string, opts ...StartSpanOption) Span {
	return internal.GetGlobalTracer().StartSpan(operationName, opts...)
}

// Extract extracts a SpanContext from the carrier. The carrier is expected
// to implement TextMapReader, otherwise an error is returned.
// If the tracer is not started, calling this function is a no-op.
func Extract(carrier interface{}) (ddtrace.SpanContext, error) {
	return internal.GetGlobalTracer().Extract(carrier)
}

// Inject injects the given SpanContext into the carrier. The carrier is
// expected to implement TextMapWriter, otherwise an error is returned.
// If the tracer is not started, calling this function is a no-op.
func Inject(ctx ddtrace.SpanContext, carrier interface{}) error {
	return internal.GetGlobalTracer().Inject(ctx, carrier)
}

// SetUser associates user information to the current trace which the
// provided span belongs to. The options can be used to tune which user
// bit of information gets monitored.
func SetUser(s Span, id string, opts ...UserMonitoringOption) {
	if s == nil {
		return
	}
	if span, ok := s.(*span); ok && span.context != nil {
		span = span.context.trace.root
		s = span
	}
	s.SetTag("usr.id", id)
	for _, fn := range opts {
		fn(s)
	}
}

// payloadQueueSize is the buffer size of the trace channel.
const payloadQueueSize = 1000

func newUnstartedTracer(opts ...StartOption) *tracer {
	c := newConfig(opts...)
	envRules, err := samplingRulesFromEnv()
	if err != nil {
		log.Warn("DIAGNOSTICS Error(s) parsing DD_TRACE_SAMPLING_RULES: %s", err)
	}
	if envRules != nil {
		c.samplingRules = envRules
	}
	sampler := newPrioritySampler()
	var writer traceWriter
	if c.logToStdout {
		writer = newLogTraceWriter(c)
	} else {
		writer = newAgentTraceWriter(c, sampler)
	}
	t := &tracer{
		config:           c,
		traceWriter:      writer,
		out:              make(chan []*span, payloadQueueSize),
		stop:             make(chan struct{}),
		flush:            make(chan chan<- struct{}),
		rulesSampling:    newRulesSampler(c.samplingRules),
		prioritySampling: sampler,
		pid:              strconv.Itoa(os.Getpid()),
		stats:            newConcentrator(c, defaultStatsBucketSize),
		obfuscator: obfuscate.NewObfuscator(obfuscate.Config{
			SQL: obfuscate.SQLConfig{
				TableNames:       c.agent.HasFlag("table_names"),
				ReplaceDigits:    c.agent.HasFlag("quantize_sql_tables") || c.agent.HasFlag("replace_sql_digits"),
				KeepSQLAlias:     c.agent.HasFlag("keep_sql_alias"),
				DollarQuotedFunc: c.agent.HasFlag("dollar_quoted_func"),
				Cache:            c.agent.HasFlag("sql_cache"),
			},
		}),
	}
	return t
}

func newTracer(opts ...StartOption) *tracer {
	t := newUnstartedTracer(opts...)
	c := t.config
	t.config.statsd.Incr("datadog.tracer.started", nil, 1)
	if c.runtimeMetrics {
		log.Debug("Runtime metrics enabled.")
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			t.reportRuntimeMetrics(defaultMetricsReportInterval)
		}()
	}
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		tick := t.config.tickChan
		if tick == nil {
			ticker := time.NewTicker(flushInterval)
			defer ticker.Stop()
			tick = ticker.C
		}
		t.worker(tick)
	}()
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		t.reportHealthMetrics(statsInterval)
	}()
	t.stats.Start()
	appsec.Start()
	return t
}

// Flush flushes any buffered traces. Flush is in effect only if a tracer
// is started. Users do not have to call Flush in order to ensure that
// traces reach Datadog. It is a convenience method dedicated to a specific
// use case described below.
//
// Flush is of use in Lambda environments, where starting and stopping
// the tracer on each invokation may create too much latency. In this
// scenario, a tracer may be started and stopped by the parent process
// whereas the invokation can make use of Flush to ensure any created spans
// reach the agent.
func Flush() {
	if t, ok := internal.GetGlobalTracer().(*tracer); ok {
		t.flushSync()
	}
}

// flushSync triggers a flush and waits for it to complete.
func (t *tracer) flushSync() {
	done := make(chan struct{})
	t.flush <- done
	<-done
}

// worker receives finished traces to be added into the payload, as well
// as periodically flushes traces to the transport.
func (t *tracer) worker(tick <-chan time.Time) {
	for {
		select {
		case trace := <-t.out:
			t.traceWriter.add(trace)

		case <-tick:
			t.config.statsd.Incr("datadog.tracer.flush_triggered", []string{"reason:scheduled"}, 1)
			t.traceWriter.flush()

		case done := <-t.flush:
			t.config.statsd.Incr("datadog.tracer.flush_triggered", []string{"reason:invoked"}, 1)
			t.traceWriter.flush()
			// TODO(x): In reality, the traceWriter.flush() call is not synchronous
			// when using the agent traceWriter. However, this functionnality is used
			// in Lambda so for that purpose this mechanism should suffice.
			done <- struct{}{}

		case <-t.stop:
		loop:
			// the loop ensures that the payload channel is fully drained
			// before the final flush to ensure no traces are lost (see #526)
			for {
				select {
				case trace := <-t.out:
					t.traceWriter.add(trace)
				default:
					break loop
				}
			}
			return
		}
	}
}

func (t *tracer) pushTrace(trace []*span) {
	select {
	case <-t.stop:
		return
	default:
	}
	select {
	case t.out <- trace:
	default:
		log.Error("payload queue full, dropping %d traces", len(trace))
	}
}

// StartSpan creates, starts, and returns a new Span with the given `operationName`.
func (t *tracer) StartSpan(operationName string, options ...ddtrace.StartSpanOption) ddtrace.Span {
	var opts ddtrace.StartSpanConfig
	for _, fn := range options {
		fn(&opts)
	}
	var startTime int64
	if opts.StartTime.IsZero() {
		startTime = now()
	} else {
		startTime = opts.StartTime.UnixNano()
	}
	var context *spanContext
	// The default pprof context is taken from the start options and is
	// not nil when using StartSpanFromContext()
	pprofContext := opts.Context
	if opts.Parent != nil {
		if ctx, ok := opts.Parent.(*spanContext); ok {
			context = ctx
			if pprofContext == nil && ctx.span != nil {
				// Inherit the context.Context from parent span if it was propagated
				// using ChildOf() rather than StartSpanFromContext(), see
				// applyPPROFLabels() below.
				pprofContext = ctx.span.pprofCtxActive
			}
		}
	}
	if pprofContext == nil {
		// For root span's without context, there is no pprofContext, but we need
		// one to avoid a panic() in pprof.WithLabels(). Using context.Background()
		// is not ideal here, as it will cause us to remove all labels from the
		// goroutine when the span finishes. However, the alternatives of not
		// applying labels for such spans or to leave the endpoint/hotspot labels
		// on the goroutine after it finishes are even less appealing. We'll have
		// to properly document this for users.
		pprofContext = gocontext.Background()
	}
	id := opts.SpanID
	if id == 0 {
		id = random.Uint64()
	}
	// span defaults
	span := &span{
		Name:         operationName,
		Service:      t.config.serviceName,
		Resource:     operationName,
		SpanID:       id,
		TraceID:      id,
		Start:        startTime,
		taskEnd:      startExecutionTracerTask(operationName),
		noDebugStack: t.config.noDebugStack,
	}
	if t.config.hostname != "" {
		span.setMeta(keyHostname, t.config.hostname)
	}
	if context != nil {
		// this is a child span
		span.TraceID = context.traceID
		span.ParentID = context.spanID
		if p, ok := context.samplingPriority(); ok {
			span.setMetric(keySamplingPriority, float64(p))
		}
		if context.span != nil {
			// local parent, inherit service
			context.span.RLock()
			span.Service = context.span.Service
			context.span.RUnlock()
		} else {
			// remote parent
			if context.origin != "" {
				// mark origin
				span.setMeta(keyOrigin, context.origin)
			}
		}
	}
	span.context = newSpanContext(span, context)
	if context == nil || context.span == nil {
		// this is either a root span or it has a remote parent, we should add the PID.
		span.setMeta(ext.Pid, t.pid)
		if _, ok := opts.Tags[ext.ServiceName]; !ok && t.config.runtimeMetrics {
			// this is a root span in the global service; runtime metrics should
			// be linked to it:
			span.setMeta("language", "go")
		}
	}
	// add tags from options
	for k, v := range opts.Tags {
		span.SetTag(k, v)
	}
	// add global tags
	for k, v := range t.config.globalTags {
		span.SetTag(k, v)
	}
	if t.config.serviceMappings != nil {
		if newSvc, ok := t.config.serviceMappings[span.Service]; ok {
			span.Service = newSvc
		}
	}
	if context == nil || context.span == nil || context.span.Service != span.Service {
		span.setMetric(keyTopLevel, 1)
		// all top level spans are measured. So the measured tag is redundant.
		delete(span.Metrics, keyMeasured)
	}
	if t.config.version != "" && span.Service == t.config.serviceName {
		span.setMeta(ext.Version, t.config.version)
	}
	if t.config.env != "" {
		span.setMeta(ext.Environment, t.config.env)
	}
	if _, ok := span.context.samplingPriority(); !ok {
		// if not already sampled or a brand new trace, sample it
		t.sample(span)
	}
	if t.config.profilerHotspots || t.config.profilerEndpoints {
		t.applyPPROFLabels(pprofContext, span)
	}
	if t.config.serviceMappings != nil {
		if newSvc, ok := t.config.serviceMappings[span.Service]; ok {
			span.Service = newSvc
		}
	}
	if log.DebugEnabled() {
		// avoid allocating the ...interface{} argument if debug logging is disabled
		log.Debug("Started Span: %v, Operation: %s, Resource: %s, Tags: %v, %v",
			span, span.Name, span.Resource, span.Meta, span.Metrics)
	}
	return span
}

// applyPPROFLabels applies pprof labels for the profiler's code hotspots and
// endpoint filtering feature to span. When span finishes, any pprof labels
// found in ctx are restored.
func (t *tracer) applyPPROFLabels(ctx gocontext.Context, span *span) {
	var labels []string
	if t.config.profilerHotspots {
		labels = append(labels, traceprof.SpanID, strconv.FormatUint(span.SpanID, 10))
	}
	// nil checks might not be needed, but better be safe than sorry
	if span.context.trace != nil && span.context.trace.root != nil {
		localRootSpan := span.context.trace.root
		if t.config.profilerHotspots {
			labels = append(labels, traceprof.LocalRootSpanID, strconv.FormatUint(localRootSpan.SpanID, 10))
		}
		if t.config.profilerEndpoints && spanResourcePIISafe(localRootSpan) {
			labels = append(labels, traceprof.TraceEndpoint, localRootSpan.Resource)
		}
	}
	if len(labels) > 0 {
		span.pprofCtxRestore = ctx
		span.pprofCtxActive = pprof.WithLabels(ctx, pprof.Labels(labels...))
		pprof.SetGoroutineLabels(span.pprofCtxActive)
	}
}

// spanResourcePIISafe returns true if s.Resource can be considered to not
// include PII with reasonable confidence. E.g. SQL queries may contain PII,
// but http, rpc or custom (s.Type == "") span resource names generally do not.
func spanResourcePIISafe(s *span) bool {
	return s.Type == ext.SpanTypeWeb || s.Type == ext.AppTypeRPC || s.Type == ""
}

// Stop stops the tracer.
func (t *tracer) Stop() {
	t.stopOnce.Do(func() {
		close(t.stop)
		t.config.statsd.Incr("datadog.tracer.stopped", nil, 1)
	})
	t.stats.Stop()
	t.wg.Wait()
	t.traceWriter.stop()
	t.config.statsd.Close()
	appsec.Stop()
}

// Inject uses the configured or default TextMap Propagator.
func (t *tracer) Inject(ctx ddtrace.SpanContext, carrier interface{}) error {
	return t.config.propagator.Inject(ctx, carrier)
}

// Extract uses the configured or default TextMap Propagator.
func (t *tracer) Extract(carrier interface{}) (ddtrace.SpanContext, error) {
	return t.config.propagator.Extract(carrier)
}

// sampleRateMetricKey is the metric key holding the applied sample rate. Has to be the same as the Agent.
const sampleRateMetricKey = "_sample_rate"

// Sample samples a span with the internal sampler.
func (t *tracer) sample(span *span) {
	if _, ok := span.context.samplingPriority(); ok {
		// sampling decision was already made
		return
	}
	sampler := t.config.sampler
	if !sampler.Sample(span) {
		span.context.trace.drop()
		return
	}
	if rs, ok := sampler.(RateSampler); ok && rs.Rate() < 1 {
		span.setMetric(sampleRateMetricKey, rs.Rate())
	}
	if t.rulesSampling.apply(span) {
		return
	}
	t.prioritySampling.apply(span)
}
