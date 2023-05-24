// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

package tracer

import (
	"math"
	"strconv"
	"sync"
	"sync/atomic"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/internal"
	"gopkg.in/DataDog/dd-trace-go.v1/internal/log"
	"gopkg.in/DataDog/dd-trace-go.v1/internal/samplernames"
)

var _ ddtrace.SpanContext = (*spanContext)(nil)

// SpanContext represents a span state that can propagate to descendant spans
// and across process boundaries. It contains all the information needed to
// spawn a direct descendant of the span that it belongs to. It can be used
// to create distributed tracing by propagating it using the provided interfaces.
type spanContext struct {
	// the below group should propagate only locally

	trace  *trace // reference to the trace that this span belongs too
	span   *span  // reference to the span that hosts this context
	errors int64  // number of spans with errors in this trace

	// the below group should propagate cross-process

	traceID uint64
	spanID  uint64

	mu         sync.RWMutex // guards below fields
	baggage    map[string]string
	hasBaggage int32  // atomic int for quick checking presence of baggage. 0 indicates no baggage, otherwise baggage exists.
	origin     string // e.g. "synthetics"
}

// newSpanContext creates a new SpanContext to serve as context for the given
// span. If the provided parent is not nil, the context will inherit the trace,
// baggage and other values from it. This method also pushes the span into the
// new context's trace and as a result, it should not be called multiple times
// for the same span.
func newSpanContext(span *span, parent *spanContext) *spanContext {
	context := &spanContext{
		traceID: span.TraceID,
		spanID:  span.SpanID,
		span:    span,
	}
	if parent != nil {
		context.trace = parent.trace
		context.origin = parent.origin
		context.errors = parent.errors
		parent.ForeachBaggageItem(func(k, v string) bool {
			context.setBaggageItem(k, v)
			return true
		})
	}
	if context.trace == nil {
		context.trace = newTrace()
	}
	if context.trace.root == nil {
		// first span in the trace can safely be assumed to be the root
		context.trace.root = span
	}
	// put span in context's trace
	context.trace.push(span)
	return context
}

// SpanID implements ddtrace.SpanContext.
func (c *spanContext) SpanID() uint64 { return c.spanID }

// TraceID implements ddtrace.SpanContext.
func (c *spanContext) TraceID() uint64 { return c.traceID }

// ForeachBaggageItem implements ddtrace.SpanContext.
func (c *spanContext) ForeachBaggageItem(handler func(k, v string) bool) {
	if atomic.LoadInt32(&c.hasBaggage) == 0 {
		return
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	for k, v := range c.baggage {
		if !handler(k, v) {
			break
		}
	}
}

func (c *spanContext) setSamplingPriority(service string, p int, sampler samplernames.SamplerName, rate float64) {
	if c.trace == nil {
		c.trace = newTrace()
	}
	c.trace.setSamplingPriority(service, p, sampler, rate)
}

func (c *spanContext) samplingPriority() (p int, ok bool) {
	if c.trace == nil {
		return 0, false
	}
	return c.trace.samplingPriority()
}

func (c *spanContext) setBaggageItem(key, val string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.baggage == nil {
		atomic.StoreInt32(&c.hasBaggage, 1)
		c.baggage = make(map[string]string, 1)
	}
	c.baggage[key] = val
}

func (c *spanContext) baggageItem(key string) string {
	if atomic.LoadInt32(&c.hasBaggage) == 0 {
		return ""
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.baggage[key]
}

// finish marks this span as finished in the trace.
func (c *spanContext) finish() { c.trace.finishedOne(c.span) }

// samplingDecision is the decision to send a trace to the agent or not.
type samplingDecision int64

const (
	// decisionNone is the default state of a trace.
	// If no decision is made about the trace, the trace won't be sent to the agent.
	decisionNone samplingDecision = iota
	// decisionDrop prevents the trace from being sent to the agent.
	decisionDrop
	// decisionKeep ensures the trace will be sent to the agent.
	decisionKeep
)

// trace contains shared context information about a trace, such as sampling
// priority, the root reference and a buffer of the spans which are part of the
// trace, if these exist.
type trace struct {
	mu               sync.RWMutex      // guards below fields
	spans            []*span           // all the spans that are part of this trace
	tags             map[string]string // trace level tags
	upstreamServices string            // _dd.p.upstream_services value from the upstream service
	finished         int               // the number of finished spans
	full             bool              // signifies that the span buffer is full
	priority         *float64          // sampling priority
	locked           bool              // specifies if the sampling priority can be altered
	samplingDecision samplingDecision  // samplingDecision indicates whether to send the trace to the agent.

	// root specifies the root of the trace, if known; it is nil when a span
	// context is extracted from a carrier, at which point there are no spans in
	// the trace yet.
	root *span
}

var (
	// traceStartSize is the initial size of our trace buffer,
	// by default we allocate for a handful of spans within the trace,
	// reasonable as span is actually way bigger, and avoids re-allocating
	// over and over. Could be fine-tuned at runtime.
	traceStartSize = 10
	// traceMaxSize is the maximum number of spans we keep in memory for a
	// single trace. This is to avoid memory leaks. If more spans than this
	// are added to a trace, then the trace is dropped and the spans are
	// discarded. Adding additional spans after a trace is dropped does
	// nothing.
	traceMaxSize = int(1e5)
)

// newTrace creates a new trace using the given callback which will be called
// upon completion of the trace.
func newTrace() *trace {
	return &trace{spans: make([]*span, 0, traceStartSize)}
}

func (t *trace) samplingPriorityLocked() (p int, ok bool) {
	if t.priority == nil {
		return 0, false
	}
	return int(*t.priority), true
}

func (t *trace) samplingPriority() (p int, ok bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.samplingPriorityLocked()
}

func (t *trace) setSamplingPriority(service string, p int, sampler samplernames.SamplerName, rate float64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.setSamplingPriorityLocked(service, p, sampler, rate)
}

func (t *trace) keep() {
	atomic.CompareAndSwapInt64((*int64)(&t.samplingDecision), int64(decisionNone), int64(decisionKeep))
}

func (t *trace) drop() {
	atomic.CompareAndSwapInt64((*int64)(&t.samplingDecision), int64(decisionNone), int64(decisionDrop))
}

func (t *trace) setTag(key, value string) {
	if t.tags == nil {
		t.tags = make(map[string]string, 1)
	}
	t.tags[key] = value
}

func (t *trace) setSamplingPriorityLocked(service string, p int, sampler samplernames.SamplerName, rate float64) {
	if t.locked {
		return
	}
	if t.root == nil {
		// this trace is distributed (no local root); modifications
		// to the sampling priority are not allowed.
		t.locked = true
	}
	if t.priority == nil {
		t.priority = new(float64)
	}
	*t.priority = float64(p)
	if sampler != samplernames.Upstream {
		if t.upstreamServices != "" {
			t.setTag(keyUpstreamServices, t.upstreamServices+";"+compactUpstreamServices(service, p, sampler, rate))
		} else {
			t.setTag(keyUpstreamServices, compactUpstreamServices(service, p, sampler, rate))
		}
	}
}

// push pushes a new span into the trace. If the buffer is full, it returns
// a errBufferFull error.
func (t *trace) push(sp *span) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.full {
		return
	}
	tr, haveTracer := internal.GetGlobalTracer().(*tracer)
	if len(t.spans) >= traceMaxSize {
		// capacity is reached, we will not be able to complete this trace.
		t.full = true
		t.spans = nil // GC
		log.Error("trace buffer full (%d), dropping trace", traceMaxSize)
		if haveTracer {
			atomic.AddInt64(&tr.tracesDropped, 1)
		}
		return
	}
	if v, ok := sp.Metrics[keySamplingPriority]; ok {
		t.setSamplingPriorityLocked(sp.Service, int(v), samplernames.Upstream, math.NaN())
	}
	t.spans = append(t.spans, sp)
	if haveTracer {
		atomic.AddInt64(&tr.spansStarted, 1)
	}
}

// finishedOne aknowledges that another span in the trace has finished, and checks
// if the trace is complete, in which case it calls the onFinish function. It uses
// the given priority, if non-nil, to mark the root span.
func (t *trace) finishedOne(s *span) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.full {
		// capacity has been reached, the buffer is no longer tracking
		// all the spans in the trace, so the below conditions will not
		// be accurate and would trigger a pre-mature flush, exposing us
		// to a race condition where spans can be modified while flushing.
		return
	}
	t.finished++
	if s == t.root && t.priority != nil {
		// after the root has finished we lock down the priority;
		// we won't be able to make changes to a span after finishing
		// without causing a race condition.
		t.root.setMetric(keySamplingPriority, *t.priority)
		t.locked = true
	}
	if len(t.spans) > 0 && s == t.spans[0] {
		// first span in chunk finished, lock down the tags
		//
		// TODO(barbayar): make sure this doesn't happen in vain when switching to
		// the new wire format. We won't need to set the tags on the first span
		// in the chunk there.
		for k, v := range t.tags {
			s.setMeta(k, v)
		}
	}
	if len(t.spans) != t.finished {
		return
	}
	defer func() {
		t.spans = nil
		t.finished = 0 // important, because a buffer can be used for several flushes
	}()
	tr, ok := internal.GetGlobalTracer().(*tracer)
	if !ok {
		return
	}
	// we have a tracer that can receive completed traces.
	atomic.AddInt64(&tr.spansFinished, int64(len(t.spans)))
	sd := samplingDecision(atomic.LoadInt64((*int64)(&t.samplingDecision)))
	if sd != decisionKeep {
		if p, ok := t.samplingPriorityLocked(); ok && p == ext.PriorityAutoReject {
			atomic.AddUint64(&tr.droppedP0Spans, uint64(len(t.spans)))
			atomic.AddUint64(&tr.droppedP0Traces, 1)
		}
		return
	}
	tr.pushTrace(t.spans)
}

func compactUpstreamServices(service string, priority int, sampler samplernames.SamplerName, rate float64) string {
	sb64 := b64Encode(service)
	p := strconv.Itoa(priority)
	s := strconv.Itoa(int(sampler))
	r := ""
	if !math.IsNaN(rate) {
		r = strconv.FormatFloat(rate, 'f', 4, 64)
	}
	return sb64 + "|" + p + "|" + s + "|" + r
}
