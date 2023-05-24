// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

package tracer

import (
	"fmt"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/internal/log"
	"gopkg.in/DataDog/dd-trace-go.v1/internal/samplernames"
)

// HTTPHeadersCarrier wraps an http.Header as a TextMapWriter and TextMapReader, allowing
// it to be used using the provided Propagator implementation.
type HTTPHeadersCarrier http.Header

var _ TextMapWriter = (*HTTPHeadersCarrier)(nil)
var _ TextMapReader = (*HTTPHeadersCarrier)(nil)

// Set implements TextMapWriter.
func (c HTTPHeadersCarrier) Set(key, val string) {
	http.Header(c).Set(key, val)
}

// ForeachKey implements TextMapReader.
func (c HTTPHeadersCarrier) ForeachKey(handler func(key, val string) error) error {
	for k, vals := range c {
		for _, v := range vals {
			if err := handler(k, v); err != nil {
				return err
			}
		}
	}
	return nil
}

// TextMapCarrier allows the use of a regular map[string]string as both TextMapWriter
// and TextMapReader, making it compatible with the provided Propagator.
type TextMapCarrier map[string]string

var _ TextMapWriter = (*TextMapCarrier)(nil)
var _ TextMapReader = (*TextMapCarrier)(nil)

// Set implements TextMapWriter.
func (c TextMapCarrier) Set(key, val string) {
	c[key] = val
}

// ForeachKey conforms to the TextMapReader interface.
func (c TextMapCarrier) ForeachKey(handler func(key, val string) error) error {
	for k, v := range c {
		if err := handler(k, v); err != nil {
			return err
		}
	}
	return nil
}

const (
	headerPropagationStyleInject  = "DD_PROPAGATION_STYLE_INJECT"
	headerPropagationStyleExtract = "DD_PROPAGATION_STYLE_EXTRACT"
)

const (
	// DefaultBaggageHeaderPrefix specifies the prefix that will be used in
	// HTTP headers or text maps to prefix baggage keys.
	DefaultBaggageHeaderPrefix = "ot-baggage-"

	// DefaultTraceIDHeader specifies the key that will be used in HTTP headers
	// or text maps to store the trace ID.
	DefaultTraceIDHeader = "x-datadog-trace-id"

	// DefaultParentIDHeader specifies the key that will be used in HTTP headers
	// or text maps to store the parent ID.
	DefaultParentIDHeader = "x-datadog-parent-id"

	// DefaultPriorityHeader specifies the key that will be used in HTTP headers
	// or text maps to store the sampling priority value.
	DefaultPriorityHeader = "x-datadog-sampling-priority"
)

// originHeader specifies the name of the header indicating the origin of the trace.
// It is used with the Synthetics product and usually has the value "synthetics".
const originHeader = "x-datadog-origin"

// traceTagsHeader holds the propagated trace tags
const traceTagsHeader = "x-datadog-tags"

// PropagatorConfig defines the configuration for initializing a propagator.
type PropagatorConfig struct {
	// BaggagePrefix specifies the prefix that will be used to store baggage
	// items in a map. It defaults to DefaultBaggageHeaderPrefix.
	BaggagePrefix string

	// TraceHeader specifies the map key that will be used to store the trace ID.
	// It defaults to DefaultTraceIDHeader.
	TraceHeader string

	// ParentHeader specifies the map key that will be used to store the parent ID.
	// It defaults to DefaultParentIDHeader.
	ParentHeader string

	// PriorityHeader specifies the map key that will be used to store the sampling priority.
	// It defaults to DefaultPriorityHeader.
	PriorityHeader string

	// MaxTagsHeaderLen specifies the maximum length of trace tags header value.
	MaxTagsHeaderLen int

	// B3 specifies if B3 headers should be added for trace propagation.
	// See https://github.com/openzipkin/b3-propagation
	B3 bool
}

// NewPropagator returns a new propagator which uses TextMap to inject
// and extract values. It propagates trace and span IDs and baggage.
// To use the defaults, nil may be provided in place of the config.
func NewPropagator(cfg *PropagatorConfig) Propagator {
	if cfg == nil {
		cfg = new(PropagatorConfig)
	}
	if cfg.BaggagePrefix == "" {
		cfg.BaggagePrefix = DefaultBaggageHeaderPrefix
	}
	if cfg.TraceHeader == "" {
		cfg.TraceHeader = DefaultTraceIDHeader
	}
	if cfg.ParentHeader == "" {
		cfg.ParentHeader = DefaultParentIDHeader
	}
	if cfg.PriorityHeader == "" {
		cfg.PriorityHeader = DefaultPriorityHeader
	}
	return &chainedPropagator{
		injectors:  getPropagators(cfg, headerPropagationStyleInject),
		extractors: getPropagators(cfg, headerPropagationStyleExtract),
	}
}

// chainedPropagator implements Propagator and applies a list of injectors and extractors.
// When injecting, all injectors are called to propagate the span context.
// When extracting, it tries each extractor, selecting the first successful one.
type chainedPropagator struct {
	injectors  []Propagator
	extractors []Propagator
}

// getPropagators returns a list of propagators based on the list found in the
// given environment variable. If the list doesn't contain any valid values the
// default propagator will be returned. Any invalid values in the list will log
// a warning and be ignored.
func getPropagators(cfg *PropagatorConfig, env string) []Propagator {
	dd := &propagator{cfg}
	ps := os.Getenv(env)
	defaultPs := []Propagator{dd}
	if cfg.B3 {
		defaultPs = append(defaultPs, &propagatorB3{})
	}
	if ps == "" {
		return defaultPs
	}
	var list []Propagator
	if cfg.B3 {
		list = append(list, &propagatorB3{})
	}
	for _, v := range strings.Split(ps, ",") {
		switch strings.ToLower(v) {
		case "datadog":
			list = append(list, dd)
		case "b3":
			if !cfg.B3 {
				// propagatorB3 hasn't already been added, add a new one.
				list = append(list, &propagatorB3{})
			}
		default:
			log.Warn("unrecognized propagator: %s\n", v)
		}
	}
	if len(list) == 0 {
		// return the default
		return defaultPs
	}
	return list
}

// Inject defines the Propagator to propagate SpanContext data
// out of the current process. The implementation propagates the
// TraceID and the current active SpanID, as well as the Span baggage.
func (p *chainedPropagator) Inject(spanCtx ddtrace.SpanContext, carrier interface{}) error {
	for _, v := range p.injectors {
		err := v.Inject(spanCtx, carrier)
		if err != nil {
			return err
		}
	}
	return nil
}

// Extract implements Propagator.
func (p *chainedPropagator) Extract(carrier interface{}) (ddtrace.SpanContext, error) {
	for _, v := range p.extractors {
		ctx, err := v.Extract(carrier)
		if ctx != nil {
			// first extractor returns
			log.Debug("Extracted span context: %#v", ctx)
			return ctx, nil
		}
		if err == ErrSpanContextNotFound {
			continue
		}
		return nil, err
	}
	return nil, ErrSpanContextNotFound
}

// propagator implements Propagator and injects/extracts span contexts
// using datadog headers. Only TextMap carriers are supported.
type propagator struct {
	cfg *PropagatorConfig
}

func (p *propagator) Inject(spanCtx ddtrace.SpanContext, carrier interface{}) error {
	switch c := carrier.(type) {
	case TextMapWriter:
		return p.injectTextMap(spanCtx, c)
	default:
		return ErrInvalidCarrier
	}
}

func (p *propagator) injectTextMap(spanCtx ddtrace.SpanContext, writer TextMapWriter) error {
	ctx, ok := spanCtx.(*spanContext)
	if !ok || ctx.traceID == 0 || ctx.spanID == 0 {
		return ErrInvalidSpanContext
	}
	// propagate the TraceID and the current active SpanID
	writer.Set(p.cfg.TraceHeader, strconv.FormatUint(ctx.traceID, 10))
	writer.Set(p.cfg.ParentHeader, strconv.FormatUint(ctx.spanID, 10))
	if sp, ok := ctx.samplingPriority(); ok {
		writer.Set(p.cfg.PriorityHeader, strconv.Itoa(sp))
	}
	if ctx.origin != "" {
		writer.Set(originHeader, ctx.origin)
	}
	// propagate OpenTracing baggage
	for k, v := range ctx.baggage {
		writer.Set(p.cfg.BaggagePrefix+k, v)
	}
	return nil
}

func (p *propagator) Extract(carrier interface{}) (ddtrace.SpanContext, error) {
	switch c := carrier.(type) {
	case TextMapReader:
		return p.extractTextMap(c)
	default:
		return nil, ErrInvalidCarrier
	}
}

func (p *propagator) extractTextMap(reader TextMapReader) (ddtrace.SpanContext, error) {
	var ctx spanContext
	err := reader.ForeachKey(func(k, v string) error {
		var err error
		key := strings.ToLower(k)
		switch key {
		case p.cfg.TraceHeader:
			ctx.traceID, err = parseUint64(v)
			if err != nil {
				return ErrSpanContextCorrupted
			}
		case p.cfg.ParentHeader:
			ctx.spanID, err = parseUint64(v)
			if err != nil {
				return ErrSpanContextCorrupted
			}
		case p.cfg.PriorityHeader:
			priority, err := strconv.Atoi(v)
			if err != nil {
				return ErrSpanContextCorrupted
			}
			ctx.setSamplingPriority("", priority, samplernames.Upstream, math.NaN())
		case originHeader:
			ctx.origin = v
		case traceTagsHeader:
			if ctx.trace == nil {
				ctx.trace = newTrace()
			}
			ctx.trace.tags, err = parsePropagatableTraceTags(v)
			ctx.trace.upstreamServices = ctx.trace.tags[keyUpstreamServices]
			if err != nil {
				log.Warn("did not extract trace tags (err: %s)", err.Error())
			}
		default:
			if strings.HasPrefix(key, p.cfg.BaggagePrefix) {
				ctx.setBaggageItem(strings.TrimPrefix(key, p.cfg.BaggagePrefix), v)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if ctx.traceID == 0 || ctx.spanID == 0 {
		return nil, ErrSpanContextNotFound
	}
	return &ctx, nil
}

const (
	b3TraceIDHeader = "x-b3-traceid"
	b3SpanIDHeader  = "x-b3-spanid"
	b3SampledHeader = "x-b3-sampled"
)

// propagatorB3 implements Propagator and injects/extracts span contexts
// using B3 headers. Only TextMap carriers are supported.
type propagatorB3 struct{}

func (p *propagatorB3) Inject(spanCtx ddtrace.SpanContext, carrier interface{}) error {
	switch c := carrier.(type) {
	case TextMapWriter:
		return p.injectTextMap(spanCtx, c)
	default:
		return ErrInvalidCarrier
	}
}

func (*propagatorB3) injectTextMap(spanCtx ddtrace.SpanContext, writer TextMapWriter) error {
	ctx, ok := spanCtx.(*spanContext)
	if !ok || ctx.traceID == 0 || ctx.spanID == 0 {
		return ErrInvalidSpanContext
	}
	writer.Set(b3TraceIDHeader, fmt.Sprintf("%016x", ctx.traceID))
	writer.Set(b3SpanIDHeader, fmt.Sprintf("%016x", ctx.spanID))
	if p, ok := ctx.samplingPriority(); ok {
		if p >= ext.PriorityAutoKeep {
			writer.Set(b3SampledHeader, "1")
		} else {
			writer.Set(b3SampledHeader, "0")
		}
	}
	return nil
}

func (p *propagatorB3) Extract(carrier interface{}) (ddtrace.SpanContext, error) {
	switch c := carrier.(type) {
	case TextMapReader:
		return p.extractTextMap(c)
	default:
		return nil, ErrInvalidCarrier
	}
}

func (*propagatorB3) extractTextMap(reader TextMapReader) (ddtrace.SpanContext, error) {
	var ctx spanContext
	err := reader.ForeachKey(func(k, v string) error {
		var err error
		key := strings.ToLower(k)
		switch key {
		case b3TraceIDHeader:
			if len(v) > 16 {
				v = v[len(v)-16:]
			}
			ctx.traceID, err = strconv.ParseUint(v, 16, 64)
			if err != nil {
				return ErrSpanContextCorrupted
			}
		case b3SpanIDHeader:
			ctx.spanID, err = strconv.ParseUint(v, 16, 64)
			if err != nil {
				return ErrSpanContextCorrupted
			}
		case b3SampledHeader:
			priority, err := strconv.Atoi(v)
			if err != nil {
				return ErrSpanContextCorrupted
			}
			ctx.setSamplingPriority("", priority, samplernames.Upstream, math.NaN())
		default:
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if ctx.traceID == 0 || ctx.spanID == 0 {
		return nil, ErrSpanContextNotFound
	}
	return &ctx, nil
}
