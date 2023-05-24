// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

package grpcsec

import (
	"encoding/json"
	"fmt"
	"net"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/internal/appsec/dyngo/instrumentation/httpsec"
	"gopkg.in/DataDog/dd-trace-go.v1/internal/log"
	"gopkg.in/DataDog/dd-trace-go.v1/internal/samplernames"
)

// SetSecurityEventTags sets the AppSec-specific span tags when a security event
// occurred into the service entry span.
func SetSecurityEventTags(span ddtrace.Span, events []json.RawMessage, addr net.Addr, md map[string][]string) {
	if err := setSecurityEventTags(span, events, addr, md); err != nil {
		log.Error("appsec: %v", err)
	}
}

func setSecurityEventTags(span ddtrace.Span, events []json.RawMessage, addr net.Addr, md map[string][]string) error {
	if err := setEventSpanTags(span, events); err != nil {
		return err
	}
	var ip string
	switch actual := addr.(type) {
	case *net.UDPAddr:
		ip = actual.IP.String()
	case *net.TCPAddr:
		ip = actual.IP.String()
	}
	if ip != "" {
		span.SetTag("network.client.ip", ip)
	}
	for h, v := range httpsec.NormalizeHTTPHeaders(md) {
		span.SetTag("grpc.metadata."+h, v)
	}
	return nil
}

// setEventSpanTags sets the security event span tags into the service entry span.
func setEventSpanTags(span ddtrace.Span, events []json.RawMessage) error {
	// Set the appsec event span tag
	val, err := makeEventTagValue(events)
	if err != nil {
		return err
	}
	span.SetTag("_dd.appsec.json", string(val))
	// Keep this span due to the security event
	//
	// This is a workaround to tell the tracer that the trace was kept by AppSec.
	// Passing any other value than `appsec.SamplerAppSec` has no effect.
	// Customers should use `span.SetTag(ext.ManualKeep, true)` pattern
	// to keep the trace, manually.
	span.SetTag(ext.ManualKeep, samplernames.AppSec)
	span.SetTag("_dd.origin", "appsec")
	// Set the appsec.event tag needed by the appsec backend
	span.SetTag("appsec.event", true)
	return nil
}

// Create the value of the security event tag.
// TODO(Julio-Guerra): a future libddwaf version should return something
//   avoiding us the following events concatenation logic which currently
//   involves unserializing the top-level JSON arrays to concatenate them
//   together.
// TODO(Julio-Guerra): avoid serializing the json in the request hot path
func makeEventTagValue(events []json.RawMessage) (json.RawMessage, error) {
	var v interface{}
	if l := len(events); l == 1 {
		// eventTag is the structure to use in the `_dd.appsec.json` span tag.
		// In this case of 1 event, it already is an array as expected.
		type eventTag struct {
			Triggers json.RawMessage `json:"triggers"`
		}
		v = eventTag{Triggers: events[0]}
	} else {
		// eventTag is the structure to use in the `_dd.appsec.json` span tag.
		// With more than one event, we need to concatenate the arrays together
		// (ie. convert [][]json.RawMessage into []json.RawMessage).
		type eventTag struct {
			Triggers []json.RawMessage `json:"triggers"`
		}
		concatenated := make([]json.RawMessage, 0, l) // at least len(events)
		for _, event := range events {
			// Unmarshal the top level array
			var tmp []json.RawMessage
			if err := json.Unmarshal(event, &tmp); err != nil {
				return nil, fmt.Errorf("unexpected error while unserializing the appsec event `%s`: %v", string(event), err)
			}
			concatenated = append(concatenated, tmp...)
		}
		v = eventTag{Triggers: concatenated}
	}

	tag, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("unexpected error while serializing the appsec event span tag: %v", err)
	}
	return tag, nil
}
