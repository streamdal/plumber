// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

// Build when CGO is disabled or the target OS or Arch are not supported
//go:build !appsec || !cgo || windows || !amd64
// +build !appsec !cgo windows !amd64

package waf

import (
	"errors"
	"time"
)

type (
	// Version of the WAF.
	Version struct{}
	// Handle represents an instance of the WAF for a given ruleset.
	Handle struct{}
	// Context is a WAF execution context.
	Context struct{}
)

var errDisabledReason = errors.New(disabledReason)

// String returns the string representation of the version.
func (*Version) String() string { return "" }

// Health allows knowing if the WAF can be used. It returns the current WAF
// version and a nil error when the WAF library is healthy. Otherwise, it
// returns a nil version and an error describing the issue.
func Health() (*Version, error) {
	return nil, errDisabledReason
}

// NewHandle creates a new instance of the WAF with the given JSON rule.
func NewHandle([]byte) (*Handle, error) { return nil, errDisabledReason }

// Addresses returns the list of addresses the WAF rule is expecting.
func (*Handle) Addresses() []string { return nil }

// Close the WAF and release the underlying C memory as soon as there are
// no more WAF contexts using the rule.
func (*Handle) Close() {}

// NewContext a new WAF context and increase the number of references to the WAF
// handle. A nil value is returned when the WAF handle can no longer be used
// or the WAF context couldn't be created.
func NewContext(*Handle) *Context { return nil }

// Run the WAF with the given Go values and timeout.
func (*Context) Run(map[string]interface{}, time.Duration) ([]byte, error) {
	return nil, errDisabledReason
}

// Close the WAF context by releasing its C memory and decreasing the number of
// references to the WAF handle.
func (*Context) Close() {}
