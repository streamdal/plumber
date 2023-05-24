// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

//go:build !windows
// +build !windows

package tracer

import "time"

// now returns the current UNIX time in nanoseconds, as computed by Time.UnixNano().
func now() int64 {
	return time.Now().UnixNano()
}
