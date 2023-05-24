// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

package tracer

import (
	"time"

	"golang.org/x/sys/windows"

	"gopkg.in/DataDog/dd-trace-go.v1/internal/log"
)

// This method is more precise than the go1.8 time.Now on Windows
// See https://msdn.microsoft.com/en-us/library/windows/desktop/hh706895(v=vs.85).aspx
// It is however ~10x slower and requires Windows 8+.
func highPrecisionNow() int64 {
	var ft windows.Filetime
	windows.GetSystemTimePreciseAsFileTime(&ft)
	return ft.Nanoseconds()
}

func lowPrecisionNow() int64 {
	return time.Now().UnixNano()
}

var now func() int64

// If GetSystemTimePreciseAsFileTime is not available we default to the less
// precise implementation based on time.Now()
func init() {
	if err := windows.LoadGetSystemTimePreciseAsFileTime(); err != nil {
		log.Warn("Unable to load high precison timer, defaulting to time.Now()")
		now = lowPrecisionNow
	} else {
		now = highPrecisionNow
	}
}
