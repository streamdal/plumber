// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

//go:build appsec
// +build appsec

package appsec

import (
	"sync"

	"gopkg.in/DataDog/dd-trace-go.v1/internal/appsec/dyngo"
	"gopkg.in/DataDog/dd-trace-go.v1/internal/log"
)

// Enabled returns true when AppSec is up and running. Meaning that the appsec build tag is enabled, the env var
// DD_APPSEC_ENABLED is set to true, and the tracer is started.
func Enabled() bool {
	mu.RLock()
	defer mu.RUnlock()
	return activeAppSec != nil
}

// Start AppSec when enabled is enabled by both using the appsec build tag and
// setting the environment variable DD_APPSEC_ENABLED to true.
func Start() {
	enabled, err := isEnabled()
	if err != nil {
		logUnexpectedStartError(err)
		return
	}
	if !enabled {
		log.Debug("appsec: disabled by the configuration: set the environment variable DD_APPSEC_ENABLED to true to enable it")
		return
	}

	cfg, err := newConfig()
	if err != nil {
		logUnexpectedStartError(err)
		return
	}
	appsec := newAppSec(cfg)
	if err := appsec.start(); err != nil {
		logUnexpectedStartError(err)
		return
	}
	setActiveAppSec(appsec)
}

// Implement the AppSec log message C1
func logUnexpectedStartError(err error) {
	log.Error("appsec: could not start because of an unexpected error: %v\nNo security activities will be collected. Please contact support at https://docs.datadoghq.com/help/ for help.", err)
}

// Stop AppSec.
func Stop() {
	setActiveAppSec(nil)
}

var (
	activeAppSec *appsec
	mu           sync.RWMutex
)

func setActiveAppSec(a *appsec) {
	mu.Lock()
	defer mu.Unlock()
	if activeAppSec != nil {
		activeAppSec.stop()
	}
	activeAppSec = a
}

type appsec struct {
	cfg           *config
	unregisterWAF dyngo.UnregisterFunc
	limiter       *TokenTicker
}

func newAppSec(cfg *config) *appsec {
	return &appsec{
		cfg: cfg,
	}
}

// Start AppSec by registering its security protections according to the configured the security rules.
func (a *appsec) start() error {
	// Register the WAF operation event listener
	a.limiter = NewTokenTicker(int64(a.cfg.traceRateLimit), int64(a.cfg.traceRateLimit))
	a.limiter.Start()
	unregisterWAF, err := registerWAF(a.cfg.rules, a.cfg.wafTimeout, a.limiter)
	if err != nil {
		return err
	}
	a.unregisterWAF = unregisterWAF
	return nil
}

// Stop AppSec by unregistering the security protections.
func (a *appsec) stop() {
	a.unregisterWAF()
	a.limiter.Stop()
}
