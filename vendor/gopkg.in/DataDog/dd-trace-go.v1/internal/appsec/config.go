// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

package appsec

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"
	"unicode"
	"unicode/utf8"

	"gopkg.in/DataDog/dd-trace-go.v1/internal/log"
)

const (
	enabledEnvVar        = "DD_APPSEC_ENABLED"
	rulesEnvVar          = "DD_APPSEC_RULES"
	wafTimeoutEnvVar     = "DD_APPSEC_WAF_TIMEOUT"
	traceRateLimitEnvVar = "DD_APPSEC_TRACE_RATE_LIMIT"
)

const (
	defaultWAFTimeout      = 4 * time.Millisecond
	defaultTraceRate  uint = 100 // up to 100 appsec traces/s
)

// config is the AppSec configuration.
type config struct {
	// rules loaded via the env var DD_APPSEC_RULES. When not set, the builtin rules will be used.
	rules []byte
	// Maximum WAF execution time
	wafTimeout time.Duration
	// AppSec trace rate limit (traces per second).
	traceRateLimit uint
}

// isEnabled returns true when appsec is enabled when the environment variable
// DD_APPSEC_ENABLED is set to true.
func isEnabled() (bool, error) {
	enabledStr := os.Getenv(enabledEnvVar)
	if enabledStr == "" {
		return false, nil
	}
	enabled, err := strconv.ParseBool(enabledStr)
	if err != nil {
		return false, fmt.Errorf("could not parse %s value `%s` as a boolean value", enabledEnvVar, enabledStr)
	}
	return enabled, nil
}

func newConfig() (*config, error) {
	rules, err := readRulesConfig()
	if err != nil {
		return nil, err
	}
	return &config{
		rules:          rules,
		wafTimeout:     readWAFTimeoutConfig(),
		traceRateLimit: readRateLimitConfig(),
	}, nil
}

func readWAFTimeoutConfig() (timeout time.Duration) {
	timeout = defaultWAFTimeout
	value := os.Getenv(wafTimeoutEnvVar)
	if value == "" {
		return
	}

	// Check if the value ends with a letter, which means the user has
	// specified their own time duration unit(s) such as 1s200ms.
	// Otherwise, default to microseconds.
	if lastRune, _ := utf8.DecodeLastRuneInString(value); !unicode.IsLetter(lastRune) {
		value += "us" // Add the default microsecond time-duration suffix
	}

	parsed, err := time.ParseDuration(value)
	if err != nil {
		logEnvVarParsingError(wafTimeoutEnvVar, value, err, timeout)
		return
	}
	if parsed <= 0 {
		logUnexpectedEnvVarValue(wafTimeoutEnvVar, parsed, "expecting a strictly positive duration", timeout)
		return
	}
	return parsed
}

func readRateLimitConfig() (rate uint) {
	rate = defaultTraceRate
	value := os.Getenv(traceRateLimitEnvVar)
	if value == "" {
		return rate
	}
	parsed, err := strconv.ParseUint(value, 10, 0)
	if err != nil {
		logEnvVarParsingError(traceRateLimitEnvVar, value, err, rate)
		return
	}
	if rate == 0 {
		logUnexpectedEnvVarValue(traceRateLimitEnvVar, parsed, "expecting a value strictly greater than 0", rate)
		return
	}
	return uint(parsed)
}

func readRulesConfig() (rules []byte, err error) {
	rules = []byte(staticRecommendedRule)
	filepath := os.Getenv(rulesEnvVar)
	if filepath == "" {
		log.Info("appsec: starting with the default recommended security rules")
		return rules, nil
	}
	buf, err := ioutil.ReadFile(filepath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Error("appsec: could not find the rules file in path %s: %v.", filepath, err)
		}
		return nil, err
	}
	log.Info("appsec: starting with the security rules from file %s", filepath)
	return buf, nil
}

func logEnvVarParsingError(name, value string, err error, defaultValue interface{}) {
	log.Error("appsec: could not parse the env var %s=%s as a duration: %v. Using default value %v.", name, value, err, defaultValue)
}

func logUnexpectedEnvVarValue(name string, value interface{}, reason string, defaultValue interface{}) {
	log.Error("appsec: unexpected configuration value of %s=%v: %s. Using default value %v.", name, value, reason, defaultValue)
}
