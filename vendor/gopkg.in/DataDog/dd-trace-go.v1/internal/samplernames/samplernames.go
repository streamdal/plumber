// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

package samplernames

import "math"

// SamplerName specifies the name of a sampler which was
// responsible for a certain sampling decision.
type SamplerName int8

const (
	// Upstream specifies that the sampling decision was made in an upstream service
	// and no sampling needs to be done.
	Upstream SamplerName = math.MinInt8
	// Unknown specifies that the span was sampled
	// but, the tracer was unable to identify the sampler.
	Unknown SamplerName = -1
	// Default specifies that the span was sampled without any sampler.
	Default SamplerName = 0
	// AgentRate specifies that the span was sampled
	// with a rate calculated by the trace agent.
	AgentRate SamplerName = 1
	// RemoteRate specifies that the span was sampled
	// with a dynamically calculated remote rate.
	RemoteRate SamplerName = 2
	// RuleRate specifies that the span was sampled by the RuleSampler.
	RuleRate SamplerName = 3
	// Manual specifies that the span was sampled manually by user.
	Manual SamplerName = 4
	// AppSec specifies that the span was sampled by AppSec.
	AppSec SamplerName = 5
	// RemoteUserRate specifies that the span was sampled
	// with an user specified remote rate.
	RemoteUserRate SamplerName = 6
)
