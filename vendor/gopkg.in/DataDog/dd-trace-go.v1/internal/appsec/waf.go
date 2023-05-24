// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

//go:build appsec
// +build appsec

package appsec

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/DataDog/dd-trace-go.v1/internal/appsec/dyngo"
	"gopkg.in/DataDog/dd-trace-go.v1/internal/appsec/dyngo/instrumentation/grpcsec"
	"gopkg.in/DataDog/dd-trace-go.v1/internal/appsec/dyngo/instrumentation/httpsec"
	"gopkg.in/DataDog/dd-trace-go.v1/internal/appsec/waf"
	"gopkg.in/DataDog/dd-trace-go.v1/internal/log"
)

// Register the WAF event listener.
func registerWAF(rules []byte, timeout time.Duration, limiter Limiter) (unreg dyngo.UnregisterFunc, err error) {
	// Check the WAF is healthy
	if _, err := waf.Health(); err != nil {
		return nil, err
	}

	// Instantiate the WAF
	waf, err := waf.NewHandle(rules)
	if err != nil {
		return nil, err
	}
	// Close the WAF in case of an error in what's following
	defer func() {
		if err != nil {
			waf.Close()
		}
	}()

	// Check if there are addresses in the rule
	ruleAddresses := waf.Addresses()
	if len(ruleAddresses) == 0 {
		return nil, errors.New("no addresses found in the rule")
	}
	// Check there are supported addresses in the rule
	httpAddresses, grpcAddresses, notSupported := supportedAddresses(ruleAddresses)
	if len(httpAddresses) == 0 && len(grpcAddresses) == 0 {
		return nil, fmt.Errorf("the addresses present in the rule are not supported: %v", notSupported)
	} else if len(notSupported) > 0 {
		log.Debug("appsec: the addresses present in the rule are partially supported: not supported=%v", notSupported)
	}

	// Register the WAF event listener
	var unregisterHTTP, unregisterGRPC dyngo.UnregisterFunc
	if len(httpAddresses) > 0 {
		log.Debug("appsec: registering http waf listening to addresses %v", httpAddresses)
		unregisterHTTP = dyngo.Register(newHTTPWAFEventListener(waf, httpAddresses, timeout, limiter))
	}
	if len(grpcAddresses) > 0 {
		log.Debug("appsec: registering grpc waf listening to addresses %v", grpcAddresses)
		unregisterGRPC = dyngo.Register(newGRPCWAFEventListener(waf, grpcAddresses, timeout, limiter))
	}

	// Return an unregistration function that will also release the WAF instance.
	return func() {
		defer waf.Close()
		if unregisterHTTP != nil {
			unregisterHTTP()
		}
		if unregisterGRPC != nil {
			unregisterGRPC()
		}
	}, nil
}

// newWAFEventListener returns the WAF event listener to register in order to enable it.
func newHTTPWAFEventListener(handle *waf.Handle, addresses []string, timeout time.Duration, limiter Limiter) dyngo.EventListener {
	return httpsec.OnHandlerOperationStart(func(op *httpsec.Operation, args httpsec.HandlerOperationArgs) {
		var body interface{}
		op.On(httpsec.OnSDKBodyOperationStart(func(op *httpsec.SDKBodyOperation, args httpsec.SDKBodyOperationArgs) {
			body = args.Body
		}))
		// At the moment, AppSec doesn't block the requests, and so we can use the fact we are in monitoring-only mode
		// to call the WAF only once at the end of the handler operation.
		op.On(httpsec.OnHandlerOperationFinish(func(op *httpsec.Operation, res httpsec.HandlerOperationRes) {
			wafCtx := waf.NewContext(handle)
			if wafCtx == nil {
				// The WAF event listener got concurrently released
				return
			}
			defer wafCtx.Close()

			// Run the WAF on the rule addresses available in the request args
			values := make(map[string]interface{}, len(addresses))
			for _, addr := range addresses {
				switch addr {
				case serverRequestRawURIAddr:
					values[serverRequestRawURIAddr] = args.RequestURI
				case serverRequestHeadersNoCookiesAddr:
					if headers := args.Headers; headers != nil {
						values[serverRequestHeadersNoCookiesAddr] = headers
					}
				case serverRequestCookiesAddr:
					if cookies := args.Cookies; cookies != nil {
						values[serverRequestCookiesAddr] = cookies
					}
				case serverRequestQueryAddr:
					if query := args.Query; query != nil {
						values[serverRequestQueryAddr] = query
					}
				case serverRequestPathParams:
					if pathParams := args.PathParams; pathParams != nil {
						values[serverRequestPathParams] = pathParams
					}
				case serverRequestBody:
					if body != nil {
						values[serverRequestBody] = body
					}
				case serverResponseStatusAddr:
					values[serverResponseStatusAddr] = res.Status
				}
			}
			matches := runWAF(wafCtx, values, timeout)

			// Log the attacks if any
			if len(matches) == 0 {
				return
			}
			log.Debug("appsec: attack detected by the waf")
			if limiter.Allow() {
				op.AddSecurityEvent(matches)
			}
		}))

	})
}

// newGRPCWAFEventListener returns the WAF event listener to register in order
// to enable it.
func newGRPCWAFEventListener(handle *waf.Handle, _ []string, timeout time.Duration, limiter Limiter) dyngo.EventListener {
	return grpcsec.OnHandlerOperationStart(func(op *grpcsec.HandlerOperation, handlerArgs grpcsec.HandlerOperationArgs) {
		// Limit the maximum number of security events, as a streaming RPC could
		// receive unlimited number of messages where we could find security events
		const maxWAFEventsPerRequest = 10
		var (
			nbEvents uint32
			logOnce  sync.Once

			events []json.RawMessage
			mu     sync.Mutex
		)
		op.On(grpcsec.OnReceiveOperationFinish(func(_ grpcsec.ReceiveOperation, res grpcsec.ReceiveOperationRes) {
			if atomic.LoadUint32(&nbEvents) == maxWAFEventsPerRequest {
				logOnce.Do(func() {
					log.Debug("appsec: ignoring the rpc message due to the maximum number of security events per grpc call reached")
				})
				return
			}
			// The current workaround of the WAF context limitations is to
			// simply instantiate and release the WAF context for the operation
			// lifetime so that:
			//   1. We avoid growing the memory usage of the context every time
			//      a grpc.server.request.message value is added to it during
			//      the RPC lifetime.
			//   2. We avoid the limitation of 1 event per attack type.
			// TODO(Julio-Guerra): a future libddwaf API should solve this out.
			wafCtx := waf.NewContext(handle)
			if wafCtx == nil {
				// The WAF event listener got concurrently released
				return
			}
			defer wafCtx.Close()
			// Run the WAF on the rule addresses available in the args
			// Note that we don't check if the address is present in the rules
			// as we only support one at the moment, so this callback cannot be
			// set when the address is not present.
			values := map[string]interface{}{grpcServerRequestMessage: res.Message}
			if md := handlerArgs.Metadata; len(md) > 0 {
				values[grpcServerRequestMetadata] = md
			}
			event := runWAF(wafCtx, values, timeout)
			if len(event) == 0 {
				return
			}
			log.Debug("appsec: attack detected by the grpc waf")
			atomic.AddUint32(&nbEvents, 1)
			mu.Lock()
			events = append(events, event)
			mu.Unlock()
		}))
		op.On(grpcsec.OnHandlerOperationFinish(func(op *grpcsec.HandlerOperation, _ grpcsec.HandlerOperationRes) {
			if len(events) > 0 && limiter.Allow() {
				op.AddSecurityEvent(events)
			}
		}))
	})
}

func runWAF(wafCtx *waf.Context, values map[string]interface{}, timeout time.Duration) []byte {
	matches, err := wafCtx.Run(values, timeout)
	if err != nil {
		if err == waf.ErrTimeout {
			log.Debug("appsec: waf timeout value of %s reached", timeout)
		} else {
			log.Error("appsec: unexpected waf error: %v", err)
			return nil
		}
	}
	return matches
}

// HTTP rule addresses currently supported by the WAF
const (
	serverRequestRawURIAddr           = "server.request.uri.raw"
	serverRequestHeadersNoCookiesAddr = "server.request.headers.no_cookies"
	serverRequestCookiesAddr          = "server.request.cookies"
	serverRequestQueryAddr            = "server.request.query"
	serverRequestPathParams           = "server.request.path_params"
	serverRequestBody                 = "server.request.body"
	serverResponseStatusAddr          = "server.response.status"
)

// List of HTTP rule addresses currently supported by the WAF
var httpAddresses = []string{
	serverRequestRawURIAddr,
	serverRequestHeadersNoCookiesAddr,
	serverRequestCookiesAddr,
	serverRequestQueryAddr,
	serverRequestPathParams,
	serverRequestBody,
	serverResponseStatusAddr,
}

// gRPC rule addresses currently supported by the WAF
const (
	grpcServerRequestMessage  = "grpc.server.request.message"
	grpcServerRequestMetadata = "grpc.server.request.metadata"
)

// List of gRPC rule addresses currently supported by the WAF
var grpcAddresses = []string{
	grpcServerRequestMessage,
	grpcServerRequestMetadata,
}

func init() {
	// sort the address lists to avoid mistakes and use sort.SearchStrings()
	sort.Strings(httpAddresses)
	sort.Strings(grpcAddresses)
}

// supportedAddresses returns the list of addresses we actually support from the
// given rule addresses.
func supportedAddresses(ruleAddresses []string) (supportedHTTP, supportedGRPC, notSupported []string) {
	// Filter the supported addresses only
	for _, addr := range ruleAddresses {
		if i := sort.SearchStrings(httpAddresses, addr); i < len(httpAddresses) && httpAddresses[i] == addr {
			supportedHTTP = append(supportedHTTP, addr)
		} else if i := sort.SearchStrings(grpcAddresses, addr); i < len(grpcAddresses) && grpcAddresses[i] == addr {
			supportedGRPC = append(supportedGRPC, addr)
		} else {
			notSupported = append(notSupported, addr)
		}
	}
	return
}
