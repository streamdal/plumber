package servicebus

//	MIT License
//
//	Copyright (c) Microsoft Corporation. All rights reserved.
//
//	Permission is hereby granted, free of charge, to any person obtaining a copy
//	of this software and associated documentation files (the "Software"), to deal
//	in the Software without restriction, including without limitation the rights
//	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//	copies of the Software, and to permit persons to whom the Software is
//	furnished to do so, subject to the following conditions:
//
//	The above copyright notice and this permission notice shall be included in all
//	copies or substantial portions of the Software.
//
//	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//	SOFTWARE

import (
	"context"
	"crypto/tls"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-amqp-common-go/v3/aad"
	"github.com/Azure/azure-amqp-common-go/v3/auth"
	"github.com/Azure/azure-amqp-common-go/v3/cbs"
	"github.com/Azure/azure-amqp-common-go/v3/conn"
	"github.com/Azure/azure-amqp-common-go/v3/sas"
	"github.com/Azure/go-amqp"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/devigned/tab"
	"nhooyr.io/websocket"
)

const (
	//	banner = `
	//   _____                 _               ____
	//  / ___/___  ______   __(_)________     / __ )__  _______
	//  \__ \/ _ \/ ___/ | / / // ___/ _ \   / __  / / / / ___/
	// ___/ /  __/ /   | |/ / // /__/  __/  / /_/ / /_/ (__  )
	///____/\___/_/    |___/_/ \___/\___/  /_____/\__,_/____/
	//`

	// Version is the semantic version number
	Version = "0.10.16"

	rootUserAgent = "/golang-service-bus"

	// default ServiceBus resource uri to authenticate with
	serviceBusResourceURI = "https://servicebus.azure.net/"
)

type (
	// Namespace provides a simplified facade over the AMQP implementation of Azure Service Bus and is the entry point
	// for using Queues, Topics and Subscriptions
	Namespace struct {
		Name          string
		Suffix        string
		TokenProvider auth.TokenProvider
		Environment   azure.Environment
		tlsConfig     *tls.Config
		userAgent     string
		useWebSocket  bool
		// used to ensure only one goroutine is running for auth auto-refresh
		initRefresh sync.Once
		// populated with the result from auto-refresh, to be called elsewhere
		cancelRefresh func() <-chan struct{}

		// for testing

		// alias for 'amqp.Dial'
		amqpDial func(addr string, opts ...amqp.ConnOption) (*amqp.Client, error)
	}

	// NamespaceOption provides structure for configuring a new Service Bus namespace
	NamespaceOption func(h *Namespace) error
)

// NamespaceWithConnectionString configures a namespace with the information provided in a Service Bus connection string
func NamespaceWithConnectionString(connStr string) NamespaceOption {
	return func(ns *Namespace) error {
		parsed, err := conn.ParsedConnectionFromStr(connStr)
		if err != nil {
			return err
		}

		if parsed.Namespace != "" {
			ns.Name = parsed.Namespace
		}

		if parsed.Suffix != "" {
			ns.Suffix = parsed.Suffix
		}

		provider, err := sas.NewTokenProvider(sas.TokenProviderWithKey(parsed.KeyName, parsed.Key))
		if err != nil {
			return err
		}

		ns.TokenProvider = provider
		return nil
	}
}

// NamespaceWithTLSConfig appends to the TLS config.
func NamespaceWithTLSConfig(tlsConfig *tls.Config) NamespaceOption {
	return func(ns *Namespace) error {
		ns.tlsConfig = tlsConfig
		return nil
	}
}

// NamespaceWithUserAgent appends to the root user-agent value.
func NamespaceWithUserAgent(userAgent string) NamespaceOption {
	return func(ns *Namespace) error {
		ns.userAgent = userAgent
		return nil
	}
}

// NamespaceWithWebSocket configures the namespace and all entities to use wss:// rather than amqps://
func NamespaceWithWebSocket() NamespaceOption {
	return func(ns *Namespace) error {
		ns.useWebSocket = true
		return nil
	}
}

// NamespaceWithEnvironmentBinding configures a namespace using the environment details. It uses one of the following methods:
//
// 1. Client Credentials: attempt to authenticate with a Service Principal via "AZURE_TENANT_ID", "AZURE_CLIENT_ID" and
//    "AZURE_CLIENT_SECRET"
//
// 2. Client Certificate: attempt to authenticate with a Service Principal via "AZURE_TENANT_ID", "AZURE_CLIENT_ID",
//    "AZURE_CERTIFICATE_PATH" and "AZURE_CERTIFICATE_PASSWORD"
//
// 3. Managed Identity (MI): attempt to authenticate via the MI assigned to the Azure resource
//
//
// The Azure Environment used can be specified using the name of the Azure Environment set in "AZURE_ENVIRONMENT" var.
func NamespaceWithEnvironmentBinding(name string) NamespaceOption {
	return func(ns *Namespace) error {
		provider, err := aad.NewJWTProvider(
			aad.JWTProviderWithEnvironmentVars(),
			// TODO: fix bug upstream to use environment resourceURI
			aad.JWTProviderWithResourceURI(ns.getResourceURI()),
		)
		if err != nil {
			return err
		}

		ns.TokenProvider = provider
		ns.Name = name
		return nil
	}
}

// NamespaceWithAzureEnvironment sets the namespace's Environment, Suffix and ResourceURI parameters according
// to the Azure Environment defined in "github.com/Azure/go-autorest/autorest/azure" package.
// This allows to configure the library to be used in the different Azure clouds.
// environmentName is the name of the cloud as defined in autorest : https://github.com/Azure/go-autorest/blob/b076c1437d051bf4c328db428b70f4fe22ad38b0/autorest/azure/environments.go#L34-L39
func NamespaceWithAzureEnvironment(namespaceName, environmentName string) NamespaceOption {
	return func(ns *Namespace) error {
		azureEnv, err := azure.EnvironmentFromName(environmentName)
		if err != nil {
			return err
		}
		ns.Environment = azureEnv
		ns.Suffix = azureEnv.ServiceBusEndpointSuffix
		ns.Name = namespaceName
		return nil
	}
}

// NamespaceWithTokenProvider sets the token provider on the namespace
func NamespaceWithTokenProvider(provider auth.TokenProvider) NamespaceOption {
	return func(ns *Namespace) error {
		ns.TokenProvider = provider
		return nil
	}
}

// NewNamespace creates a new namespace configured through NamespaceOption(s)
func NewNamespace(opts ...NamespaceOption) (*Namespace, error) {
	ns := &Namespace{
		Environment: azure.PublicCloud,
		amqpDial:    amqp.Dial,
	}

	for _, opt := range opts {
		err := opt(ns)
		if err != nil {
			return nil, err
		}
	}

	return ns, nil
}

func (ns *Namespace) newClient(ctx context.Context) (*amqp.Client, error) {
	ctx, span := ns.startSpanFromContext(ctx, "sb.namespace.newClient")
	defer span.End()
	defaultConnOptions := []amqp.ConnOption{
		amqp.ConnSASLAnonymous(),
		amqp.ConnMaxSessions(65535),
		amqp.ConnProperty("product", "MSGolangClient"),
		amqp.ConnProperty("version", Version),
		amqp.ConnProperty("platform", runtime.GOOS),
		amqp.ConnProperty("framework", runtime.Version()),
		amqp.ConnProperty("user-agent", ns.getUserAgent()),
	}

	if ns.tlsConfig != nil {
		defaultConnOptions = append(
			defaultConnOptions,
			amqp.ConnTLS(true),
			amqp.ConnTLSConfig(ns.tlsConfig),
		)
	}

	if ns.useWebSocket {
		wssHost := ns.getWSSHostURI() + "$servicebus/websocket"
		opts := &websocket.DialOptions{Subprotocols: []string{"amqp"}}
		wssConn, _, err := websocket.Dial(ctx, wssHost, opts)

		if err != nil {
			return nil, err
		}
		nConn := websocket.NetConn(context.Background(), wssConn, websocket.MessageBinary)

		return amqp.New(nConn, append(defaultConnOptions, amqp.ConnServerHostname(ns.getHostname()))...)
	}

	return ns.amqpDial(ns.getAMQPHostURI(), defaultConnOptions...)
}

// negotiateClaim performs initial authentication and starts periodic refresh of credentials.
// the returned func is to cancel() the refresh goroutine.
func (ns *Namespace) negotiateClaim(ctx context.Context, client *amqp.Client, entityPath string) (func() <-chan struct{}, error) {
	ctx, span := ns.startSpanFromContext(ctx, "sb.namespace.negotiateClaim")
	defer span.End()

	audience := ns.getEntityAudience(entityPath)
	if err := cbs.NegotiateClaim(ctx, audience, client, ns.TokenProvider); err != nil {
		return nil, err
	}
	ns.initRefresh.Do(func() {
		// start the periodic refresh of credentials
		refreshCtx, cancel := context.WithCancel(context.Background())
		exitChan := make(chan struct{})
		go func() {
			defer func() {
				// reset the guard when the refresh goroutine exits
				ns.initRefresh = sync.Once{}
				// signal that the refresh goroutine has exited
				close(exitChan)
			}()
			for {
				select {
				case <-refreshCtx.Done():
					return
				case <-time.After(15 * time.Minute):
					refreshCtx, span := ns.startSpanFromContext(refreshCtx, "sb.namespace.negotiateClaim.refresh")
					defer span.End()
					// refresh credentials until it succeeds
					for {
						err := cbs.NegotiateClaim(refreshCtx, audience, client, ns.TokenProvider)
						if err == nil {
							break
						}
						// the refresh failed, wait a few seconds then try again
						tab.For(refreshCtx).Error(err)
						select {
						case <-refreshCtx.Done():
							return
						case <-time.After(5 * time.Second):
							// retry
						}
					}
				}
			}
		}()
		ns.cancelRefresh = func() <-chan struct{} {
			cancel()
			return exitChan
		}
	})
	return ns.cancelRefresh, nil
}

func (ns *Namespace) getWSSHostURI() string {
	suffix := ns.resolveSuffix()
	if strings.HasSuffix(suffix, "onebox.windows-int.net") {
		return fmt.Sprintf("wss://%s:4446/", ns.getHostname())
	}
	return fmt.Sprintf("wss://%s/", ns.getHostname())
}

func (ns *Namespace) getAMQPHostURI() string {
	return fmt.Sprintf("amqps://%s/", ns.getHostname())
}

func (ns *Namespace) getHTTPSHostURI() string {
	suffix := ns.resolveSuffix()
	if strings.HasSuffix(suffix, "onebox.windows-int.net") {
		return fmt.Sprintf("https://%s:4446/", ns.getHostname())
	}
	return fmt.Sprintf("https://%s/", ns.getHostname())
}

func (ns *Namespace) getHostname() string {
	return strings.Join([]string{ns.Name, ns.resolveSuffix()}, ".")
}

func (ns *Namespace) getEntityAudience(entityPath string) string {
	return ns.getAMQPHostURI() + entityPath
}

func (ns *Namespace) getUserAgent() string {
	userAgent := rootUserAgent
	if ns.userAgent != "" {
		userAgent = fmt.Sprintf("%s/%s", userAgent, ns.userAgent)
	}
	return userAgent
}

func (ns *Namespace) getResourceURI() string {
	if ns.Environment.ResourceIdentifiers.ServiceBus == "" {
		return serviceBusResourceURI
	}
	return ns.Environment.ResourceIdentifiers.ServiceBus
}

func (ns *Namespace) resolveSuffix() string {
	if ns.Suffix != "" {
		return ns.Suffix
	}
	return azure.PublicCloud.ServiceBusEndpointSuffix
}
