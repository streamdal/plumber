// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package pulsar

import (
	"fmt"
	"net/url"
	"time"

	"github.com/gogo/protobuf/proto"

	"github.com/sirupsen/logrus"

	"github.com/apache/pulsar-client-go/pulsar/internal"
	"github.com/apache/pulsar-client-go/pulsar/internal/auth"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
)

const (
	defaultConnectionTimeout = 5 * time.Second
	defaultOperationTimeout  = 30 * time.Second
)

type client struct {
	cnxPool       internal.ConnectionPool
	rpcClient     internal.RPCClient
	handlers      internal.ClientHandlers
	lookupService internal.LookupService
	metrics       *internal.Metrics

	log log.Logger
}

func newClient(options ClientOptions) (Client, error) {
	var logger log.Logger
	if options.Logger != nil {
		logger = options.Logger
	} else {
		logger = log.NewLoggerWithLogrus(logrus.StandardLogger())
	}

	if options.URL == "" {
		return nil, newError(InvalidConfiguration, "URL is required for client")
	}

	url, err := url.Parse(options.URL)
	if err != nil {
		logger.WithError(err).Error("Failed to parse service URL")
		return nil, newError(InvalidConfiguration, "Invalid service URL")
	}

	var tlsConfig *internal.TLSOptions
	switch url.Scheme {
	case "pulsar", "http":
		tlsConfig = nil
	case "pulsar+ssl", "https":
		tlsConfig = &internal.TLSOptions{
			AllowInsecureConnection: options.TLSAllowInsecureConnection,
			TrustCertsFilePath:      options.TLSTrustCertsFilePath,
			ValidateHostname:        options.TLSValidateHostname,
		}
	default:
		return nil, newError(InvalidConfiguration, fmt.Sprintf("Invalid URL scheme '%s'", url.Scheme))
	}

	var authProvider auth.Provider
	var ok bool

	if options.Authentication == nil {
		authProvider = auth.NewAuthDisabled()
	} else {
		authProvider, ok = options.Authentication.(auth.Provider)
		if !ok {
			return nil, newError(AuthenticationError, "invalid auth provider interface")
		}
	}
	err = authProvider.Init()
	if err != nil {
		return nil, err
	}

	connectionTimeout := options.ConnectionTimeout
	if connectionTimeout.Nanoseconds() == 0 {
		connectionTimeout = defaultConnectionTimeout
	}

	operationTimeout := options.OperationTimeout
	if operationTimeout.Nanoseconds() == 0 {
		operationTimeout = defaultOperationTimeout
	}

	maxConnectionsPerHost := options.MaxConnectionsPerBroker
	if maxConnectionsPerHost <= 0 {
		maxConnectionsPerHost = 1
	}

	var metrics *internal.Metrics
	if options.CustomMetricsLabels != nil {
		metrics = internal.NewMetricsProvider(options.CustomMetricsLabels)
	} else {
		metrics = internal.NewMetricsProvider(map[string]string{})
	}

	c := &client{
		cnxPool: internal.NewConnectionPool(tlsConfig, authProvider, connectionTimeout, maxConnectionsPerHost, logger,
			metrics),
		log:     logger,
		metrics: metrics,
	}
	serviceNameResolver := internal.NewPulsarServiceNameResolver(url)

	c.rpcClient = internal.NewRPCClient(url, serviceNameResolver, c.cnxPool, operationTimeout, logger, metrics)

	switch url.Scheme {
	case "pulsar", "pulsar+ssl":
		c.lookupService = internal.NewLookupService(c.rpcClient, url, serviceNameResolver,
			tlsConfig != nil, options.ListenerName, logger, metrics)
	case "http", "https":
		httpClient, err := internal.NewHTTPClient(url, serviceNameResolver, tlsConfig,
			operationTimeout, logger, metrics, authProvider)
		if err != nil {
			return nil, newError(InvalidConfiguration, fmt.Sprintf("Failed to init http client with err: '%s'",
				err.Error()))
		}
		c.lookupService = internal.NewHTTPLookupService(httpClient, url, serviceNameResolver,
			tlsConfig != nil, logger, metrics)
	default:
		return nil, newError(InvalidConfiguration, fmt.Sprintf("Invalid URL scheme '%s'", url.Scheme))
	}

	c.handlers = internal.NewClientHandlers()

	return c, nil
}

func (c *client) CreateProducer(options ProducerOptions) (Producer, error) {
	producer, err := newProducer(c, &options)
	if err == nil {
		c.handlers.Add(producer)
	}
	return producer, err
}

func (c *client) Subscribe(options ConsumerOptions) (Consumer, error) {
	consumer, err := newConsumer(c, options)
	if err != nil {
		return nil, err
	}
	c.handlers.Add(consumer)
	return consumer, nil
}

func (c *client) CreateReader(options ReaderOptions) (Reader, error) {
	reader, err := newReader(c, options)
	if err != nil {
		return nil, err
	}
	c.handlers.Add(reader)
	return reader, nil
}

func (c *client) TopicPartitions(topic string) ([]string, error) {
	topicName, err := internal.ParseTopicName(topic)
	if err != nil {
		return nil, err
	}

	r, err := c.lookupService.GetPartitionedTopicMetadata(topic)
	if err != nil {
		return nil, err
	}
	if r != nil {
		if r.Partitions > 0 {
			partitions := make([]string, r.Partitions)
			for i := 0; i < r.Partitions; i++ {
				partitions[i] = fmt.Sprintf("%s-partition-%d", topic, i)
			}
			return partitions, nil
		}
	}

	// Non-partitioned topic
	return []string{topicName.Name}, nil
}

func (c *client) Close() {
	c.handlers.Close()
	c.cnxPool.Close()
	c.lookupService.Close()
}

func (c *client) namespaceTopics(namespace string) ([]string, error) {
	id := c.rpcClient.NewRequestID()
	req := &pb.CommandGetTopicsOfNamespace{
		RequestId: proto.Uint64(id),
		Namespace: proto.String(namespace),
		Mode:      pb.CommandGetTopicsOfNamespace_PERSISTENT.Enum(),
	}
	res, err := c.rpcClient.RequestToAnyBroker(id, pb.BaseCommand_GET_TOPICS_OF_NAMESPACE, req)
	if err != nil {
		return nil, err
	}
	if res.Response.Error != nil {
		return []string{}, newError(LookupError, res.Response.GetError().String())
	}

	return res.Response.GetTopicsOfNamespaceResponse.GetTopics(), nil
}
