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

package internal

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"

	"github.com/apache/pulsar-client-go/pulsar/internal/auth"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"

	ua "go.uber.org/atomic"
)

const (
	// TODO: Find a better way to embed the version in the library code
	PulsarVersion       = "0.1"
	ClientVersionString = "Pulsar Go " + PulsarVersion

	PulsarProtocolVersion = int32(pb.ProtocolVersion_v13)
)

type TLSOptions struct {
	TrustCertsFilePath      string
	AllowInsecureConnection bool
	ValidateHostname        bool
}

// ConnectionListener is a user of a connection (eg. a producer or
// a consumer) that can register itself to get notified
// when the connection is closed.
type ConnectionListener interface {
	// ReceivedSendReceipt receive and process the return value of the send command.
	ReceivedSendReceipt(response *pb.CommandSendReceipt)

	// ConnectionClosed close the TCP connection.
	ConnectionClosed()
}

// Connection is a interface of client cnx.
type Connection interface {
	SendRequest(requestID uint64, req *pb.BaseCommand, callback func(*pb.BaseCommand, error))
	SendRequestNoWait(req *pb.BaseCommand) error
	WriteData(data Buffer)
	RegisterListener(id uint64, listener ConnectionListener)
	UnregisterListener(id uint64)
	AddConsumeHandler(id uint64, handler ConsumerHandler)
	DeleteConsumeHandler(id uint64)
	ID() string
	GetMaxMessageSize() int32
	Close()
}

type ConsumerHandler interface {
	MessageReceived(response *pb.CommandMessage, headersAndPayload Buffer) error

	// ConnectionClosed close the TCP connection.
	ConnectionClosed()
}

type connectionState int32

const (
	connectionInit = iota
	connectionReady
	connectionClosed
)

func (s connectionState) String() string {
	switch s {
	case connectionInit:
		return "Initializing"
	case connectionReady:
		return "Ready"
	case connectionClosed:
		return "Closed"
	default:
		return "Unknown"
	}
}

const keepAliveInterval = 30 * time.Second

type request struct {
	id       *uint64
	cmd      *pb.BaseCommand
	callback func(command *pb.BaseCommand, err error)
}

type incomingCmd struct {
	cmd               *pb.BaseCommand
	headersAndPayload Buffer
}

type connection struct {
	sync.Mutex
	cond              *sync.Cond
	state             ua.Int32
	connectionTimeout time.Duration
	closeOnce         sync.Once

	logicalAddr  *url.URL
	physicalAddr *url.URL
	cnx          net.Conn

	writeBufferLock sync.Mutex
	writeBuffer     Buffer
	reader          *connectionReader

	lastDataReceivedLock sync.Mutex
	lastDataReceivedTime time.Time
	pingTicker           *time.Ticker
	pingCheckTicker      *time.Ticker

	log log.Logger

	requestIDGenerator uint64

	incomingRequestsCh chan *request
	incomingCmdCh      chan *incomingCmd
	closeCh            chan interface{}
	writeRequestsCh    chan Buffer

	pendingLock sync.Mutex
	pendingReqs map[uint64]*request
	listeners   map[uint64]ConnectionListener

	consumerHandlersLock sync.RWMutex
	consumerHandlers     map[uint64]ConsumerHandler

	tlsOptions *TLSOptions
	auth       auth.Provider

	maxMessageSize int32
	metrics        *Metrics
}

// connectionOptions defines configurations for creating connection.
type connectionOptions struct {
	logicalAddr       *url.URL
	physicalAddr      *url.URL
	tls               *TLSOptions
	connectionTimeout time.Duration
	auth              auth.Provider
	logger            log.Logger
	metrics           *Metrics
}

func newConnection(opts connectionOptions) *connection {
	cnx := &connection{
		connectionTimeout:    opts.connectionTimeout,
		logicalAddr:          opts.logicalAddr,
		physicalAddr:         opts.physicalAddr,
		writeBuffer:          NewBuffer(4096),
		log:                  opts.logger.SubLogger(log.Fields{"remote_addr": opts.physicalAddr}),
		pendingReqs:          make(map[uint64]*request),
		lastDataReceivedTime: time.Now(),
		pingTicker:           time.NewTicker(keepAliveInterval),
		pingCheckTicker:      time.NewTicker(keepAliveInterval),
		tlsOptions:           opts.tls,
		auth:                 opts.auth,

		closeCh:            make(chan interface{}),
		incomingRequestsCh: make(chan *request, 10),
		incomingCmdCh:      make(chan *incomingCmd, 10),

		// This channel is used to pass data from producers to the connection
		// go routine. It can become contended or blocking if we have multiple
		// partition produces writing on a single connection. In general it's
		// good to keep this above the number of partition producers assigned
		// to a single connection.
		writeRequestsCh:  make(chan Buffer, 256),
		listeners:        make(map[uint64]ConnectionListener),
		consumerHandlers: make(map[uint64]ConsumerHandler),
		metrics:          opts.metrics,
	}
	cnx.setState(connectionInit)
	cnx.reader = newConnectionReader(cnx)
	cnx.cond = sync.NewCond(cnx)
	return cnx
}

func (c *connection) start() {
	// Each connection gets its own goroutine that will
	go func() {
		if c.connect() {
			if c.doHandshake() {
				c.metrics.ConnectionsOpened.Inc()
				c.run()
			} else {
				c.metrics.ConnectionsHandshakeErrors.Inc()
				c.changeState(connectionClosed)
			}
		} else {
			c.metrics.ConnectionsEstablishmentErrors.Inc()
			c.changeState(connectionClosed)
		}
	}()
}

func (c *connection) connect() bool {
	c.log.Info("Connecting to broker")

	var (
		err       error
		cnx       net.Conn
		tlsConfig *tls.Config
	)

	if c.tlsOptions == nil {
		// Clear text connection
		cnx, err = net.DialTimeout("tcp", c.physicalAddr.Host, c.connectionTimeout)
	} else {
		// TLS connection
		tlsConfig, err = c.getTLSConfig()
		if err != nil {
			c.log.WithError(err).Warn("Failed to configure TLS ")
			return false
		}

		d := &net.Dialer{Timeout: c.connectionTimeout}
		cnx, err = tls.DialWithDialer(d, "tcp", c.physicalAddr.Host, tlsConfig)
	}

	if err != nil {
		c.log.WithError(err).Warn("Failed to connect to broker.")
		c.Close()
		return false
	}

	c.Lock()
	c.cnx = cnx
	c.log = c.log.SubLogger(log.Fields{"local_addr": c.cnx.LocalAddr()})
	c.log.Info("TCP connection established")
	c.Unlock()

	return true
}

func (c *connection) doHandshake() bool {
	// Send 'Connect' command to initiate handshake
	authData, err := c.auth.GetData()
	if err != nil {
		c.log.WithError(err).Warn("Failed to load auth credentials")
		return false
	}

	// During the initial handshake, the internal keep alive is not
	// active yet, so we need to timeout write and read requests
	c.cnx.SetDeadline(time.Now().Add(keepAliveInterval))
	cmdConnect := &pb.CommandConnect{
		ProtocolVersion: proto.Int32(PulsarProtocolVersion),
		ClientVersion:   proto.String(ClientVersionString),
		AuthMethodName:  proto.String(c.auth.Name()),
		AuthData:        authData,
		FeatureFlags: &pb.FeatureFlags{
			SupportsAuthRefresh: proto.Bool(true),
		},
	}

	if c.logicalAddr.Host != c.physicalAddr.Host {
		cmdConnect.ProxyToBrokerUrl = proto.String(c.logicalAddr.Host)
	}
	c.writeCommand(baseCommand(pb.BaseCommand_CONNECT, cmdConnect))
	cmd, _, err := c.reader.readSingleCommand()
	if err != nil {
		c.log.WithError(err).Warn("Failed to perform initial handshake")
		return false
	}

	// Reset the deadline so that we don't use read timeouts
	c.cnx.SetDeadline(time.Time{})

	if cmd.Connected == nil {
		c.log.Warnf("Failed to establish connection with broker: '%s'",
			cmd.Error.GetMessage())
		return false
	}
	if cmd.Connected.MaxMessageSize != nil && *cmd.Connected.MaxMessageSize > 0 {
		c.log.Debug("Got MaxMessageSize from handshake response:", *cmd.Connected.MaxMessageSize)
		c.maxMessageSize = *cmd.Connected.MaxMessageSize
	} else {
		c.log.Debug("No MaxMessageSize from handshake response, use default: ", MaxMessageSize)
		c.maxMessageSize = MaxMessageSize
	}
	c.log.Info("Connection is ready")
	c.changeState(connectionReady)
	return true
}

func (c *connection) waitUntilReady() error {
	c.Lock()
	defer c.Unlock()

	for c.getState() != connectionReady {
		c.log.Debugf("Wait until connection is ready. State: %s", c.getState().String())
		if c.getState() == connectionClosed {
			return errors.New("connection error")
		}
		// wait for a new connection state change
		c.cond.Wait()
	}

	return nil
}

func (c *connection) failLeftRequestsWhenClose() {
	reqLen := len(c.incomingRequestsCh)
	for i := 0; i < reqLen; i++ {
		c.internalSendRequest(<-c.incomingRequestsCh)
	}
	close(c.incomingRequestsCh)
}

func (c *connection) run() {
	// All reads come from the reader goroutine
	go c.reader.readFromConnection()
	go c.runPingCheck()

	c.log.Debugf("Connection run start channel %+v, requestLength %d", c, len(c.incomingRequestsCh))

	defer func() {
		// all the accesses to the pendingReqs should be happened in this run loop thread,
		// including the final cleanup, to avoid the issue https://github.com/apache/pulsar-client-go/issues/239
		c.pendingLock.Lock()
		for id, req := range c.pendingReqs {
			req.callback(nil, errors.New("connection closed"))
			delete(c.pendingReqs, id)
		}
		c.pendingLock.Unlock()
		c.Close()
	}()

	go func() {
		for {
			select {
			case <-c.closeCh:
				c.failLeftRequestsWhenClose()
				return

			case req := <-c.incomingRequestsCh:
				if req == nil {
					return // TODO: this never gonna be happen
				}
				c.internalSendRequest(req)
			}
		}
	}()

	for {
		select {
		case <-c.closeCh:
			return

		case cmd := <-c.incomingCmdCh:
			c.internalReceivedCommand(cmd.cmd, cmd.headersAndPayload)

		case data := <-c.writeRequestsCh:
			if data == nil {
				return
			}
			c.internalWriteData(data)

		case <-c.pingTicker.C:
			c.sendPing()
		}
	}
}

func (c *connection) runPingCheck() {
	for {
		select {
		case <-c.closeCh:
			return
		case <-c.pingCheckTicker.C:
			if c.lastDataReceived().Add(2 * keepAliveInterval).Before(time.Now()) {
				// We have not received a response to the previous Ping request, the
				// connection to broker is stale
				c.log.Warn("Detected stale connection to broker")
				c.TriggerClose()
				return
			}
		}
	}
}

func (c *connection) WriteData(data Buffer) {
	select {
	case c.writeRequestsCh <- data:
		// Channel is not full
		return

	default:
		// Channel full, fallback to probe if connection is closed
	}

	for {
		select {
		case c.writeRequestsCh <- data:
			// Successfully wrote on the channel
			return

		case <-time.After(100 * time.Millisecond):
			// The channel is either:
			// 1. blocked, in which case we need to wait until we have space
			// 2. the connection is already closed, then we need to bail out
			c.log.Debug("Couldn't write on connection channel immediately")
			state := c.getState()
			if state != connectionReady {
				c.log.Debug("Connection was already closed")
				return
			}
		}
	}

}

func (c *connection) internalWriteData(data Buffer) {
	c.log.Debug("Write data: ", data.ReadableBytes())
	if _, err := c.cnx.Write(data.ReadableSlice()); err != nil {
		c.log.WithError(err).Warn("Failed to write on connection")
		c.TriggerClose()
	}
}

func (c *connection) writeCommand(cmd *pb.BaseCommand) {
	// Wire format
	// [FRAME_SIZE] [CMD_SIZE][CMD]
	cmdSize := uint32(cmd.Size())
	frameSize := cmdSize + 4

	c.writeBufferLock.Lock()
	defer c.writeBufferLock.Unlock()

	c.writeBuffer.Clear()
	c.writeBuffer.WriteUint32(frameSize)

	c.writeBuffer.WriteUint32(cmdSize)
	c.writeBuffer.ResizeIfNeeded(cmdSize)
	_, err := cmd.MarshalToSizedBuffer(c.writeBuffer.WritableSlice()[:cmdSize])
	if err != nil {
		c.log.WithError(err).Error("Protobuf serialization error")
		panic("Protobuf serialization error")
	}

	c.writeBuffer.WrittenBytes(cmdSize)
	c.internalWriteData(c.writeBuffer)
}

func (c *connection) receivedCommand(cmd *pb.BaseCommand, headersAndPayload Buffer) {
	c.incomingCmdCh <- &incomingCmd{cmd, headersAndPayload}
}

func (c *connection) internalReceivedCommand(cmd *pb.BaseCommand, headersAndPayload Buffer) {
	c.log.Debugf("Received command: %s -- payload: %v", cmd, headersAndPayload)
	c.setLastDataReceived(time.Now())

	switch *cmd.Type {
	case pb.BaseCommand_SUCCESS:
		c.handleResponse(cmd.Success.GetRequestId(), cmd)

	case pb.BaseCommand_PRODUCER_SUCCESS:
		c.handleResponse(cmd.ProducerSuccess.GetRequestId(), cmd)

	case pb.BaseCommand_PARTITIONED_METADATA_RESPONSE:
		c.handleResponse(cmd.PartitionMetadataResponse.GetRequestId(), cmd)

	case pb.BaseCommand_LOOKUP_RESPONSE:
		lookupResult := cmd.LookupTopicResponse
		c.handleResponse(lookupResult.GetRequestId(), cmd)

	case pb.BaseCommand_CONSUMER_STATS_RESPONSE:
		c.handleResponse(cmd.ConsumerStatsResponse.GetRequestId(), cmd)

	case pb.BaseCommand_GET_LAST_MESSAGE_ID_RESPONSE:
		c.handleResponse(cmd.GetLastMessageIdResponse.GetRequestId(), cmd)

	case pb.BaseCommand_GET_TOPICS_OF_NAMESPACE_RESPONSE:
		c.handleResponse(cmd.GetTopicsOfNamespaceResponse.GetRequestId(), cmd)

	case pb.BaseCommand_GET_SCHEMA_RESPONSE:
		c.handleResponse(cmd.GetSchemaResponse.GetRequestId(), cmd)

	case pb.BaseCommand_ERROR:
		c.handleResponseError(cmd.GetError())

	case pb.BaseCommand_CLOSE_PRODUCER:
		c.handleCloseProducer(cmd.GetCloseProducer())

	case pb.BaseCommand_CLOSE_CONSUMER:
		c.handleCloseConsumer(cmd.GetCloseConsumer())

	case pb.BaseCommand_AUTH_CHALLENGE:
		c.handleAuthChallenge(cmd.GetAuthChallenge())

	case pb.BaseCommand_SEND_RECEIPT:
		c.handleSendReceipt(cmd.GetSendReceipt())

	case pb.BaseCommand_SEND_ERROR:

	case pb.BaseCommand_MESSAGE:
		c.handleMessage(cmd.GetMessage(), headersAndPayload)

	case pb.BaseCommand_PING:
		c.handlePing()
	case pb.BaseCommand_PONG:
		c.handlePong()

	case pb.BaseCommand_ACTIVE_CONSUMER_CHANGE:

	default:
		c.log.Errorf("Received invalid command type: %s", cmd.Type)
		c.TriggerClose()
	}
}

func (c *connection) Write(data Buffer) {
	c.writeRequestsCh <- data
}

func (c *connection) SendRequest(requestID uint64, req *pb.BaseCommand,
	callback func(command *pb.BaseCommand, err error)) {
	if c.getState() == connectionClosed {
		callback(req, ErrConnectionClosed)
	} else {
		c.incomingRequestsCh <- &request{
			id:       &requestID,
			cmd:      req,
			callback: callback,
		}
	}
}

func (c *connection) SendRequestNoWait(req *pb.BaseCommand) error {
	if c.getState() == connectionClosed {
		return ErrConnectionClosed
	}

	c.incomingRequestsCh <- &request{
		id:       nil,
		cmd:      req,
		callback: nil,
	}
	return nil
}

func (c *connection) internalSendRequest(req *request) {
	c.pendingLock.Lock()
	if req.id != nil {
		c.pendingReqs[*req.id] = req
	}
	c.pendingLock.Unlock()
	if c.getState() == connectionClosed {
		c.log.Warnf("internalSendRequest failed for connectionClosed")
		if req.callback != nil {
			req.callback(req.cmd, ErrConnectionClosed)
		}
	} else {
		c.writeCommand(req.cmd)
	}
}

func (c *connection) handleResponse(requestID uint64, response *pb.BaseCommand) {
	c.pendingLock.Lock()
	request, ok := c.pendingReqs[requestID]
	if !ok {
		c.log.Warnf("Received unexpected response for request %d of type %s", requestID, response.Type)
		c.pendingLock.Unlock()
		return
	}

	delete(c.pendingReqs, requestID)
	c.pendingLock.Unlock()
	request.callback(response, nil)
}

func (c *connection) handleResponseError(serverError *pb.CommandError) {
	requestID := serverError.GetRequestId()
	c.pendingLock.Lock()
	request, ok := c.pendingReqs[requestID]
	if !ok {
		c.log.Warnf("Received unexpected error response for request %d of type %s",
			requestID, serverError.GetError())
		c.pendingLock.Unlock()
		return
	}

	delete(c.pendingReqs, requestID)
	c.pendingLock.Unlock()

	errMsg := fmt.Sprintf("server error: %s: %s", serverError.GetError(), serverError.GetMessage())
	request.callback(nil, errors.New(errMsg))
}

func (c *connection) handleSendReceipt(response *pb.CommandSendReceipt) {
	producerID := response.GetProducerId()

	c.Lock()
	producer, ok := c.listeners[producerID]
	c.Unlock()

	if ok {
		producer.ReceivedSendReceipt(response)
	} else {
		c.log.WithField("producerID", producerID).Warn("Got unexpected send receipt for message: ", response.MessageId)
	}
}

func (c *connection) handleMessage(response *pb.CommandMessage, payload Buffer) {
	c.log.Debug("Got Message: ", response)
	consumerID := response.GetConsumerId()
	if consumer, ok := c.consumerHandler(consumerID); ok {
		err := consumer.MessageReceived(response, payload)
		if err != nil {
			c.log.
				WithError(err).
				WithField("consumerID", consumerID).
				Error("handle message Id: ", response.MessageId)
		}
	} else {
		c.log.WithField("consumerID", consumerID).Warn("Got unexpected message: ", response.MessageId)
	}
}

func (c *connection) lastDataReceived() time.Time {
	c.lastDataReceivedLock.Lock()
	defer c.lastDataReceivedLock.Unlock()
	t := c.lastDataReceivedTime
	return t
}

func (c *connection) setLastDataReceived(t time.Time) {
	c.lastDataReceivedLock.Lock()
	defer c.lastDataReceivedLock.Unlock()
	c.lastDataReceivedTime = t
}

func (c *connection) sendPing() {
	c.log.Debug("Sending PING")
	c.writeCommand(baseCommand(pb.BaseCommand_PING, &pb.CommandPing{}))
}

func (c *connection) handlePong() {
	c.log.Debug("Received PONG response")
}

func (c *connection) handlePing() {
	c.log.Debug("Responding to PING request")
	c.writeCommand(baseCommand(pb.BaseCommand_PONG, &pb.CommandPong{}))
}

func (c *connection) handleAuthChallenge(authChallenge *pb.CommandAuthChallenge) {
	c.log.Debugf("Received auth challenge from broker: %s", authChallenge.GetChallenge().GetAuthMethodName())

	// Get new credentials from the provider
	authData, err := c.auth.GetData()
	if err != nil {
		c.log.WithError(err).Warn("Failed to load auth credentials")
		c.TriggerClose()
		return
	}

	cmdAuthResponse := &pb.CommandAuthResponse{
		ProtocolVersion: proto.Int32(PulsarProtocolVersion),
		ClientVersion:   proto.String(ClientVersionString),
		Response: &pb.AuthData{
			AuthMethodName: proto.String(c.auth.Name()),
			AuthData:       authData,
		},
	}

	c.writeCommand(baseCommand(pb.BaseCommand_AUTH_RESPONSE, cmdAuthResponse))
}

func (c *connection) handleCloseConsumer(closeConsumer *pb.CommandCloseConsumer) {
	consumerID := closeConsumer.GetConsumerId()
	c.log.Infof("Broker notification of Closed consumer: %d", consumerID)

	c.Lock()
	defer c.Unlock()

	if consumer, ok := c.consumerHandler(consumerID); ok {
		consumer.ConnectionClosed()
		c.DeleteConsumeHandler(consumerID)
	} else {
		c.log.WithField("consumerID", consumerID).Warnf("Consumer with ID not found while closing consumer")
	}
}

func (c *connection) handleCloseProducer(closeProducer *pb.CommandCloseProducer) {
	c.log.Infof("Broker notification of Closed producer: %d", closeProducer.GetProducerId())
	producerID := closeProducer.GetProducerId()

	c.Lock()
	defer c.Unlock()
	if producer, ok := c.listeners[producerID]; ok {
		producer.ConnectionClosed()
		delete(c.listeners, producerID)
	} else {
		c.log.WithField("producerID", producerID).Warn("Producer with ID not found while closing producer")
	}
}

func (c *connection) RegisterListener(id uint64, listener ConnectionListener) {
	c.Lock()
	defer c.Unlock()

	c.listeners[id] = listener
}

func (c *connection) UnregisterListener(id uint64) {
	c.Lock()
	defer c.Unlock()

	delete(c.listeners, id)
}

// Triggers the connection close by forcing the socket to close and
// broadcasting the notification on the close channel
func (c *connection) TriggerClose() {
	c.closeOnce.Do(func() {
		cnx := c.cnx
		if cnx != nil {
			cnx.Close()
		}

		close(c.closeCh)
	})
}

func (c *connection) Close() {
	c.Lock()
	defer c.Unlock()

	c.cond.Broadcast()

	if c.getState() == connectionClosed {
		return
	}

	c.log.Info("Connection closed")
	// do not use changeState() since they share the same lock
	c.setState(connectionClosed)
	c.TriggerClose()
	c.pingTicker.Stop()
	c.pingCheckTicker.Stop()

	for _, listener := range c.listeners {
		listener.ConnectionClosed()
	}

	consumerHandlers := make(map[uint64]ConsumerHandler)
	c.consumerHandlersLock.RLock()
	for id, handler := range c.consumerHandlers {
		consumerHandlers[id] = handler
	}
	c.consumerHandlersLock.RUnlock()

	for _, handler := range consumerHandlers {
		handler.ConnectionClosed()
	}

	c.metrics.ConnectionsClosed.Inc()
}

func (c *connection) changeState(state connectionState) {
	c.Lock()
	c.setState(state)
	c.cond.Broadcast()
	c.Unlock()
}

func (c *connection) getState() connectionState {
	return connectionState(c.state.Load())
}

func (c *connection) setState(state connectionState) {
	c.state.Store(int32(state))
}

func (c *connection) newRequestID() uint64 {
	return atomic.AddUint64(&c.requestIDGenerator, 1)
}

func (c *connection) getTLSConfig() (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: c.tlsOptions.AllowInsecureConnection,
	}

	if c.tlsOptions.TrustCertsFilePath != "" {
		caCerts, err := ioutil.ReadFile(c.tlsOptions.TrustCertsFilePath)
		if err != nil {
			return nil, err
		}

		tlsConfig.RootCAs = x509.NewCertPool()
		ok := tlsConfig.RootCAs.AppendCertsFromPEM(caCerts)
		if !ok {
			return nil, errors.New("failed to parse root CAs certificates")
		}
	}

	if c.tlsOptions.ValidateHostname {
		tlsConfig.ServerName = c.physicalAddr.Hostname()
	}

	cert, err := c.auth.GetTLSCertificate()
	if err != nil {
		return nil, err
	}

	if cert != nil {
		tlsConfig.Certificates = []tls.Certificate{*cert}
	}

	return tlsConfig, nil
}

func (c *connection) AddConsumeHandler(id uint64, handler ConsumerHandler) {
	c.consumerHandlersLock.Lock()
	defer c.consumerHandlersLock.Unlock()
	c.consumerHandlers[id] = handler
}

func (c *connection) DeleteConsumeHandler(id uint64) {
	c.consumerHandlersLock.Lock()
	defer c.consumerHandlersLock.Unlock()
	delete(c.consumerHandlers, id)
}

func (c *connection) consumerHandler(id uint64) (ConsumerHandler, bool) {
	c.consumerHandlersLock.RLock()
	defer c.consumerHandlersLock.RUnlock()
	h, ok := c.consumerHandlers[id]
	return h, ok
}

func (c *connection) ID() string {
	return fmt.Sprintf("%s -> %s", c.cnx.LocalAddr(), c.cnx.RemoteAddr())
}

func (c *connection) GetMaxMessageSize() int32 {
	return c.maxMessageSize
}
