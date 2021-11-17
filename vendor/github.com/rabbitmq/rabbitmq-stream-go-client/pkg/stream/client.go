package stream

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"math/rand"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

type TuneState struct {
	requestedMaxFrameSize int
	requestedHeartbeat    int
}

type ClientProperties struct {
	items map[string]string
}

type ConnectionProperties struct {
	host string
	port string
}

type HeartBeat struct {
	mutex sync.Mutex
	value time.Time
}

type Client struct {
	socket               socket
	destructor           *sync.Once
	clientProperties     ClientProperties
	connectionProperties ConnectionProperties
	tuneState            TuneState
	coordinator          *Coordinator
	broker               *Broker
	plainCRCBuffer       []byte

	mutex            *sync.Mutex
	metadataListener metadataListener
	lastHeartBeat    HeartBeat
}

func newClient(connectionName string, broker *Broker) *Client {
	var clientBroker = broker
	if broker == nil {
		clientBroker = newBrokerDefault()
	}

	c := &Client{
		coordinator:          NewCoordinator(),
		broker:               clientBroker,
		destructor:           &sync.Once{},
		mutex:                &sync.Mutex{},
		clientProperties:     ClientProperties{items: make(map[string]string)},
		connectionProperties: ConnectionProperties{},
		plainCRCBuffer:       make([]byte, 4096),
		lastHeartBeat: HeartBeat{
			value: time.Now(),
		},
		socket: socket{
			mutex:      &sync.Mutex{},
			destructor: &sync.Once{},
		},
	}
	c.setConnectionName(connectionName)
	return c
}

func NewDirectClient(broker *Broker) (*Client, error) {
	client := newClient("direct-connection", broker)
	return client, client.connect()
}

func (c *Client) getSocket() *socket {
	//c.mutex.Lock()
	//defer c.mutex.Unlock()
	return &c.socket
}

func (c *Client) setSocketConnection(connection net.Conn) {
	//c.mutex.Lock()
	//defer c.mutex.Unlock()
	c.socket.connection = connection
	c.socket.writer = bufio.NewWriter(connection)
}

func (c *Client) getTuneState() TuneState {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.tuneState
}

func (c *Client) getLastHeartBeat() time.Time {
	c.lastHeartBeat.mutex.Lock()
	defer c.lastHeartBeat.mutex.Unlock()
	return c.lastHeartBeat.value
}

func (c *Client) setLastHeartBeat(value time.Time) {
	c.lastHeartBeat.mutex.Lock()
	defer c.lastHeartBeat.mutex.Unlock()
	c.lastHeartBeat.value = value
}

func (c *Client) connect() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if !c.socket.isOpen() {
		u, err := url.Parse(c.broker.GetUri())
		if err != nil {
			return err
		}
		host, port := u.Hostname(), u.Port()
		c.tuneState.requestedHeartbeat = 60
		c.tuneState.requestedMaxFrameSize = 1048576

		var dialer = &net.Dialer{
			Control: controlFunc,
		}

		var connection net.Conn
		var errorConnection error
		if c.broker.isTLS() {
			conf := &tls.Config{}

			if c.broker.tlsConfig != nil {
				conf = c.broker.tlsConfig
			}

			var tlsDial = &tls.Dialer{
				NetDialer: dialer,
				Config:    conf,
			}

			connection, errorConnection = tlsDial.Dial("tcp", net.JoinHostPort(host, port))
			if errorConnection != nil {
				logs.LogDebug("TLS error connection %s", errorConnection)
				return errorConnection
			}

		} else {
			connection, errorConnection = dialer.Dial("tcp", net.JoinHostPort(host, port))
			if errorConnection != nil {
				logs.LogDebug("%s", errorConnection)
				return errorConnection
			}
		}

		c.setSocketConnection(connection)
		c.socket.setOpen()

		go c.handleResponse()
		err2 := c.peerProperties()

		if err2 != nil {
			logs.LogDebug("%s", err2)
			return err2
		}
		pwd, _ := u.User.Password()
		err2 = c.authenticate(u.User.Username(), pwd)
		if err2 != nil {
			logs.LogDebug("User:%s, %s", u.User.Username(), err2)
			return err2
		}
		vhost := "/"
		if len(u.Path) > 1 {
			vhost, _ = url.QueryUnescape(u.Path[1:])
		}
		err2 = c.open(vhost)
		if err2 != nil {
			logs.LogDebug("%s", err2)
			return err2
		}

		c.heartBeat()
		logs.LogDebug("User %s, connected to: %s, vhost:%s", u.User.Username(),
			net.JoinHostPort(host, port),
			vhost)
	}
	return nil
}

func (c *Client) setConnectionName(connectionName string) {
	c.clientProperties.items["connection_name"] = connectionName
}

func (c *Client) peerProperties() error {
	clientPropertiesSize := 4 // size of the map, always there

	c.clientProperties.items["product"] = "RabbitMQ Stream"
	c.clientProperties.items["copyright"] = "Copyright (c) 2021 VMware, Inc. or its affiliates."
	c.clientProperties.items["information"] = "Licensed under the MPL 2.0. See https://www.rabbitmq.com/"
	c.clientProperties.items["version"] = ClientVersion
	c.clientProperties.items["platform"] = "Golang"
	for key, element := range c.clientProperties.items {
		clientPropertiesSize = clientPropertiesSize + 2 + len(key) + 2 + len(element)
	}

	length := 2 + 2 + 4 + clientPropertiesSize
	resp := c.coordinator.NewResponse(commandPeerProperties)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandPeerProperties,
		correlationId)
	writeInt(b, len(c.clientProperties.items))

	for key, element := range c.clientProperties.items {
		writeString(b, key)
		writeString(b, element)
	}

	return c.handleWrite(b.Bytes(), resp).Err
}

func (c *Client) authenticate(user string, password string) error {

	saslMechanisms, err := c.getSaslMechanisms()
	if err != nil {
		return err
	}
	saslMechanism := ""
	for i := 0; i < len(saslMechanisms); i++ {
		if saslMechanisms[i] == "PLAIN" {
			saslMechanism = "PLAIN"
		}
	}
	response := unicodeNull + user + unicodeNull + password
	saslResponse := []byte(response)
	return c.sendSaslAuthenticate(saslMechanism, saslResponse)
}

func (c *Client) getSaslMechanisms() ([]string, error) {
	length := 2 + 2 + 4
	resp := c.coordinator.NewResponse(commandSaslHandshake)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandSaslHandshake,
		correlationId)

	errWrite := c.socket.writeAndFlush(b.Bytes())
	data := <-resp.data
	err := c.coordinator.RemoveResponseById(correlationId)
	if err != nil {
		return nil, err
	}
	if errWrite != nil {
		return nil, errWrite
	}
	return data.([]string), nil

}

func (c *Client) sendSaslAuthenticate(saslMechanism string, challengeResponse []byte) error {
	length := 2 + 2 + 4 + 2 + len(saslMechanism) + 4 + len(challengeResponse)
	resp := c.coordinator.NewResponse(commandSaslAuthenticate)
	respTune := c.coordinator.NewResponseWitName("tune")
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandSaslAuthenticate,
		correlationId)

	writeString(b, saslMechanism)
	writeInt(b, len(challengeResponse))
	b.Write(challengeResponse)
	err := c.handleWrite(b.Bytes(), resp)
	if err.Err != nil {
		return err.Err
	}
	// double read for TUNE
	tuneData := <-respTune.data
	errR := c.coordinator.RemoveResponseByName("tune")
	if errR != nil {
		return errR
	}

	return c.socket.writeAndFlush(tuneData.([]byte))
}

func (c *Client) open(virtualHost string) error {
	length := 2 + 2 + 4 + 2 + len(virtualHost)
	resp := c.coordinator.NewResponse(commandOpen, virtualHost)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandOpen,
		correlationId)
	writeString(b, virtualHost)
	err := c.handleWriteWithResponse(b.Bytes(), resp, false)
	if err.Err != nil {
		return err.Err
	}

	advHostPort := <-resp.data
	c.connectionProperties.host = advHostPort.(ConnectionProperties).host
	c.connectionProperties.port = advHostPort.(ConnectionProperties).port

	_ = c.coordinator.RemoveResponseById(resp.correlationid)
	return nil

}

func (c *Client) DeleteStream(streamName string) error {
	length := 2 + 2 + 4 + 2 + len(streamName)
	resp := c.coordinator.NewResponse(commandDeleteStream, streamName)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandDeleteStream,
		correlationId)

	writeString(b, streamName)

	return c.handleWrite(b.Bytes(), resp).Err
}

func (c *Client) heartBeat() {
	ticker := time.NewTicker(60 * time.Second)
	tickerHeatBeat := time.NewTicker(20 * time.Second)
	resp := c.coordinator.NewResponseWitName("heartbeat")
	var heartBeatMissed int32
	go func() {
		for c.socket.isOpen() {
			<-tickerHeatBeat.C
			if time.Since(c.getLastHeartBeat()) > time.Duration(c.tuneState.requestedHeartbeat)*time.Second {
				v := atomic.AddInt32(&heartBeatMissed, 1)
				logs.LogWarn("Missing heart beat: %d", v)
				if v >= 2 {
					logs.LogWarn("Too many heartbeat missing: %d", v)
					c.Close()
				}
			} else {
				atomic.StoreInt32(&heartBeatMissed, 0)
			}

		}
		tickerHeatBeat.Stop()
	}()

	go func() {
		for {
			select {
			case code := <-resp.code:
				if code.id == closeChannel {
					_ = c.coordinator.RemoveResponseByName("heartbeat")
				}
				ticker.Stop()
				return
			case <-ticker.C:
				logs.LogDebug("Sending heart beat: %s", time.Now())
				c.sendHeartbeat()
			}
		}
	}()

}

func (c *Client) sendHeartbeat() {
	length := 4
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandHeartbeat)
	_ = c.socket.writeAndFlush(b.Bytes())
}

func (c *Client) closeHartBeat() {
	c.destructor.Do(func() {
		r, err := c.coordinator.GetResponseByName("heartbeat")
		if err != nil {
			logs.LogWarn("error removing heartbeat: %s", err)
		} else {
			r.code <- Code{id: closeChannel}
		}
	})

}

func (c *Client) Close() error {

	for _, p := range c.coordinator.Producers() {
		err := c.coordinator.RemoveProducerById(p.(*Producer).id, Event{
			Command:    CommandClose,
			StreamName: p.(*Producer).GetStreamName(),
			Name:       p.(*Producer).GetName(),
			Reason:     "socket client closed",
			Err:        nil,
		})

		if err != nil {
			logs.LogWarn("error removing producer: %s", err)
		}
	}
	for _, cs := range c.coordinator.consumers {
		err := c.coordinator.RemoveConsumerById(cs.(*Consumer).ID, Event{
			Command:    CommandClose,
			StreamName: cs.(*Consumer).GetStreamName(),
			Name:       cs.(*Consumer).GetName(),
			Reason:     "socket client closed",
			Err:        nil,
		})
		if err != nil {
			logs.LogWarn("error removing consumer: %s", err)
		}
	}

	//c.mutex.Lock()
	if c.metadataListener != nil {
		close(c.metadataListener)
		c.metadataListener = nil
	}
	//c.mutex.Unlock()

	var err error
	if c.getSocket().isOpen() {

		c.closeHartBeat()
		res := c.coordinator.NewResponse(CommandClose)
		length := 2 + 2 + 4 + 2 + 2 + len("OK")
		var b = bytes.NewBuffer(make([]byte, 0, length+4))
		writeProtocolHeader(b, length, CommandClose, res.correlationid)
		writeUShort(b, responseCodeOk)
		writeString(b, "OK")

		errW := c.socket.writeAndFlush(b.Bytes())
		if errW != nil {
			logs.LogWarn("error during send client close %s", errW)
		}
		_ = c.coordinator.RemoveResponseById(res.correlationid)
	}
	c.getSocket().shutdown(nil)
	return err
}

func (c *Client) ReusePublisher(streamName string, existingProducer *Producer) (*Producer, error) {
	existingProducer.options.client = c
	_, err := c.coordinator.GetProducerById(existingProducer.id)
	if err != nil {
		c.coordinator.producers[existingProducer.id] = existingProducer
	} else {
		return nil, fmt.Errorf("can't reuse producer")
	}
	res := c.internalDeclarePublisher(streamName, existingProducer)

	return existingProducer, res.Err
}

func (c *Client) DeclarePublisher(streamName string, options *ProducerOptions) (*Producer, error) {
	if options == nil {
		options = NewProducerOptions()
	}

	if options.QueueSize < minQueuePublisherSize || options.QueueSize > maxQueuePublisherSize {
		return nil, fmt.Errorf("QueueSize values must be between %d and %d",
			minQueuePublisherSize, maxQueuePublisherSize)
	}

	if options.BatchSize < minBatchSize || options.BatchSize > maxBatchSize {
		return nil, fmt.Errorf("BatchSize values must be between %d and %d",
			minBatchSize, maxBatchSize)
	}

	if options.BatchPublishingDelay < minBatchPublishingDelay || options.BatchPublishingDelay > maxBatchPublishingDelay {
		return nil, fmt.Errorf("BatchPublishingDelay values must be between %d and %d",
			minBatchPublishingDelay, maxBatchPublishingDelay)
	}

	producer, err := c.coordinator.NewProducer(&ProducerOptions{
		client:               c,
		streamName:           streamName,
		Name:                 options.Name,
		QueueSize:            options.QueueSize,
		BatchSize:            options.BatchSize,
		BatchPublishingDelay: options.BatchPublishingDelay,
	})

	if err != nil {
		return nil, err
	}
	res := c.internalDeclarePublisher(streamName, producer)
	if res.Err == nil {
		producer.startPublishTask()
	}
	return producer, res.Err
}

func (c *Client) internalDeclarePublisher(streamName string, producer *Producer) responseError {
	publisherReferenceSize := 0
	if producer.options != nil {
		if producer.options.Name != "" {
			publisherReferenceSize = len(producer.options.Name)
		}
	}

	length := 2 + 2 + 4 + 1 + 2 + publisherReferenceSize + 2 + len(streamName)
	resp := c.coordinator.NewResponse(commandDeclarePublisher, streamName)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandDeclarePublisher,
		correlationId)

	writeByte(b, producer.id)
	writeShort(b, int16(publisherReferenceSize))
	if publisherReferenceSize > 0 {
		writeBytes(b, []byte(producer.options.Name))
	}

	writeString(b, streamName)
	res := c.handleWrite(b.Bytes(), resp)

	if publisherReferenceSize > 0 {
		producer.sequence = c.queryPublisherSequence(producer.options.Name, streamName)
	}

	return res
}

func (c *Client) metaData(streams ...string) *StreamsMetadata {

	length := 2 + 2 + 4 + 4 // API code, version, correlation id, size of array
	for _, stream := range streams {
		length += 2
		length += len(stream)

	}
	resp := c.coordinator.NewResponse(commandMetadata)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandMetadata,
		correlationId)

	writeInt(b, len(streams))
	for _, stream := range streams {
		writeString(b, stream)
	}

	err := c.handleWriteWithResponse(b.Bytes(), resp, false)
	if err.Err != nil {
		return nil
	}

	data := <-resp.data
	_ = c.coordinator.RemoveResponseById(resp.correlationid)
	return data.(*StreamsMetadata)
}

func (c *Client) queryPublisherSequence(publisherReference string, stream string) int64 {

	length := 2 + 2 + 4 + 2 + len(publisherReference) + 2 + len(stream)
	resp := c.coordinator.NewResponse(commandQueryPublisherSequence)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandQueryPublisherSequence, correlationId)

	writeString(b, publisherReference)
	writeString(b, stream)
	c.handleWriteWithResponse(b.Bytes(), resp, false)
	sequence := <-resp.data
	_ = c.coordinator.RemoveResponseById(resp.correlationid)
	return sequence.(int64)

}

func (c *Client) BrokerLeader(stream string) (*Broker, error) {
	streamsMetadata := c.metaData(stream)
	if streamsMetadata == nil {
		return nil, fmt.Errorf("leader error for stream for stream: %s", stream)
	}

	streamMetadata := streamsMetadata.Get(stream)
	if streamMetadata.responseCode != responseCodeOk {
		return nil, lookErrorCode(streamMetadata.responseCode)
	}
	if streamMetadata.Leader == nil {
		return nil, fmt.Errorf("leader error for stream for stream: %s", stream)
	}

	streamMetadata.Leader.advPort = streamMetadata.Leader.Port
	streamMetadata.Leader.advHost = streamMetadata.Leader.Host
	return streamMetadata.Leader, nil
}

func (c *Client) StreamExists(stream string) bool {
	streamsMetadata := c.metaData(stream)
	if streamsMetadata == nil {
		return false
	}

	streamMetadata := streamsMetadata.Get(stream)
	return streamMetadata.responseCode == responseCodeOk
}
func (c *Client) BrokerForConsumer(stream string) (*Broker, error) {
	streamsMetadata := c.metaData(stream)
	if streamsMetadata == nil {
		return nil, fmt.Errorf("leader error for stream for stream: %s", stream)
	}

	streamMetadata := streamsMetadata.Get(stream)
	if streamMetadata.responseCode != responseCodeOk {
		return nil, lookErrorCode(streamMetadata.responseCode)
	}
	var brokers []*Broker
	brokers = append(brokers, streamMetadata.Leader)
	brokers = append(brokers, streamMetadata.Replicas...)
	rand.Seed(time.Now().UnixNano())
	n := rand.Intn(len(brokers))
	return brokers[n], nil
}

func (c *Client) DeclareStream(streamName string, options *StreamOptions) error {
	if streamName == "" {
		return fmt.Errorf("stream Name can't be empty")
	}

	resp := c.coordinator.NewResponse(commandCreateStream, streamName)
	length := 2 + 2 + 4 + 2 + len(streamName) + 4
	correlationId := resp.correlationid
	if options == nil {
		options = NewStreamOptions()
	}

	args, err := options.buildParameters()
	if err != nil {
		_ = c.coordinator.RemoveResponseById(resp.correlationid)
		return err
	}
	for key, element := range args {
		length = length + 2 + len(key) + 2 + len(element)
	}
	var b = bytes.NewBuffer(make([]byte, 0, length))
	writeProtocolHeader(b, length, commandCreateStream,
		correlationId)
	writeString(b, streamName)
	writeInt(b, len(args))

	for key, element := range args {
		writeString(b, key)
		writeString(b, element)
	}

	return c.handleWrite(b.Bytes(), resp).Err

}

func (c *Client) DeclareSubscriber(streamName string,
	messagesHandler MessagesHandler,
	options *ConsumerOptions) (*Consumer, error) {
	if options == nil {
		options = NewConsumerOptions()
	}

	if options.Offset.typeOfs <= 0 || options.Offset.typeOfs > 6 {
		return nil, fmt.Errorf("specify a valid Offset")
	}

	options.client = c
	options.streamName = streamName
	consumer := c.coordinator.NewConsumer(messagesHandler, options)
	length := 2 + 2 + 4 + 1 + 2 + len(streamName) + 2 + 2
	if options.Offset.isOffset() ||
		options.Offset.isTimestamp() {
		length += 8
	}

	if options.Offset.isLastConsumed() {
		lastOffset, err := consumer.QueryOffset()
		if err != nil {
			_ = c.coordinator.RemoveConsumerById(consumer.ID, Event{
				Command:    CommandQueryOffset,
				StreamName: streamName,
				Name:       consumer.GetName(),
				Reason:     "error QueryOffset",
				Err:        err,
			})
			return nil, err
		}
		options.Offset.offset = lastOffset
		// here we change the type since typeLastConsumed is not part of the protocol
		options.Offset.typeOfs = typeOffset
	}

	// copy the option offset to the consumer offset
	// the option.offset won't change ( in case we need to retrive the original configuration)
	// consumer.current offset will be moved when reading
	consumer.setCurrentOffset(options.Offset.offset)

	resp := c.coordinator.NewResponse(commandSubscribe, streamName)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandSubscribe,
		correlationId)
	writeByte(b, consumer.ID)

	writeString(b, streamName)

	writeShort(b, options.Offset.typeOfs)

	if options.Offset.isOffset() ||
		options.Offset.isTimestamp() {
		writeLong(b, options.Offset.offset)
	}
	writeShort(b, 10)

	err := c.handleWrite(b.Bytes(), resp)

	go func() {
		for {
			select {
			case code := <-consumer.response.code:
				if code.id == closeChannel {
					return
				}

			case data := <-consumer.response.data:
				consumer.setCurrentOffset(data.(int64))

			case messages := <-consumer.response.messages:
				for _, message := range messages {
					consumer.MessagesHandler(ConsumerContext{Consumer: consumer}, message)
				}

			}
		}
	}()
	return consumer, err.Err
}
