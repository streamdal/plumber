package stream

import (
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"math/rand"
	"net/url"
	"strconv"
	"sync"
	"time"
)

type Environment struct {
	producers *producersEnvironment
	consumers *consumersEnvironment
	options   *EnvironmentOptions
}

func NewEnvironment(options *EnvironmentOptions) (*Environment, error) {
	client := newClient("go-stream-locator", nil)
	defer func(client *Client) {
		err := client.Close()
		if err != nil {
			return
		}
	}(client)
	if options == nil {
		options = NewEnvironmentOptions()
	}

	if options.MaxConsumersPerClient <= 0 || options.MaxProducersPerClient <= 0 ||
		options.MaxConsumersPerClient > 254 || options.MaxProducersPerClient > 254 {
		return nil, fmt.Errorf(" MaxConsumersPerClient and MaxProducersPerClient must be between 1 and 254")
	}

	if len(options.ConnectionParameters) == 0 {
		options.ConnectionParameters = []*Broker{newBrokerDefault()}
	}

	for _, parameter := range options.ConnectionParameters {

		if parameter.Uri != "" {
			u, err := url.Parse(parameter.Uri)
			if err != nil {
				return nil, err
			}
			parameter.User = u.User.Username()
			parameter.Password, _ = u.User.Password()
			parameter.Host = u.Host
			parameter.Port = u.Port()
			parameter.Scheme = u.Scheme
		}

		parameter.mergeWithDefault()

		client.broker = parameter
	}
	return &Environment{
		options:   options,
		producers: newProducers(options.MaxProducersPerClient),
		consumers: newConsumerEnvironment(options.MaxConsumersPerClient),
	}, client.connect()
}
func (env *Environment) newReconnectClient() (*Client, error) {
	broker := &env.options.ConnectionParameters[0]
	client := newClient("go-stream-locator", *broker)

	err := client.connect()
	tentatives := 1
	for err != nil {
		logs.LogError("Can't connect the locator client, error:%s, retry in %d seconds, broker: ", err, tentatives,
			client.broker)
		time.Sleep(time.Duration(tentatives) * time.Second)
		rand.Seed(time.Now().UnixNano())
		n := rand.Intn(len(env.options.ConnectionParameters))
		client = newClient("stream-locator", env.options.ConnectionParameters[n])
		tentatives = tentatives + 1
		err = client.connect()

	}

	return client, client.connect()
}

func (env *Environment) DeclareStream(streamName string, options *StreamOptions) error {
	client, err := env.newReconnectClient()
	defer func(client *Client) {
		err := client.Close()
		if err != nil {
			return
		}
	}(client)
	if err != nil {
		return err
	}
	return client.DeclareStream(streamName, options)
}

func (env *Environment) DeleteStream(streamName string) error {
	client, err := env.newReconnectClient()
	defer func(client *Client) {
		err := client.Close()
		if err != nil {
			return
		}
	}(client)
	if err != nil {
		return err
	}
	return client.DeleteStream(streamName)
}

func (env *Environment) NewProducer(streamName string, producerOptions *ProducerOptions) (*Producer, error) {
	client, err := env.newReconnectClient()
	defer func(client *Client) {
		err := client.Close()
		if err != nil {
			return
		}
	}(client)
	if err != nil {
		return nil, err
	}

	return env.producers.newProducer(client, streamName, producerOptions)
}

func (env *Environment) StreamExists(streamName string) (bool, error) {
	client, err := env.newReconnectClient()
	defer func(client *Client) {
		err := client.Close()
		if err != nil {
			return
		}
	}(client)
	if err != nil {
		return false, err
	}
	return client.StreamExists(streamName), nil
}

func (env *Environment) StreamMetaData(streamName string) (*StreamMetadata, error) {
	client, err := env.newReconnectClient()
	defer func(client *Client) {
		err := client.Close()
		if err != nil {
			return
		}
	}(client)
	if err != nil {
		return nil, err
	}
	streamsMetadata := client.metaData(streamName)
	streamMetadata := streamsMetadata.Get(streamName)

	tentatives := 0
	for streamMetadata == nil || streamMetadata.Leader == nil && tentatives < 3 {
		streamsMetadata = client.metaData(streamName)
		streamMetadata = streamsMetadata.Get(streamName)
		tentatives++
		time.Sleep(100 * time.Millisecond)
	}

	if streamMetadata.Leader == nil {

		return nil, errors.New("can't find leader for stream: " + streamName)
	}

	return streamMetadata, nil
}

func (env *Environment) NewConsumer(streamName string,
	messagesHandler MessagesHandler,
	options *ConsumerOptions) (*Consumer, error) {
	client, err := env.newReconnectClient()
	defer func(client *Client) {
		err := client.Close()
		if err != nil {
			return
		}
	}(client)
	if err != nil {
		return nil, err
	}

	return env.consumers.NewSubscriber(client, streamName, messagesHandler, options)
}

func (env *Environment) Close() error {
	_ = env.producers.close()
	_ = env.consumers.close()
	return nil
}

type EnvironmentOptions struct {
	ConnectionParameters  []*Broker
	MaxProducersPerClient int
	MaxConsumersPerClient int
}

func NewEnvironmentOptions() *EnvironmentOptions {
	return &EnvironmentOptions{
		MaxProducersPerClient: 1,
		MaxConsumersPerClient: 1,
		ConnectionParameters:  []*Broker{},
	}
}

func (envOptions *EnvironmentOptions) SetMaxProducersPerClient(maxProducersPerClient int) *EnvironmentOptions {
	envOptions.MaxProducersPerClient = maxProducersPerClient
	return envOptions
}

func (envOptions *EnvironmentOptions) SetMaxConsumersPerClient(maxConsumersPerClient int) *EnvironmentOptions {
	envOptions.MaxConsumersPerClient = maxConsumersPerClient
	return envOptions
}

func (envOptions *EnvironmentOptions) SetUri(uri string) *EnvironmentOptions {
	if len(envOptions.ConnectionParameters) == 0 {
		envOptions.ConnectionParameters = append(envOptions.ConnectionParameters, &Broker{Uri: uri})
	} else {
		envOptions.ConnectionParameters[0].Uri = uri
	}

	return envOptions
}

func (envOptions *EnvironmentOptions) SetUris(uris []string) *EnvironmentOptions {
	for _, s := range uris {
		envOptions.ConnectionParameters = append(envOptions.ConnectionParameters, &Broker{Uri: s})
	}
	return envOptions
}

func (envOptions *EnvironmentOptions) SetHost(host string) *EnvironmentOptions {
	if len(envOptions.ConnectionParameters) == 0 {
		envOptions.ConnectionParameters = append(envOptions.ConnectionParameters, &Broker{Host: host})
	} else {
		envOptions.ConnectionParameters[0].Host = host
	}
	return envOptions
}

func (envOptions *EnvironmentOptions) SetTLSConfig(config *tls.Config) *EnvironmentOptions {
	if len(envOptions.ConnectionParameters) == 0 {
		envOptions.ConnectionParameters = append(envOptions.ConnectionParameters, &Broker{tlsConfig: config})
	} else {
		for _, parameter := range envOptions.ConnectionParameters {
			parameter.tlsConfig = config
		}
	}

	return envOptions
}

func (envOptions *EnvironmentOptions) IsTLS(val bool) *EnvironmentOptions {
	if val {
		if len(envOptions.ConnectionParameters) == 0 {
			envOptions.ConnectionParameters = append(envOptions.ConnectionParameters, &Broker{Scheme: "rabbitmq-stream+tls"})
		} else {
			for _, parameter := range envOptions.ConnectionParameters {
				parameter.Scheme = "rabbitmq-stream+tls"
			}
		}
	}
	return envOptions
}

func (envOptions *EnvironmentOptions) SetPort(port int) *EnvironmentOptions {
	if len(envOptions.ConnectionParameters) == 0 {
		envOptions.ConnectionParameters = append(envOptions.ConnectionParameters, &Broker{Port: strconv.Itoa(port)})
	} else {
		envOptions.ConnectionParameters[0].Port = strconv.Itoa(port)
	}
	return envOptions

}

func (envOptions *EnvironmentOptions) SetUser(user string) *EnvironmentOptions {
	if len(envOptions.ConnectionParameters) == 0 {
		envOptions.ConnectionParameters = append(envOptions.ConnectionParameters, &Broker{User: user})
	} else {
		envOptions.ConnectionParameters[0].User = user
	}
	return envOptions

}

func (envOptions *EnvironmentOptions) SetPassword(password string) *EnvironmentOptions {
	if len(envOptions.ConnectionParameters) == 0 {
		envOptions.ConnectionParameters = append(envOptions.ConnectionParameters, &Broker{Password: password})
	} else {
		envOptions.ConnectionParameters[0].Password = password
	}
	return envOptions

}

type environmentCoordinator struct {
	mutex             *sync.Mutex
	mutexContext      *sync.RWMutex
	clientsPerContext map[int]*Client
	maxItemsForClient int
	nextId            int
}

func (cc *environmentCoordinator) isProducerListFull(clientsPerContextId int) bool {
	return cc.clientsPerContext[clientsPerContextId].coordinator.
		ProducersCount() >= cc.maxItemsForClient
}

func (cc *environmentCoordinator) isConsumerListFull(clientsPerContextId int) bool {
	return cc.clientsPerContext[clientsPerContextId].coordinator.
		ConsumersCount() >= cc.maxItemsForClient
}

func (cc *environmentCoordinator) maybeCleanClients() {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	cc.mutexContext.Lock()
	defer cc.mutexContext.Unlock()
	for i, client := range cc.clientsPerContext {
		if !client.socket.isOpen() {
			delete(cc.clientsPerContext, i)
		}
	}
}

func (client *Client) maybeCleanProducers(streamName string) {
	client.mutex.Lock()
	for pidx, producer := range client.coordinator.Producers() {
		if producer.(*Producer).GetStreamName() == streamName {
			err := client.coordinator.RemoveProducerById(pidx.(uint8), Event{
				Command:    CommandMetadataUpdate,
				StreamName: streamName,
				Name:       producer.(*Producer).GetName(),
				Reason:     "Meta data update",
				Err:        nil,
			})
			if err != nil {
				return
			}
		}
	}
	client.mutex.Unlock()
	if client.coordinator.ProducersCount() == 0 {
		err := client.Close()
		if err != nil {
			return
		}
	}
}

func (client *Client) maybeCleanConsumers(streamName string) {
	client.mutex.Lock()
	for pidx, consumer := range client.coordinator.consumers {
		if consumer.(*Consumer).options.streamName == streamName {
			err := client.coordinator.RemoveConsumerById(pidx.(uint8), Event{
				Command:    CommandMetadataUpdate,
				StreamName: streamName,
				Name:       consumer.(*Consumer).GetName(),
				Reason:     "Meta data update",
				Err:        nil,
			})
			if err != nil {
				return
			}
		}
	}
	client.mutex.Unlock()
	if client.coordinator.ConsumersCount() == 0 {
		err := client.Close()
		if err != nil {
			return
		}
	}
}

func (cc *environmentCoordinator) newProducer(leader *Broker, streamName string,
	options *ProducerOptions) (*Producer, error) {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	var clientResult *Client
	for i, client := range cc.clientsPerContext {
		if !cc.isProducerListFull(i) {
			clientResult = client
			break
		}
	}

	if clientResult == nil {
		clientResult = newClient("go-stream-producer", leader)
		chMeta := make(chan metaDataUpdateEvent, 1)
		clientResult.metadataListener = chMeta
		go func(ch <-chan metaDataUpdateEvent, cl *Client) {
			for metaDataUpdateEvent := range ch {
				clientResult.maybeCleanProducers(metaDataUpdateEvent.StreamName)
				cc.maybeCleanClients()
				if !cl.socket.isOpen() {
					return
				}
			}

		}(chMeta, clientResult)

		cc.nextId++
		cc.clientsPerContext[cc.nextId] = clientResult
	}

	err := clientResult.connect()
	if err != nil {
		return nil, err
	}

	producer, err := clientResult.DeclarePublisher(streamName, options)

	if err != nil {
		return nil, err
	}

	return producer, nil
}

func (cc *environmentCoordinator) newConsumer(leader *Broker,
	streamName string, messagesHandler MessagesHandler,
	options *ConsumerOptions) (*Consumer, error) {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	var clientResult *Client
	for i, client := range cc.clientsPerContext {
		if !cc.isConsumerListFull(i) {
			clientResult = client
			break
		}
	}

	if clientResult == nil {
		clientResult = newClient("go-stream-consumer", leader)
		chMeta := make(chan metaDataUpdateEvent)
		clientResult.metadataListener = chMeta
		go func(ch <-chan metaDataUpdateEvent, cl *Client) {
			for metaDataUpdateEvent := range ch {
				clientResult.maybeCleanConsumers(metaDataUpdateEvent.StreamName)
				cc.maybeCleanClients()
				if !cl.socket.isOpen() {
					return
				}
			}

		}(chMeta, clientResult)

		cc.nextId++
		cc.clientsPerContext[cc.nextId] = clientResult
	}
	// try to reconnect in case the socket is closed
	err := clientResult.connect()
	if err != nil {
		return nil, err
	}

	subscriber, err := clientResult.DeclareSubscriber(streamName, messagesHandler, options)

	if err != nil {
		return nil, err
	}
	return subscriber, nil
}

func (cc *environmentCoordinator) close() error {
	for _, client := range cc.getClientsPerContext() {
		err := client.Close()
		if err != nil {
			logs.LogWarn("Error during close the client, %s", err)
		}
	}
	return nil
}

func (cc *environmentCoordinator) getClientsPerContext() map[int]*Client {
	cc.mutexContext.Lock()
	defer cc.mutexContext.Unlock()
	return cc.clientsPerContext
}

type producersEnvironment struct {
	mutex                *sync.Mutex
	producersCoordinator map[string]*environmentCoordinator
	maxItemsForClient    int
}

func newProducers(maxItemsForClient int) *producersEnvironment {
	producers := &producersEnvironment{
		mutex:                &sync.Mutex{},
		producersCoordinator: map[string]*environmentCoordinator{},
		maxItemsForClient:    maxItemsForClient,
	}
	return producers
}

func (ps *producersEnvironment) newProducer(clientLocator *Client, streamName string,
	options *ProducerOptions) (*Producer, error) {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	leader, err := clientLocator.BrokerLeader(streamName)
	if err != nil {
		return nil, err
	}
	if ps.producersCoordinator[leader.hostPort()] == nil {
		ps.producersCoordinator[leader.hostPort()] = &environmentCoordinator{
			clientsPerContext: map[int]*Client{},
			mutex:             &sync.Mutex{},
			maxItemsForClient: ps.maxItemsForClient,
			mutexContext:      &sync.RWMutex{},
			nextId:            0,
		}
	}
	leader.cloneFrom(clientLocator.broker)

	producer, err := ps.producersCoordinator[leader.hostPort()].newProducer(leader, streamName,
		options)
	if err != nil {
		return nil, err
	}
	producer.onClose = func(ch <-chan uint8) {
		for _, coordinator := range ps.producersCoordinator {
			coordinator.maybeCleanClients()
		}
	}

	return producer, err
}

func (ps *producersEnvironment) close() error {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	for _, coordinator := range ps.producersCoordinator {
		_ = coordinator.close()
	}
	return nil
}

func (ps *producersEnvironment) getCoordinators() map[string]*environmentCoordinator {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	return ps.producersCoordinator
}

type consumersEnvironment struct {
	mutex                *sync.Mutex
	consumersCoordinator map[string]*environmentCoordinator
	maxItemsForClient    int
	PublishErrorListener ChannelPublishError
}

func newConsumerEnvironment(maxItemsForClient int) *consumersEnvironment {
	producers := &consumersEnvironment{
		mutex:                &sync.Mutex{},
		consumersCoordinator: map[string]*environmentCoordinator{},
		maxItemsForClient:    maxItemsForClient,
	}
	return producers
}

func (ps *consumersEnvironment) NewSubscriber(clientLocator *Client, streamName string,
	messagesHandler MessagesHandler,
	consumerOptions *ConsumerOptions) (*Consumer, error) {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	consumerBroker, err := clientLocator.BrokerForConsumer(streamName)
	if err != nil {
		return nil, err
	}
	if ps.consumersCoordinator[consumerBroker.hostPort()] == nil {
		ps.consumersCoordinator[consumerBroker.hostPort()] = &environmentCoordinator{
			clientsPerContext: map[int]*Client{},
			mutex:             &sync.Mutex{},
			maxItemsForClient: ps.maxItemsForClient,
			mutexContext:      &sync.RWMutex{},
			nextId:            0,
		}
	}
	consumerBroker.cloneFrom(clientLocator.broker)
	consumer, err := ps.consumersCoordinator[consumerBroker.hostPort()].
		newConsumer(consumerBroker, streamName, messagesHandler, consumerOptions)
	if err != nil {
		return nil, err
	}
	consumer.onClose = func(ch <-chan uint8) {
		for _, coordinator := range ps.consumersCoordinator {
			coordinator.maybeCleanClients()
		}
	}
	return consumer, err
}

func (ps *consumersEnvironment) close() error {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	for _, coordinator := range ps.consumersCoordinator {
		_ = coordinator.close()
	}
	return nil
}

func (ps *consumersEnvironment) getCoordinators() map[string]*environmentCoordinator {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	return ps.consumersCoordinator
}
