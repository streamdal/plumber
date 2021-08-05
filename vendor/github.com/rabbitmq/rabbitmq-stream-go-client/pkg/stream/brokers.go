package stream

import (
	"crypto/tls"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

type AddressResolver struct {
	Host string
	Port int
}

type Broker struct {
	Host      string
	Port      string
	User      string
	Vhost     string
	Uri       string
	Password  string
	Scheme    string
	tlsConfig *tls.Config
	advHost   string
	advPort   string
}

func newBrokerDefault() *Broker {
	return &Broker{
		Scheme:   "rabbitmq-stream",
		Host:     "localhost",
		Port:     StreamTcpPort,
		User:     "guest",
		Password: "guest",
		Vhost:    "/",
	}
}

func (br *Broker) isTLS() bool {
	return strings.Index(br.Scheme, "+tls") > 0
}

func (br *Broker) mergeWithDefault() {
	broker := newBrokerDefault()
	if br.Host == "" {
		br.Host = broker.Host
	}
	if br.Vhost == "" {
		br.Vhost = broker.Vhost
	}

	if br.User == "" {
		br.User = broker.User
	}
	if br.User == "" {
		br.User = broker.User
	}
	if br.Password == "" {
		br.Password = broker.Password
	}
	if br.Port == "" || br.Port == "0" {
		br.Port = broker.Port
	}
	if br.Scheme == "" {
		br.Scheme = broker.Scheme
	}

	if br.tlsConfig == nil {
		br.tlsConfig = broker.tlsConfig
	}

}

func (br *Broker) cloneFrom(broker *Broker, resolver *AddressResolver) {
	br.User = broker.User
	br.Password = broker.Password
	br.Vhost = broker.Vhost
	br.Scheme = broker.Scheme
	br.tlsConfig = broker.tlsConfig
	if resolver != nil {
		br.Host = resolver.Host
		br.Port = strconv.Itoa(resolver.Port)
	}
}

func (br *Broker) GetUri() string {
	if br.Uri == "" {
		br.Uri = fmt.Sprintf("%s://%s:%s@%s:%s/%s",
			br.Scheme,
			br.User, br.Password,
			br.Host, br.Port, br.Vhost)
	}
	return br.Uri
}

func newBroker(host string, port string) *Broker {
	return &Broker{
		Host: host,
		Port: port,
	}
}

type Brokers struct {
	items *sync.Map
}

func newBrokers() *Brokers {
	return &Brokers{items: &sync.Map{}}
}

func (brs *Brokers) Add(brokerReference int16, host string, port uint32) *Broker {
	broker := newBroker(host, strconv.Itoa(int(port)))
	brs.items.Store(brokerReference, broker)
	return broker
}

func (brs *Brokers) Get(brokerReference int16) *Broker {
	value, ok := brs.items.Load(brokerReference)
	if !ok {
		return nil
	}

	return value.(*Broker)
}

func (br *Broker) hostPort() string {
	return fmt.Sprintf("%s:%s", br.Host, br.Port)
}

type StreamMetadata struct {
	stream       string
	responseCode uint16
	Leader       *Broker
	replicas     []*Broker
}

func (StreamMetadata) New(stream string, responseCode uint16,
	leader *Broker, replicas []*Broker) *StreamMetadata {
	return &StreamMetadata{stream: stream, responseCode: responseCode,
		Leader: leader, replicas: replicas}
}

type StreamsMetadata struct {
	items *sync.Map
}

func (StreamsMetadata) New() *StreamsMetadata {
	return &StreamsMetadata{&sync.Map{}}
}

func (smd *StreamsMetadata) Add(stream string, responseCode uint16,
	leader *Broker, replicas []*Broker) *StreamMetadata {
	streamMetadata := StreamMetadata{}.New(stream, responseCode,
		leader, replicas)
	smd.items.Store(stream, streamMetadata)
	return streamMetadata
}

func (smd *StreamsMetadata) Get(stream string) *StreamMetadata {
	value, ok := smd.items.Load(stream)
	if !ok {
		return nil
	}
	return value.(*StreamMetadata)
}
