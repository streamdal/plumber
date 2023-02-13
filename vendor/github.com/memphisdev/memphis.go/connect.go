// Credit for The NATS.IO Authors
// Copyright 2021-2022 The Memphis Authors
// Licensed under the Apache License, Version 2.0 (the “License”);
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an “AS IS” BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.package server

package memphis

import (
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

const configurationUpdatesSubject = "$memphis_sdk_configurations_updates"

// Option is a function on the options for a connection.
type Option func(*Options) error

type TLSOpts struct {
	TlsCert string
	TlsKey  string
	CaFile  string
}

type Options struct {
	Host              string
	Port              int
	Username          string
	ConnectionToken   string
	Reconnect         bool
	MaxReconnect      int
	ReconnectInterval time.Duration
	Timeout           time.Duration
	TLSOpts           TLSOpts
}

type queryReq struct {
	resp chan bool
}

type ConfigurationsUpdate struct {
	StationName string `json:"station_name"`
	Type        string `json:"type"`
	Update      bool   `json:"update"`
}

func (c *Conn) IsConnected() bool {
	return c.brokerConn.IsConnected()
}

// Conn - holds the connection with memphis.
type Conn struct {
	opts               Options
	ConnId             string
	username           string
	brokerConn         *nats.Conn
	js                 nats.JetStreamContext
	stationUpdatesMu   sync.RWMutex
	stationUpdatesSubs map[string]*stationUpdateSub
	configUpdatesMu    sync.RWMutex
	configUpdatesSub   configurationsUpdateSub
}

type attachSchemaReq struct {
	Name        string `json:"name"`
	StationName string `json:"station_name"`
	Username    string `json:"username"`
}

type detachSchemaReq struct {
	StationName string `json:"station_name"`
	Username    string `json:"username"`
}

// getDefaultOptions - returns default configuration options for the client.
func getDefaultOptions() Options {
	return Options{
		Port:              6666,
		Reconnect:         true,
		MaxReconnect:      3,
		ReconnectInterval: 200 * time.Millisecond,
		Timeout:           15 * time.Second,
		TLSOpts: TLSOpts{
			TlsCert: "",
			TlsKey:  "",
			CaFile:  "",
		},
	}
}

type errorResp struct {
	Message string `json:"message"`
}

type configurationsUpdateSub struct {
	ConfigUpdatesCh            chan ConfigurationsUpdate
	ConfigUpdateSub            *nats.Subscription
	ClusterConfigurations      map[string]bool
	StationSchemaverseToDlsMap map[string]bool
}

// Connect - creates connection with memphis.
func Connect(host, username, connectionToken string, options ...Option) (*Conn, error) {
	opts := getDefaultOptions()

	opts.Host = normalizeHost(host)
	opts.Username = username
	opts.ConnectionToken = connectionToken

	for _, opt := range options {
		if opt != nil {
			if err := opt(&opts); err != nil {
				return nil, memphisError(err)
			}
		}
	}
	conn, err := opts.connect()
	if err != nil {
		return nil, err
	}
	err = conn.listenToConfigurationUpdates()
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func normalizeHost(host string) string {
	r := regexp.MustCompile("^http(s?)://")
	return r.ReplaceAllString(host, "")
}

func randomHex(n int) (string, error) {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return "", memphisError(err)
	}
	return hex.EncodeToString(bytes), nil
}

func (opts Options) connect() (*Conn, error) {
	if opts.MaxReconnect > 9 {
		opts.MaxReconnect = 9
	}

	if !opts.Reconnect {
		opts.MaxReconnect = 0
	}

	connId, err := randomHex(12)
	if err != nil {
		return nil, memphisError(err)
	}

	c := Conn{
		ConnId: connId,
		opts:   opts,
	}

	if err := c.startConn(); err != nil {
		return nil, memphisError(err)
	}

	c.stationUpdatesSubs = make(map[string]*stationUpdateSub)

	return &c, nil
}

func disconnectedError(conn *nats.Conn, err error) {
	if err != nil {
		fmt.Printf("Error %v", err.Error())
	}
}

func (c *Conn) startConn() error {
	opts := &c.opts
	var err error
	url := opts.Host + ":" + strconv.Itoa(opts.Port)
	natsOpts := nats.Options{
		Url:               url,
		AllowReconnect:    opts.Reconnect,
		MaxReconnect:      opts.MaxReconnect,
		ReconnectWait:     opts.ReconnectInterval,
		Timeout:           opts.Timeout,
		Token:             opts.ConnectionToken,
		DisconnectedErrCB: disconnectedError,
		Name:              c.ConnId + "::" + opts.Username,
	}
	if (opts.TLSOpts.TlsCert != "") || (opts.TLSOpts.TlsKey != "") || (opts.TLSOpts.CaFile != "") {
		if opts.TLSOpts.TlsCert == "" {
			return memphisError(errors.New("Must provide a TLS cert file"))
		}
		if opts.TLSOpts.TlsKey == "" {
			return memphisError(errors.New("Must provide a TLS key file"))
		}
		if opts.TLSOpts.CaFile == "" {
			return memphisError(errors.New("Must provide a TLS ca file"))
		}
		cert, err := tls.LoadX509KeyPair(opts.TLSOpts.TlsCert, opts.TLSOpts.TlsKey)
		if err != nil {
			return memphisError(errors.New("memphis: error loading client certificate: " + err.Error()))
		}
		cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
		if err != nil {
			return memphisError(errors.New("memphis: error parsing client certificate: " + err.Error()))
		}
		TLSConfig := &tls.Config{MinVersion: tls.VersionTLS12}
		TLSConfig.Certificates = []tls.Certificate{cert}
		certs := x509.NewCertPool()

		pemData, err := ioutil.ReadFile(opts.TLSOpts.CaFile)
		if err != nil {
			return memphisError(errors.New("memphis: error loading ca file: " + err.Error()))
		}
		certs.AppendCertsFromPEM(pemData)
		TLSConfig.RootCAs = certs
		natsOpts.TLSConfig = TLSConfig
	}

	c.brokerConn, err = natsOpts.Connect()
	if err != nil {
		return memphisError(err)
	}
	c.js, err = c.brokerConn.JetStream()

	if err != nil {
		c.brokerConn.Close()
		return memphisError(err)
	}
	c.username = opts.Username
	return nil
}

func (c *Conn) Close() {
	c.brokerConn.Close()
}

func (c *Conn) brokerCorePublish(subject, reply string, msg []byte) error {
	return c.brokerConn.PublishRequest(subject, reply, msg)
}

func (c *Conn) brokerPublish(msg *nats.Msg, opts ...nats.PubOpt) (nats.PubAckFuture, error) {
	return c.js.PublishMsgAsync(msg, opts...)
}

func (c *Conn) brokerPullSubscribe(subject, durable string, opts ...nats.SubOpt) (*nats.Subscription, error) {
	return c.js.PullSubscribe(subject, durable, opts...)
}

func (c *Conn) brokerQueueSubscribe(subj, queue string, cb nats.MsgHandler) (*nats.Subscription, error) {
	return c.brokerConn.QueueSubscribe(subj, queue, cb)
}

func (c *Conn) getSchemaAttachSubject() string {
	return "$memphis_schema_attachments"
}

func (c *Conn) getSchemaDetachSubject() string {
	return "$memphis_schema_detachments"
}

// Port - default is 6666.
func Port(port int) Option {
	return func(o *Options) error {
		o.Port = port
		return nil
	}
}

// Reconnect - whether to do reconnect while connection is lost.
func Reconnect(reconnect bool) Option {
	return func(o *Options) error {
		o.Reconnect = reconnect
		return nil
	}
}

// MaxReconnect - the amount of reconnect attempts.
func MaxReconnect(maxReconnect int) Option {
	return func(o *Options) error {
		o.MaxReconnect = maxReconnect
		return nil
	}
}

// ReconnectInterval - interval in miliseconds between reconnect attempts.
func ReconnectInterval(reconnectInterval time.Duration) Option {
	return func(o *Options) error {
		o.ReconnectInterval = reconnectInterval
		return nil
	}
}

// Timeout - connection timeout in miliseconds.
func Timeout(timeout time.Duration) Option {
	return func(o *Options) error {
		o.Timeout = timeout
		return nil
	}
}

// Tls - paths to tls cert, key and ca files.
func Tls(TlsCert string, TlsKey string, CaFile string) Option {
	return func(o *Options) error {
		o.TLSOpts = TLSOpts{
			TlsCert: TlsCert,
			TlsKey:  TlsKey,
			CaFile:  CaFile,
		}
		return nil
	}
}

type directObj interface {
	getCreationSubject() string
	getCreationReq() any
	handleCreationResp([]byte) error
	getDestructionSubject() string
	getDestructionReq() any
}

func defaultHandleCreationResp(resp []byte) error {
	if len(resp) > 0 {
		return memphisError(errors.New(string(resp)))
	}
	return nil
}

func (c *Conn) create(do directObj) error {
	subject := do.getCreationSubject()
	req := do.getCreationReq()

	b, err := json.Marshal(req)
	if err != nil {
		return memphisError(err)
	}

	msg, err := c.brokerConn.Request(subject, b, 5*time.Second)
	if err != nil {
		return memphisError(err)
	}

	return do.handleCreationResp(msg.Data)
}

func (c *Conn) AttachSchema(name string, stationName string) error {
	subject := c.getSchemaAttachSubject()

	creationReq := &attachSchemaReq{
		Name:        name,
		StationName: stationName,
		Username:    c.username,
	}

	b, err := json.Marshal(creationReq)
	if err != nil {
		return memphisError(err)
	}

	msg, err := c.brokerConn.Request(subject, b, 5*time.Second)
	if err != nil {
		return memphisError(err)
	}
	if len(msg.Data) > 0 {
		return memphisError(errors.New(string(msg.Data)))
	}
	return nil
}

func (c *Conn) DetachSchema(stationName string) error {
	subject := c.getSchemaDetachSubject()

	req := &detachSchemaReq{
		StationName: stationName,
		Username:    c.username,
	}

	b, err := json.Marshal(req)
	if err != nil {
		return memphisError(err)
	}

	msg, err := c.brokerConn.Request(subject, b, 5*time.Second)
	if err != nil {
		return memphisError(err)
	}
	if len(msg.Data) > 0 {
		return memphisError(errors.New(string(msg.Data)))
	}
	return nil
}

func (c *Conn) destroy(o directObj) error {
	subject := o.getDestructionSubject()
	destructionReq := o.getDestructionReq()

	b, err := json.Marshal(destructionReq)
	if err != nil {
		return memphisError(err)
	}

	msg, err := c.brokerConn.Request(subject, b, 5*time.Second)
	if err != nil {
		return memphisError(err)
	}
	if len(msg.Data) > 0 && !strings.Contains(string(msg.Data), "not exist") {
		return memphisError(errors.New(string(msg.Data)))
	}

	return nil
}

func getInternalName(name string) string {
	name = strings.ToLower(name)
	return replaceDelimiters(name)
}

const (
	delimToReplace   = "."
	delimReplacement = "#"
)

func replaceDelimiters(in string) string {
	return strings.Replace(in, delimToReplace, delimReplacement, -1)
}

func (c *Conn) listenToConfigurationUpdates() error {
	c.configUpdatesSub = configurationsUpdateSub{
		ConfigUpdatesCh:            make(chan ConfigurationsUpdate),
		ClusterConfigurations:      make(map[string]bool),
		StationSchemaverseToDlsMap: make(map[string]bool),
	}
	cus := c.configUpdatesSub

	go cus.configurationsUpdatesHandler(&c.configUpdatesMu)
	var err error
	cus.ConfigUpdateSub, err = c.brokerConn.Subscribe(configurationUpdatesSubject, cus.createUpdatesHandler())
	if err != nil {
		close(cus.ConfigUpdatesCh)
		return memphisError(err)
	}

	return nil
}

func (cus *configurationsUpdateSub) createUpdatesHandler() nats.MsgHandler {
	return func(msg *nats.Msg) {
		var update ConfigurationsUpdate
		err := json.Unmarshal(msg.Data, &update)
		if err != nil {
			log.Printf("schema update unmarshal error: %v\n", memphisError(err))
			return
		}
		cus.ConfigUpdatesCh <- update
	}
}

func (cus *configurationsUpdateSub) configurationsUpdatesHandler(lock *sync.RWMutex) {
	for {
		update, ok := <-cus.ConfigUpdatesCh
		if !ok {
			return
		}
		lock.Lock()
		switch update.Type {
		case "send_notification":
			cus.ClusterConfigurations[update.Type] = update.Update
		case "schemaverse_to_dls":
			cus.StationSchemaverseToDlsMap[getInternalName(update.StationName)] = update.Update
		}
		lock.Unlock()
	}
}

func GetDlsSubject(subjType string, stationName string, id string) string {
	return fmt.Sprintf("$memphis-%s-dls", stationName) + "." + subjType + "." + id
}

func GetDlsMsgId(stationName string, producerName string, timeSent string) string {
	// Remove any spaces might be in ID
	msgId := strings.ReplaceAll(stationName+"~"+producerName+"~0~"+timeSent, " ", "")
	msgId = strings.ReplaceAll(msgId, ",", "+")
	return msgId
}
