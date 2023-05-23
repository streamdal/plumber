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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	schemaUpdatesSubjectTemplate   = "$memphis_schema_updates_%s"
	memphisNotificationsSubject    = "$memphis_notifications"
	schemaVFailAlertType           = "schema_validation_fail_alert"
	lastProducerCreationReqVersion = 1
)

// Producer - memphis producer object.
type Producer struct {
	Name        string
	stationName string
	conn        *Conn
}

type createProducerReq struct {
	Name           string `json:"name"`
	StationName    string `json:"station_name"`
	ConnectionId   string `json:"connection_id"`
	ProducerType   string `json:"producer_type"`
	RequestVersion int    `json:"req_version"`
	Username       string `json:"username"`
}

type createProducerResp struct {
	SchemaUpdateInit        SchemaUpdateInit `json:"schema_update"`
	SchemaVerseToDls        bool             `json:"schemaverse_to_dls"`
	ClusterSendNotification bool             `json:"send_notification"`
	Err                     string           `json:"error"`
}

type SchemaUpdateType int

const (
	SchemaUpdateTypeInit SchemaUpdateType = iota + 1
	SchemaUpdateTypeDrop
)

type SchemaUpdate struct {
	UpdateType SchemaUpdateType
	Init       SchemaUpdateInit `json:"init,omitempty"`
}

type SchemaUpdateInit struct {
	SchemaName    string        `json:"schema_name"`
	ActiveVersion SchemaVersion `json:"active_version"`
	SchemaType    string        `json:"type"`
}

type SchemaVersion struct {
	VersionNumber     int    `json:"version_number"`
	Descriptor        string `json:"descriptor"`
	Content           string `json:"schema_content"`
	MessageStructName string `json:"message_struct_name"`
}

type removeProducerReq struct {
	Name        string `json:"name"`
	StationName string `json:"station_name"`
	Username    string `json:"username"`
}

// ProducerOpts - configuration options for producer creation.
type ProducerOpts struct {
	GenUniqueSuffix bool
}

type Notification struct {
	Title string
	Msg   string
	Code  string
	Type  string
}

type DlsMessage struct {
	ID           string            `json:"_id"`
	StationName  string            `json:"station_name"`
	Producer     ProducerDetails   `json:"producer"`
	Message      MessagePayloadDls `json:"message"`
	CreationDate time.Time         `json:"creation_date"`
}

type ProducerDetails struct {
	Name         string `json:"name"`
	ConnectionId string `json:"connection_id"`
}

type MessagePayloadDls struct {
	TimeSent time.Time         `json:"time_sent"`
	Size     int               `json:"size"`
	Data     string            `json:"data"`
	Headers  map[string]string `json:"headers"`
}

// ProducerOpt - a function on the options for producer creation.
type ProducerOpt func(*ProducerOpts) error

// getDefaultProducerOpts - returns default configuration options for producer creation.
func getDefaultProducerOpts() ProducerOpts {
	return ProducerOpts{GenUniqueSuffix: false}
}

func extendNameWithRandSuffix(name string) (string, error) {
	suffix, err := randomHex(4)
	if err != nil {
		return "", memphisError(err)
	}
	return name + "_" + suffix, err
}

// CreateProducer - creates a producer.
func (c *Conn) CreateProducer(stationName, name string, opts ...ProducerOpt) (*Producer, error) {
	defaultOpts := getDefaultProducerOpts()
	var err error
	for _, opt := range opts {
		if err = opt(&defaultOpts); err != nil {
			return nil, memphisError(err)
		}
	}

	if defaultOpts.GenUniqueSuffix {
		name, err = extendNameWithRandSuffix(name)
		if err != nil {
			return nil, memphisError(err)
		}
	}

	p := Producer{
		Name:        name,
		stationName: stationName,
		conn:        c,
	}

	err = c.listenToSchemaUpdates(stationName)
	if err != nil {
		return nil, memphisError(err)
	}

	if err = c.create(&p); err != nil {
		if err := c.removeSchemaUpdatesListener(stationName); err != nil {
			return nil, memphisError(err)
		}
		return nil, memphisError(err)
	}

	return &p, nil
}

// Station.CreateProducer - creates a producer attached to this station.
func (s *Station) CreateProducer(name string, opts ...ProducerOpt) (*Producer, error) {
	return s.conn.CreateProducer(s.Name, name, opts...)
}

func (p *Producer) getCreationSubject() string {
	return "$memphis_producer_creations"
}

func (p *Producer) getCreationReq() any {
	return createProducerReq{
		Name:           p.Name,
		StationName:    p.stationName,
		ConnectionId:   p.conn.ConnId,
		ProducerType:   "application",
		RequestVersion: lastProducerCreationReqVersion,
		Username:       p.conn.username,
	}
}

func (p *Producer) handleCreationResp(resp []byte) error {
	cr := &createProducerResp{}
	err := json.Unmarshal(resp, cr)
	if err != nil {
		// unmarshal failed, we may be dealing with an old broker
		return defaultHandleCreationResp(resp)
	}

	if cr.Err != "" {
		return memphisError(errors.New(cr.Err))
	}

	sn := getInternalName(p.stationName)

	p.conn.stationUpdatesMu.Lock()
	sd := &p.conn.stationUpdatesSubs[sn].schemaDetails
	sd.handleSchemaUpdateInit(cr.SchemaUpdateInit)
	p.conn.stationUpdatesMu.Unlock()

	p.conn.configUpdatesMu.Lock()
	cu := &p.conn.configUpdatesSub
	cu.ClusterConfigurations["send_notification"] = cr.ClusterSendNotification
	cu.StationSchemaverseToDlsMap[sn] = cr.SchemaVerseToDls
	p.conn.configUpdatesMu.Unlock()

	return nil
}

func (p *Producer) getDestructionSubject() string {
	return "$memphis_producer_destructions"
}

func (p *Producer) getDestructionReq() any {
	return removeProducerReq{Name: p.Name, StationName: p.stationName, Username: p.conn.username}
}

// Destroy - destoy this producer.
func (p *Producer) Destroy() error {
	if err := p.conn.removeSchemaUpdatesListener(p.stationName); err != nil {
		return memphisError(err)
	}
	return p.conn.destroy(p)
}

type Headers struct {
	MsgHeaders map[string][]string
}

// ProduceOpts - configuration options for produce operations.
type ProduceOpts struct {
	Message      any
	AckWaitSec   int
	MsgHeaders   Headers
	AsyncProduce bool
}

// ProduceOpt - a function on the options for produce operations.
type ProduceOpt func(*ProduceOpts) error

// getDefaultProduceOpts - returns default configuration options for produce operations.
func getDefaultProduceOpts() ProduceOpts {
	msgHeaders := make(map[string][]string)
	return ProduceOpts{AckWaitSec: 15, MsgHeaders: Headers{MsgHeaders: msgHeaders}, AsyncProduce: false}
}

// Producer.Produce - produces a message into a station. message is of type []byte/protoreflect.ProtoMessage in case it is a schema validated station
func (p *Producer) Produce(message any, opts ...ProduceOpt) error {
	defaultOpts := getDefaultProduceOpts()
	defaultOpts.Message = message

	for _, opt := range opts {
		if opt != nil {
			if err := opt(&defaultOpts); err != nil {
				return memphisError(err)
			}
		}
	}

	return defaultOpts.produce(p)
}

func (hdr *Headers) validateHeaderKey(key string) error {
	if strings.HasPrefix(key, "$memphis") {
		return memphisError(errors.New("Keys in headers should not start with $memphis"))
	}
	return nil
}

func (hdr *Headers) New() {
	hdr.MsgHeaders = map[string][]string{}
}

func (hdr *Headers) Add(key, value string) error {
	err := hdr.validateHeaderKey(key)
	if err != nil {
		return memphisError(err)
	}

	hdr.MsgHeaders[key] = []string{value}
	return nil
}

// ProducerOpts.produce - produces a message into a station using a configuration struct.
func (opts *ProduceOpts) produce(p *Producer) error {
	opts.MsgHeaders.MsgHeaders["$memphis_connectionId"] = []string{p.conn.ConnId}
	opts.MsgHeaders.MsgHeaders["$memphis_producedBy"] = []string{p.Name}

	data, err := p.validateMsg(opts.Message, opts.MsgHeaders.MsgHeaders)
	if err != nil {
		return memphisError(err)
	}

	natsMessage := nats.Msg{
		Header:  opts.MsgHeaders.MsgHeaders,
		Subject: getInternalName(p.stationName) + ".final",
		Data:    data,
	}

	stallWaitDuration := time.Second * time.Duration(opts.AckWaitSec)
	paf, err := p.conn.brokerPublish(&natsMessage, nats.StallWait(stallWaitDuration))
	if err != nil {
		return memphisError(err)
	}

	if opts.AsyncProduce {
		return nil
	}

	select {
	case <-paf.Ok():
		return nil
	case err = <-paf.Err():
		return memphisError(err)
	}
}

func (p *Producer) sendNotification(title string, msg string, code string, msgType string) {
	notification := Notification{
		Title: title,
		Msg:   msg,
		Type:  msgType,
		Code:  code,
	}
	msgToPublish, _ := json.Marshal(notification)

	_ = p.conn.brokerConn.Publish(memphisNotificationsSubject, msgToPublish)
}

func (p *Producer) msgToString(msg any) string {
	var stringMsg string
	switch msg.(type) {
	case []byte:
		stringMsg = string(msg.([]byte)[:])
	default:
		stringMsg = fmt.Sprintf("%v", msg)
	}

	return stringMsg
}

func (p *Producer) sendMsgToDls(msg any, headers map[string][]string, err error) {
	internStation := getInternalName(p.stationName)
	if p.conn.configUpdatesSub.StationSchemaverseToDlsMap[internStation] {
		msgToSend := p.msgToString(msg)
		timeSent := time.Now()
		id := GetDlsMsgId(internStation, p.Name, time.Now().String())
		headersForDls := make(map[string]string)
		for k, v := range headers {
			concat := strings.Join(v, " ")
			headersForDls[k] = concat
		}
		schemaFailMsg := &DlsMessage{
			ID:          id,
			StationName: internStation,
			Producer: ProducerDetails{
				Name:         p.Name,
				ConnectionId: p.conn.ConnId,
			},
			Message: MessagePayloadDls{
				TimeSent: timeSent,
				Data:     hex.EncodeToString([]byte(msgToSend)),
				Headers:  headersForDls,
			},
			CreationDate: timeSent,
		}
		msgToPublish, _ := json.Marshal(schemaFailMsg)
		_ = p.conn.brokerConn.Publish(GetDlsSubject("schema", internStation, id), msgToPublish)

		if p.conn.configUpdatesSub.ClusterConfigurations["send_notification"] {
			p.sendNotification("Schema validation has failed", "Station: "+p.stationName+"\nProducer: "+p.Name+"\nError: "+err.Error(), msgToSend, schemaVFailAlertType)
		}
	}
}

func (p *Producer) validateMsg(msg any, headers map[string][]string) ([]byte, error) {
	sd, err := p.getSchemaDetails()
	if err != nil {
		return nil, memphisError(errors.New("Schema validation has failed: " + err.Error()))
	}

	// empty schema type means there is no schema and validation is not needed
	// so we just verify the type is byte slice or map[string]interface{}
	if sd.schemaType == "" {
		switch msg.(type) {
		case []byte:
			return msg.([]byte), nil
		case map[string]interface{}:
			return json.Marshal(msg)
		default:
			return nil, memphisError(errors.New("Unsupported message type"))
		}

	}

	msgBytes, err := sd.validateMsg(msg)
	if err != nil {
		p.sendMsgToDls(msg, headers, err)
		return nil, memphisError(errors.New("Schema validation has failed: " + err.Error()))
	}

	return msgBytes, nil
}

func (p *Producer) getSchemaDetails() (schemaDetails, error) {
	return p.conn.getSchemaDetails(p.stationName)
}

// ProducerGenUniqueSuffix - whether to generate a unique suffix for this producer.
func ProducerGenUniqueSuffix() ProducerOpt {
	return func(opts *ProducerOpts) error {
		opts.GenUniqueSuffix = true
		return nil
	}
}

// AckWaitSec - max time in seconds to wait for an ack from memphis.
func AckWaitSec(ackWaitSec int) ProduceOpt {
	return func(opts *ProduceOpts) error {
		opts.AckWaitSec = ackWaitSec
		return nil
	}
}

// MsgHeaders - set headers to a message
func MsgHeaders(hdrs Headers) ProduceOpt {
	return func(opts *ProduceOpts) error {
		opts.MsgHeaders = hdrs
		return nil
	}
}

// AsyncProduce - produce operation won't wait for broker acknowledgement
func AsyncProduce() ProduceOpt {
	return func(opts *ProduceOpts) error {
		opts.AsyncProduce = true
		return nil
	}
}

func MsgId(id string) ProduceOpt {
	return func(opts *ProduceOpts) error {
		opts.MsgHeaders.MsgHeaders["msg-id"] = []string{id}
		return nil
	}
}
