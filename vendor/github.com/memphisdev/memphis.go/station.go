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
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	graphqlParse "github.com/graph-gophers/graphql-go"
	"github.com/santhosh-tekuri/jsonschema/v5"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Station - memphis station object.
type Station struct {
	Name              string
	RetentionType     RetentionType
	RetentionValue    int
	StorageType       StorageType
	Replicas          int
	IdempotencyWindow time.Duration
	conn              *Conn
	SchemaName        string
	DlsConfiguration  dlsConfiguration
}

// RetentionType - station's message retention type
type RetentionType int

const (
	MaxMessageAgeSeconds RetentionType = iota
	Messages
	Bytes
)

func (r RetentionType) String() string {
	return [...]string{"message_age_sec", "messages", "bytes"}[r]
}

// StorageType - station's message storage type
type StorageType int

const (
	Disk StorageType = iota
	Memory
)

func (s StorageType) String() string {
	return [...]string{"file", "memory"}[s]
}

type createStationReq struct {
	Name                    string           `json:"name"`
	RetentionType           string           `json:"retention_type"`
	RetentionValue          int              `json:"retention_value"`
	StorageType             string           `json:"storage_type"`
	Replicas                int              `json:"replicas"`
	IdempotencyWindowMillis int              `json:"idempotency_window_in_ms"`
	SchemaName              string           `json:"schema_name"`
	DlsConfiguration        dlsConfiguration `json:"dls_configuration"`
	Username                string           `json:"username"`
}

type removeStationReq struct {
	Name     string `json:"station_name"`
	Username string `json:"username"`
}

// StationsOpts - configuration options for a station.
type StationOpts struct {
	Name                     string
	RetentionType            RetentionType
	RetentionVal             int
	StorageType              StorageType
	Replicas                 int
	IdempotencyWindow        time.Duration
	SchemaName               string
	SendPoisonMsgToDls       bool
	SendSchemaFailedMsgToDls bool
}

type dlsConfiguration struct {
	Poison      bool `json:"poison"`
	Schemaverse bool `json:"schemaverse"`
}

// StationOpt - a function on the options for a station.
type StationOpt func(*StationOpts) error

// GetStationDefaultOptions - returns default configuration options for the station.
func GetStationDefaultOptions() StationOpts {
	return StationOpts{
		RetentionType:            MaxMessageAgeSeconds,
		RetentionVal:             604800,
		StorageType:              Disk,
		Replicas:                 1,
		IdempotencyWindow:        2 * time.Minute,
		SchemaName:               "",
		SendPoisonMsgToDls:       true,
		SendSchemaFailedMsgToDls: true,
	}
}

func (c *Conn) CreateStation(Name string, opts ...StationOpt) (*Station, error) {
	defaultOpts := GetStationDefaultOptions()

	defaultOpts.Name = Name

	for _, opt := range opts {
		if opt != nil {
			if err := opt(&defaultOpts); err != nil {
				return nil, memphisError(err)
			}
		}
	}
	res, err := defaultOpts.createStation(c)
	if err != nil && strings.Contains(err.Error(), "already exist") {
		return res, nil
	}
	return res, memphisError(err)
}

func (opts *StationOpts) createStation(c *Conn) (*Station, error) {
	s := Station{
		Name:              opts.Name,
		RetentionType:     opts.RetentionType,
		RetentionValue:    opts.RetentionVal,
		StorageType:       opts.StorageType,
		Replicas:          opts.Replicas,
		IdempotencyWindow: opts.IdempotencyWindow,
		conn:              c,
		SchemaName:        opts.SchemaName,
		DlsConfiguration: dlsConfiguration{
			Poison:      opts.SendPoisonMsgToDls,
			Schemaverse: opts.SendSchemaFailedMsgToDls,
		},
	}

	return &s, s.conn.create(&s)

}

type StationName string

func (s *Station) Destroy() error {
	return s.conn.destroy(s)
}

func (s *Station) getCreationSubject() string {
	return "$memphis_station_creations"
}

func (s *Station) getSchemaDetachSubject() string {
	return "$memphis_schema_detachments"
}

func (s *Station) getCreationReq() any {
	return createStationReq{
		Name:                    s.Name,
		RetentionType:           s.RetentionType.String(),
		RetentionValue:          s.RetentionValue,
		StorageType:             s.StorageType.String(),
		Replicas:                s.Replicas,
		IdempotencyWindowMillis: int(s.IdempotencyWindow.Milliseconds()),
		SchemaName:              s.SchemaName,
		DlsConfiguration:        s.DlsConfiguration,
		Username:                s.conn.username,
	}
}

func (s *Station) handleCreationResp(resp []byte) error {
	return defaultHandleCreationResp(resp)
}

func (s *Station) getDestructionSubject() string {
	return "$memphis_station_destructions"
}

func (s *Station) getDestructionReq() any {
	return removeStationReq{Name: s.Name, Username: s.conn.username}
}

// Name - station's name
func Name(name string) StationOpt {
	return func(opts *StationOpts) error {
		opts.Name = name
		return nil
	}
}

// SchemaName - shcema's name to attach
func SchemaName(schemaName string) StationOpt {
	return func(opts *StationOpts) error {
		opts.SchemaName = schemaName
		return nil
	}
}

// RetentionTypeOpt - retention type, default is MaxMessageAgeSeconds.
func RetentionTypeOpt(retentionType RetentionType) StationOpt {
	return func(opts *StationOpts) error {
		opts.RetentionType = retentionType
		return nil
	}
}

// RetentionVal -  number which represents the retention based on the retentionType, default is 604800.
func RetentionVal(retentionVal int) StationOpt {
	return func(opts *StationOpts) error {
		opts.RetentionVal = retentionVal
		return nil
	}
}

// StorageTypeOpt - persistance storage for messages of the station, default is storageTypes.FILE.
func StorageTypeOpt(storageType StorageType) StationOpt {
	return func(opts *StationOpts) error {
		opts.StorageType = storageType
		return nil
	}
}

// Replicas - number of replicas for the messages of the data, default is 1.
func Replicas(replicas int) StationOpt {
	return func(opts *StationOpts) error {
		opts.Replicas = replicas
		return nil
	}
}

// IdempotencyWindow - time frame in which idempotency track messages, default is 2 minutes. This feature is enabled only for messages contain Msg Id
func IdempotencyWindow(idempotencyWindow time.Duration) StationOpt {
	return func(opts *StationOpts) error {
		opts.IdempotencyWindow = idempotencyWindow
		return nil
	}
}

// SendPoisonMsgToDls - send poison message to dls, default is true
func SendPoisonMsgToDls(sendPoisonMsgToDls bool) StationOpt {
	return func(opts *StationOpts) error {
		opts.SendPoisonMsgToDls = sendPoisonMsgToDls
		return nil
	}
}

// SendSchemaFailedMsgToDls - send message to dls after schema validation fail, default is true
func SendSchemaFailedMsgToDls(sendSchemaFailedMsgToDls bool) StationOpt {
	return func(opts *StationOpts) error {
		opts.SendSchemaFailedMsgToDls = sendSchemaFailedMsgToDls
		return nil
	}
}

// Station schema updates related

type stationUpdateSub struct {
	refCount        int
	schemaUpdateCh  chan SchemaUpdate
	schemaUpdateSub *nats.Subscription
	schemaDetails   schemaDetails
}

type schemaDetails struct {
	name          string
	schemaType    string
	activeVersion SchemaVersion
	msgDescriptor protoreflect.MessageDescriptor
	jsonSchema    *jsonschema.Schema
	graphQlSchema *graphqlParse.Schema
}

func (c *Conn) listenToSchemaUpdates(stationName string) error {
	sn := getInternalName(stationName)
	sus, ok := c.stationUpdatesSubs[sn]
	if !ok {
		c.stationUpdatesSubs[sn] = &stationUpdateSub{
			refCount:       1,
			schemaUpdateCh: make(chan SchemaUpdate),
			schemaDetails:  schemaDetails{},
		}
		sus := c.stationUpdatesSubs[sn]
		schemaUpdatesSubject := fmt.Sprintf(schemaUpdatesSubjectTemplate, sn)
		go sus.schemaUpdatesHandler(&c.stationUpdatesMu)
		var err error
		sus.schemaUpdateSub, err = c.brokerConn.Subscribe(schemaUpdatesSubject, sus.createMsgHandler())
		if err != nil {
			close(sus.schemaUpdateCh)
			return memphisError(err)
		}

		return nil
	}
	sus.refCount++
	return nil
}

func (sus *stationUpdateSub) createMsgHandler() nats.MsgHandler {
	return func(msg *nats.Msg) {
		var update SchemaUpdate
		err := json.Unmarshal(msg.Data, &update)
		if err != nil {
			log.Printf("schema update unmarshal error: %v\n", memphisError(err))
			return
		}
		sus.schemaUpdateCh <- update
	}
}

func (c *Conn) removeSchemaUpdatesListener(stationName string) error {
	sn := getInternalName(stationName)

	c.stationUpdatesMu.Lock()
	defer c.stationUpdatesMu.Unlock()

	sus, ok := c.stationUpdatesSubs[sn]
	if !ok {
		return memphisError(errors.New("listener doesn't exist"))
	}

	sus.refCount--
	if sus.refCount <= 0 {
		close(sus.schemaUpdateCh)
		if err := sus.schemaUpdateSub.Unsubscribe(); err != nil {
			return memphisError(err)
		}
		delete(c.stationUpdatesSubs, sn)
	}

	return nil
}

func (c *Conn) getSchemaDetails(stationName string) (schemaDetails, error) {
	sn := getInternalName(stationName)

	c.stationUpdatesMu.RLock()
	defer c.stationUpdatesMu.RUnlock()

	sus, ok := c.stationUpdatesSubs[sn]
	if !ok {
		return schemaDetails{}, memphisError(errors.New("station subscription doesn't exist"))
	}

	return sus.schemaDetails, nil
}

func (sus *stationUpdateSub) schemaUpdatesHandler(lock *sync.RWMutex) {
	for {
		update, ok := <-sus.schemaUpdateCh
		if !ok {
			return
		}

		lock.Lock()
		sd := &sus.schemaDetails
		switch update.UpdateType {
		case SchemaUpdateTypeInit:
			sd.handleSchemaUpdateInit(update.Init)
		case SchemaUpdateTypeDrop:
			sd.handleSchemaUpdateDrop()
		}
		lock.Unlock()
	}
}

func (sd *schemaDetails) handleSchemaUpdateInit(sui SchemaUpdateInit) {
	sd.name = sui.SchemaName
	sd.schemaType = sui.SchemaType
	sd.activeVersion = sui.ActiveVersion
	if sd.schemaType == "protobuf" {
		if err := sd.compileDescriptor(); err != nil {
			log.Println(err.Error())
		}
	} else if sd.schemaType == "json" {
		if err := sd.compileJsonSchema(); err != nil {
			log.Println(err.Error())
		}
	} else if sd.schemaType == "graphql" {
		if err := sd.compileGraphQl(); err != nil {
			log.Println(err.Error())
		}
	}
}

func (sd *schemaDetails) handleSchemaUpdateDrop() {
	*sd = schemaDetails{}
}

func (sd *schemaDetails) compileDescriptor() error {
	descriptorSet := descriptorpb.FileDescriptorSet{}
	err := proto.Unmarshal([]byte(sd.activeVersion.Descriptor), &descriptorSet)
	if err != nil {
		return memphisError(err)
	}

	localRegistry, err := protodesc.NewFiles(&descriptorSet)
	if err != nil {
		return memphisError(err)
	}

	filePath := fmt.Sprintf("%v_%v.proto", sd.name, sd.activeVersion.VersionNumber)
	fileDesc, err := localRegistry.FindFileByPath(filePath)
	if err != nil {
		return memphisError(err)
	}

	msgsDesc := fileDesc.Messages()
	msgDesc := msgsDesc.ByName(protoreflect.Name(sd.activeVersion.MessageStructName))

	sd.msgDescriptor = msgDesc
	return nil
}

func (sd *schemaDetails) compileJsonSchema() error {
	sch, err := jsonschema.CompileString(sd.name, sd.activeVersion.Content)
	if err != nil {
		return memphisError(err)
	}
	sd.jsonSchema = sch
	return nil
}

func (sd *schemaDetails) compileGraphQl() error {
	schemaContent := sd.activeVersion.Content
	schemaGraphQl, err := graphqlParse.ParseSchema(schemaContent, nil)
	if err != nil {
		return memphisError(err)
	}
	sd.graphQlSchema = schemaGraphQl
	return nil
}

func (sd *schemaDetails) validateMsg(msg any) ([]byte, error) {
	switch sd.schemaType {
	case "protobuf":
		return sd.validateProtoMsg(msg)
	case "json":
		return sd.validJsonSchemaMsg(msg)
	case "graphql":
		return sd.validateGraphQlMsg(msg)
	default:
		return nil, memphisError(errors.New("Invalid schema type"))
	}
}

func (sd *schemaDetails) validateProtoMsg(msg any) ([]byte, error) {
	var (
		msgBytes []byte
		err      error
	)
	switch msg.(type) {
	case protoreflect.ProtoMessage:
		msgBytes, err = proto.Marshal(msg.(protoreflect.ProtoMessage))
		if err != nil {
			return nil, memphisError(err)
		}
	case []byte:
		msgBytes = msg.([]byte)
	case map[string]interface{}:
		bytes, err := json.Marshal(msg)
		if err != nil {
			return nil, err
		}
		pMsg := dynamicpb.NewMessage(sd.msgDescriptor)
		err = protojson.Unmarshal(bytes, pMsg)
		if err != nil {
			return nil, memphisError(err)
		}
		msgBytes, err = proto.Marshal(pMsg)
		if err != nil {
			return nil, memphisError(err)
		}
	default:
		return nil, memphisError(errors.New("Unsupported message type"))
	}

	protoMsg := dynamicpb.NewMessage(sd.msgDescriptor)
	err = proto.Unmarshal(msgBytes, protoMsg)
	if err != nil {
		if strings.Contains(err.Error(), "cannot parse invalid wire-format data") {
			err = errors.New("Invalid message format, expecting protobuf")
		}
		return nil, memphisError(err)
	}

	return msgBytes, nil
}

func (sd *schemaDetails) validJsonSchemaMsg(msg any) ([]byte, error) {
	var (
		msgBytes []byte
		err      error
		message  interface{}
	)

	switch msg.(type) {
	case []byte:
		msgBytes = msg.([]byte)
		if err := json.Unmarshal(msgBytes, &message); err != nil {
			err = errors.New("Bad JSON format - " + err.Error())
			return nil, memphisError(err)
		}
	case map[string]interface{}:
		message = msg
		msgBytes, err = json.Marshal(msg)
		if err != nil {
			return nil, memphisError(err)
		}

	default:
		msgType := reflect.TypeOf(msg).Kind()
		if msgType == reflect.Struct {
			msgBytes, err = json.Marshal(msg)
			if err != nil {
				return nil, memphisError(err)
			}
			if err := json.Unmarshal(msgBytes, &message); err != nil {
				return nil, memphisError(err)
			}
		} else {
			return nil, memphisError(errors.New("Unsupported message type"))
		}
	}
	if err = sd.jsonSchema.Validate(message); err != nil {
		return nil, memphisError(err)
	}

	return msgBytes, nil
}

func (sd *schemaDetails) validateGraphQlMsg(msg any) ([]byte, error) {
	var (
		msgBytes []byte
		err      error
		message  string
	)

	switch msg.(type) {
	case string:
		message = fmt.Sprintf("%v", msg)
		msgBytes, err = json.Marshal(msg)
		if err != nil {
			return nil, memphisError(err)
		}
	case []byte:
		msgBytes = msg.([]byte)
		message = string(msgBytes)
	}

	validateResult := sd.graphQlSchema.Validate(message)
	var validateErrorGql string
	if len(validateResult) > 0 {
		var validateErrors []string
		for _, graphQlErr := range validateResult {
			validateErrors = append(validateErrors, graphQlErr.Error())
			var resultErr string
			validateErrorGql = strings.Join(validateErrors, resultErr)
		}
		if strings.Contains(validateErrorGql, "syntax error") {
			return nil, memphisError(errors.New("Invalid message format, expecting GraphQL"))
		}

		return nil, memphisError(errors.New(validateErrorGql))
	}
	return msgBytes, nil
}
