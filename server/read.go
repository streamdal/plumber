package server

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/reader"
	"github.com/batchcorp/plumber/server/types"
)

// beginRead is a goroutine that is launched when a read is started. It will continue running until plumber exits
// or a read is stopped via the API
func (s *Server) beginRead(ctx context.Context, r *types.Read) {
	r.Log.Warn("Starting read")
	defer r.Backend.Close(ctx)

	r.ReadOptions.XActive = true

	resultsCh := make(chan *records.ReadRecord, 1)
	errorCh := make(chan *records.ErrorRecord, 1)

	connCounter := s.StatsService.AddCounter(&opts.Counter{
		Resource:   opts.Counter_RESOURCE_CONNECTION,
		Type:       opts.Counter_TYPE_MESSAGE_RECEIVED,
		ResourceId: r.ReadOptions.ConnectionId,
	})

	readCounter := s.StatsService.AddCounter(&opts.Counter{
		Resource:   opts.Counter_RESOURCE_READ,
		Type:       opts.Counter_TYPE_MESSAGE_RECEIVED,
		ResourceId: r.ReadOptions.XId,
	})

	go func() {
		if err := r.Backend.Read(ctx, r.ReadOptions, resultsCh, errorCh); err != nil {
			errorCh <- &records.ErrorRecord{
				OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
				Error:               err.Error(),
			}
		}
	}()

MAIN:
	for {

		select {
		case readRecord := <-resultsCh:

			readCounter.Incr(1)
			connCounter.Incr(1)

			// TODO: Implement a decoder pipeline -- decoding mid-flight without
			//   a pipeline will cause things to slow down starting at 100 msgs/s

			// Decode the msg
			decodedPayload, err := reader.Decode(r.ReadOptions, r.MsgDesc, readRecord.Payload)
			if err != nil {
				// TODO: need to send the err back to the client somehow
				r.Log.Errorf("unable to decode msg for backend '%s': %s", r.Backend.Name(), err)
				continue
			}

			s.InferJSONSchema(r, readRecord)

			// Message is JSON, perform query if needed
			//filter := r.ReadOptions.GetFilter()
			//if filter != nil && gjson.ValidBytes(decodedPayload) {
			//	found, err := jsonquery.Find(decodedPayload, filter.Query)
			//	if err != nil {
			//		r.Log.Errorf("Could not query payload, message ignored: %s", err)
			//		continue
			//	}
			//	if !found {
			//		r.Log.Debug("Message did not match filter, dropping")
			//		continue
			//	}
			//}

			// Update read record with (maybe) decoded payload
			readRecord.Payload = decodedPayload

			// Send message payload to all attached streams
			r.AttachedClientsMutex.RLock()

			for id, s := range r.AttachedClients {
				r.Log.Debugf("Read message to stream '%s'", id)
				s.MessageCh <- readRecord
			}

			r.AttachedClientsMutex.RUnlock()
		case errRecord := <-errorCh:
			r.Log.Errorf("IMPORTANT: Received an error from reader: %+v", errRecord)
			s.ErrorsService.AddError(&protos.ErrorMessage{
				Resource:   "read",
				ResourceId: r.ReadOptions.XId,
				Error:      errRecord.Error,
			})
		case <-r.ContextCxl.Done():
			r.Log.Info("Read stopped")
			break MAIN
		}
	}

	r.Log.Debugf("reader id '%s' exiting", r.ReadOptions.XId)
}

func (s *Server) InferJSONSchema(r *types.Read, readRecord *records.ReadRecord) error {
	//inferOpts := r.ReadOptions.InferSchemaOptions
	//if inferOpts == nil {
	//	return nil
	//}
	//
	//var isNewSchema bool
	//var schema *protos.Schema
	//
	//if inferOpts.SchemaId != "" {
	//	// TODO: figure this out. We need to only get this once and store on the read, otherwise we will
	//	// TODO: be making an extra mutex hit for every single message on the read
	//	schema = s.PersistentConfig.GetSchema(inferOpts.SchemaId)
	//}
	//
	//// Either the schema was not found, or we're making a fresh new one
	//if schema == nil {
	//	isNewSchema = true
	//	schema = createJSONSchemaDef(r.ReadOptions.Name, "") // TODO: ownerID
	//}
	//
	//var inferred *jsonschema.JSONSchema
	//var err error
	//inferred, err = jsonschema.InferFromJSON("plumber.local/"+r.ReadOptions.Name+".json", "Inferred schema for "+r.ReadOptions.Name, "", readRecord.Payload)
	//if err != nil {
	//	return errors.Wrap(err, "unable to infer schema from payload")
	//}
	//
	//latestVersion := getLatestSchemaVersion(schema)
	//
	//if latestVersion != nil {
	//	currentSchema, err := jsonschema.Unmarshal(latestVersion.GetJsonSchemaSettings().Schema)
	//	if err != nil {
	//		return errors.Wrap(err, "unable to unmarshal existing JSONSchema schema")
	//	}
	//
	//	mergedSchema, err := jsonschema.Merge(*currentSchema, *inferred)
	//	if err != nil {
	//		return errors.Wrap(err, "could not update JSONSchema schema")
	//	}
	//
	//	inferred = mergedSchema
	//}
	//
	//schemaBytes, err := json.Marshal(inferred)
	//if err != nil {
	//	return errors.Wrap(err, "unable to marshal inferred schema to JSON")
	//}
	//
	//newVersion := &protos.SchemaVersion{
	//	Files: map[string]string{
	//		"schema.json": string(schemaBytes),
	//	},
	//	Status:  protos.SchemaStatus_SCHEMA_STATUS_PROPOSED,
	//	Version: latestVersion.Version + 1,
	//	Settings: &protos.SchemaVersion_JsonSchemaSettings{
	//		JsonSchemaSettings: &encoding.JSONSchemaSettings{
	//			Schema: schemaBytes,
	//		},
	//	},
	//}
	//
	//schema.Versions = append(schema.Versions, newVersion)
	//s.PersistentConfig.SetSchema(schema.Id, schema)
	//
	//// Running in goroutine since this can take a bit
	//go s.persistSchema(r.ContextCxl, isNewSchema, schema)

	return nil
}

// createJSONSchemaDef is a helper method to keep InferJSONSchema smaller. It returns a protos.Schema struct with
// necessary fields filled out
func createJSONSchemaDef(name, ownerID string) *protos.Schema {
	return &protos.Schema{
		Id:      uuid.NewV4().String(),
		Name:    "Inferred schema from read " + name,
		Type:    protos.SchemaType_SCHEMA_TYPE_JSONSCHEMA,
		OwnerId: ownerID,
		Notes:   "Automatically generated by Plumber",
		Versions: []*protos.SchemaVersion{
			{
				Status:  protos.SchemaStatus_SCHEMA_STATUS_PROPOSED,
				Version: 1,
				Files: map[string]string{

					"schema.json": "",
				},
				Settings: &protos.SchemaVersion_JsonSchemaSettings{
					JsonSchemaSettings: &encoding.JSONSchemaSettings{
						Schema: nil,
					},
				},
			},
		},
	}
}

// getLatestSchemaVersion retrieves the most recent schema version
func getLatestSchemaVersion(schema *protos.Schema) *protos.SchemaVersion {
	if schema.Versions == nil || len(schema.Versions) == 0 {
		return nil
	}

	return schema.Versions[len(schema.Versions)-1]
}

// persistSchema will persist a schema to etcd and publish events so that other plumber instances will
// receive the new or updated schema
func (s *Server) persistSchema(ctx context.Context, newSchema bool, schema *protos.Schema) {
	if newSchema {
		// Publish CreateSchema message
		if err := s.Etcd.PublishCreateSchema(ctx, schema); err != nil {
			err = errors.Wrap(err, "unable to publish CreateSchema message")
			s.Log.Error(err)
			return
		}
	} else {
		// Publish CreateSchema message
		if err := s.Etcd.PublishUpdateSchema(ctx, schema); err != nil {
			err = errors.Wrap(err, "unable to publish UpdateSchema message")
			s.Log.Error(err)
			return
		}
	}

	data, err := proto.Marshal(schema)
	if err != nil {
		err = errors.Wrap(err, "unable to marshal schema proto")
		s.Log.Error(err)
		return
	}

	// Save to etcd
	_, err = s.Etcd.Put(ctx, "/plumber-server/schemas"+"/"+schema.Id, string(data))
	if err != nil {
		err = errors.Wrapf(err, "unable to save schema '%s' to etcd", schema.Id)
		s.Log.Error(err)
		return
	}
}
