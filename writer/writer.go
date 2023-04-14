package writer

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/serializers"
)

// GenerateWriteValue generates a slice of WriteRecords that can be passed to
// backends to perform a write.
func GenerateWriteValue(writeOpts *opts.WriteOptions, fds *dpb.FileDescriptorSet) ([]*records.WriteRecord, error) {
	writeValues := make([]*records.WriteRecord, 0)

	if writeOpts == nil {
		return nil, errors.New("write opts cannot be nil")
	}

	if writeOpts.Record == nil {
		return nil, errors.New("write opts record cannot be nil")
	}

	// Input already provided
	if writeOpts.Record.Input != "" {
		wv, err := generateWriteValue([]byte(writeOpts.Record.Input), writeOpts, fds)
		if err != nil {
			return nil, err
		}

		writeValues = append(writeValues, &records.WriteRecord{
			Input:         string(wv),
			InputMetadata: writeOpts.Record.InputMetadata,
		})

		return writeValues, nil
	}

	// If server, the only possible input is Record.Input
	if writeOpts.XCliOptions == nil {
		return nil, errors.New("no input found - unable to generate write value")
	}

	// File source
	if writeOpts.XCliOptions.InputFile != "" {
		data, err := os.ReadFile(writeOpts.XCliOptions.InputFile)
		if err != nil {
			return nil, fmt.Errorf("unable to read file '%s': %s", writeOpts.XCliOptions.InputFile, err)
		}

		wv, err := generateWriteValue(data, writeOpts, fds)
		if err != nil {
			return nil, err
		}

		writeValues = append(writeValues, &records.WriteRecord{
			Input:         string(wv),
			InputMetadata: writeOpts.Record.InputMetadata,
		})

		return writeValues, nil
	}

	// TODO: This is kind of lame - stdin should probably just go straight into
	// WriteOpts.Record.Input (and make Input into repeated string)

	// Stdin source
	for _, data := range writeOpts.XCliOptions.InputStdin {
		wv, err := generateWriteValue([]byte(data), writeOpts, fds)
		if err != nil {
			return nil, err
		}

		writeValues = append(writeValues, &records.WriteRecord{
			Input:         string(wv),
			InputMetadata: writeOpts.Record.InputMetadata,
		})
	}

	if len(writeValues) == 0 {
		return nil, errors.New("exhausted all input sources - no input found")
	}

	return writeValues, nil
}

// generateWriteValue will transform input data into the required format for transmission
func generateWriteValue(data []byte, writeOpts *opts.WriteOptions, fds *dpb.FileDescriptorSet) ([]byte, error) {
	// Input: Plain / unset
	if writeOpts.EncodeOptions == nil ||
		writeOpts.EncodeOptions.EncodeType == encoding.EncodeType_ENCODE_TYPE_UNSET ||
		writeOpts.EncodeOptions.EncodeType == encoding.EncodeType_ENCODE_TYPE_CLOUDEVENT {
		return data, nil
	}

	// Input: AVRO
	if writeOpts.EncodeOptions.EncodeType == encoding.EncodeType_ENCODE_TYPE_AVRO {
		var encoded []byte
		var err error

		avroOpts := writeOpts.EncodeOptions.AvroSettings

		if len(avroOpts.Schema) > 0 {
			// Schema passed by server either in the request or it
			// was retrieved from cache and inserted into the request
			encoded, err = serializers.AvroEncode(avroOpts.Schema, data)
		} else if avroOpts.AvroSchemaFile != "" {
			// Local file
			encoded, err = serializers.AvroEncodeWithSchemaFile(writeOpts.EncodeOptions.AvroSettings.AvroSchemaFile, data)
		}
		if err != nil {
			return nil, errors.Wrap(err, "unable to encode data in avro format")
		}

		// Since AWS SQS works with strings only, we must convert it to base64
		if writeOpts.AwsSqs != nil && writeOpts.AwsSqs.Args.QueueName != "" {
			b64 := base64.StdEncoding.EncodeToString(encoded)
			return []byte(b64), nil
		}

		return data, nil
	}

	// Input: JSONPB
	if writeOpts.EncodeOptions.EncodeType == encoding.EncodeType_ENCODE_TYPE_JSONPB {
		out, convertErr := ConvertJSONPBToProtobuf(data, writeOpts, fds)
		if convertErr != nil {
			return nil, errors.Wrap(convertErr, "unable to convert JSONPB to protobuf")
		}

		// Since AWS SQS works with strings only, we must convert it to base64
		if writeOpts.AwsSqs != nil && writeOpts.AwsSqs.Args.QueueName != "" {
			encoded := base64.StdEncoding.EncodeToString(out)
			return []byte(encoded), nil
		}

		return out, nil
	}

	return nil, errors.New("unsupported --input-type")
}

// ConvertJSONPBToProtobuf converts input data from jsonpb -> protobuf -> bytes
func ConvertJSONPBToProtobuf(data []byte, writeOpts *opts.WriteOptions, fds *dpb.FileDescriptorSet) ([]byte, error) {
	// TODO: memoize these so we don't have to do this for every message
	descriptors, err := pb.GetAllMessageDescriptorsInFDS(fds)
	if err != nil {
		return nil, err
	}

	protoOpts := writeOpts.EncodeOptions.ProtobufSettings

	envelopeMD, err := pb.FindMessageDescriptorInFDS(fds, protoOpts.ProtobufRootMessage)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to find envelope message descriptor '%s'", protoOpts.ProtobufRootMessage)
	}

	mf := dynamic.NewMessageFactoryWithDefaults()
	resolver := dynamic.AnyResolver(mf, descriptors...)
	unmarshaler := &jsonpb.Unmarshaler{AnyResolver: resolver}

	envelope := mf.NewDynamicMessage(envelopeMD)
	if envelope == nil {
		return nil, errors.New("BUG: Got a nil message from dynamic.NewDynamicMessage")
	}

	if protoOpts.ProtobufEnvelopeType == encoding.EnvelopeType_ENVELOPE_TYPE_SHALLOW {
		// Get payload MD
		payloadMD, err := pb.FindMessageDescriptorInFDS(fds, protoOpts.ShallowEnvelopeMessage)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to find payload message descriptor '%s'", protoOpts.ShallowEnvelopeMessage)
		}

		payload := mf.NewDynamicMessage(payloadMD)

		// Get field name
		field := envelope.FindFieldDescriptor(protoOpts.ShallowEnvelopeFieldNumber)
		if field == nil {
			return nil, fmt.Errorf("unable to find protobuf field '%d' in envelope message", protoOpts.ShallowEnvelopeFieldNumber)
		}

		return convertJSONPBToProtobufShallow(data, envelope, payload, protoOpts.ShallowEnvelopeFieldNumber)
	}

	// Unmarshal JSON into protobuf message
	if err := unmarshaler.Unmarshal(bytes.NewReader(data), envelope); err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal envelope")
	}

	// Re-encode to bytes as if it just came in to collectors
	out, err := proto.Marshal(envelope)
	if err != nil {
		return nil, errors.Wrap(err, "unable to marshal protobuf message")
	}

	return out, nil
}

// convertJSONPBToProtobufShallow converts a JSONPB message to a protobuf message using shallow envelope schemas
func convertJSONPBToProtobufShallow(data []byte, envelope, payload *dynamic.Message, payloadFieldID int32) ([]byte, error) {
	// Get field name
	field := envelope.FindFieldDescriptor(payloadFieldID)
	if field == nil {
		return nil, fmt.Errorf("unable to find protobuf field '%d' in envelope message", payloadFieldID)
	}

	// Unmarshal payload into map. We need to extract the contents of the shallow payload field
	// and then remove it from the envelope JSON so that both the envelope JSON and payload JSON
	// can be unmarshaled into dynamic.Message type
	tmpJSON := make(map[string]interface{})
	if err := json.Unmarshal(data, &tmpJSON); err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal jsonpb data")
	}

	// Pull out payload map data
	payloadJSON, ok := tmpJSON[field.GetJSONName()]
	if !ok {
		return nil, fmt.Errorf("unable to find field '%s' in jsonpb data", field.GetJSONName())
	}

	// Marshal payload map back into JSON so we can unmarshal into dynamic.Message
	payloadJSONBytes, err := json.Marshal(payloadJSON)
	if err != nil {
		return nil, errors.Wrap(err, "unable to marshal payload json")
	}

	// Unmarshal payload JSON into dynamic.Message
	if err := jsonpb.Unmarshal(bytes.NewBuffer(payloadJSONBytes), payload); err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal envelope data into dynamic message")
	}

	// Now let's encode payload into a protobuf message
	pbPayload, err := proto.Marshal(payload)
	if err != nil {
		return nil, errors.Wrap(err, "unable to marshal payload protobuf message")
	}

	// Delete payload field data from envelope map so that we can unmarshal the envelope into dynamic.Message
	delete(tmpJSON, field.GetJSONName())

	// Marshal envelope, minus the payload field data, back into JSON
	envelopeJSONBytes, err := json.Marshal(tmpJSON)
	if err != nil {
		return nil, errors.Wrap(err, "unable to marshal envelope json")
	}

	// Payload data is no longer in the envelope JOSN. Unmarshal of the envelope will now succeed
	if err := jsonpb.Unmarshal(bytes.NewBuffer(envelopeJSONBytes), envelope); err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal envelope data into dynamic message")
	}

	// Set the
	envelope.SetFieldByNumber(int(payloadFieldID), pbPayload)

	// Now let's encode that into a proper protobuf message
	pbBytes, err := proto.Marshal(envelope)
	if err != nil {
		return nil, errors.Wrap(err, "unable to marshal dynamic protobuf message to bytes")
	}

	return pbBytes, nil

}
