package pb

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"
)

// DecodeProtobufToJSON is a wrapper for decoding/unmarshalling []byte of
// protobuf into a dynamic.Message and then marshalling that into JSON.
func DecodeProtobufToJSON(readOpts *opts.ReadOptions, fds *dpb.FileDescriptorSet, message []byte) ([]byte, error) {
	// TODO: memoize these so we don't have to do this for every message
	descriptors, err := GetAllMessageDescriptorsInFDS(fds)
	if err != nil {
		return nil, err
	}

	mf := dynamic.NewMessageFactoryWithDefaults()
	resolver := dynamic.AnyResolver(mf, descriptors...)
	marshaler := &jsonpb.Marshaler{AnyResolver: resolver}

	protoOpts := readOpts.DecodeOptions.GetProtobufSettings()
	if protoOpts == nil {
		return nil, errors.New("protobuf settings cannot be nil")
	}

	envelopeMD, err := FindMessageDescriptorInFDS(fds, protoOpts.ProtobufRootMessage)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to find envelope message descriptor '%s'", protoOpts.ProtobufRootMessage)
	}

	// SQS doesn't like binary
	if readOpts.AwsSqs.Args.QueueName != "" {
		// Our implementation of 'protobuf-over-sqs' encodes protobuf in b64
		plain, err := base64.StdEncoding.DecodeString(string(message))
		if err != nil {
			return nil, fmt.Errorf("unable to decode base64 to protobuf")
		}
		message = plain
	}

	var payload *dynamic.Message
	if protoOpts.ProtobufEnvelopeType == encoding.EnvelopeType_ENVELOPE_TYPE_SHALLOW {
		payloadMD, err := FindMessageDescriptorInFDS(fds, protoOpts.ShallowEnvelopeMessage)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to find payload message descriptor '%s'", protoOpts.ShallowEnvelopeMessage)
		}

		payload = dynamic.NewMessage(payloadMD)
		if payload == nil {
			return nil, errors.New("BUG: cannot create dynamic message for shallow envelope payload")
		}
	}

	envelope := mf.NewDynamicMessage(envelopeMD)
	if envelope == nil {
		return nil, errors.New("BUG: cannot create dynamic message for envelope")
	}

	if err := proto.Unmarshal(message, envelope); err != nil {
		return nil, fmt.Errorf("unable to unmarshal protobuf to dynamic message: %s", err)
	}

	// Not shallow envelope, short circuit and return JSON of the message
	if payload == nil {
		jsonData, err := envelope.MarshalJSONPB(marshaler)
		if err != nil {
			return nil, errors.Wrap(err, "unable to marshal dynamic message into JSON")
		}

		return jsonData, nil
	}

	// Shallow envelope from here on out
	untypedPayload := envelope.GetFieldByNumber(int(protoOpts.ShallowEnvelopeFieldNumber))

	payloadData, ok := untypedPayload.([]byte)
	if !ok {
		return nil, errors.New("BUG: unable to type assert payload field to []byte")
	}

	// Get field contents
	if err := proto.Unmarshal(payloadData, payload); err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal shallow envelope payload into dynamic message")
	}

	payloadFD := envelope.FindFieldDescriptor(protoOpts.ShallowEnvelopeFieldNumber)
	if payloadFD == nil {
		return nil, fmt.Errorf("unable to find field descriptor for fieldID '%d'", protoOpts.ShallowEnvelopeFieldNumber)
	}
	mapName := payloadFD.GetJSONName()

	out, err := mergePayloadIntoEnvelope(envelope, payload, marshaler, mapName)
	if err != nil {
		return nil, err
	}

	return out, nil
}

func mergePayloadIntoEnvelope(envelope, payload *dynamic.Message, marshaler *jsonpb.Marshaler, mapName string) ([]byte, error) {
	envelopeJSONData, err := envelope.MarshalJSON()
	if err != nil {
		err = errors.Wrap(err, "unable to marshal dynamic message into JSON")
		return nil, err
	}

	wholeEvent := make(map[string]interface{})
	if err := json.Unmarshal(envelopeJSONData, &wholeEvent); err != nil {
		err = errors.Wrap(err, "unable to unmarshal envelope into map")
		return nil, err
	}

	payloadJSONData, err := payload.MarshalJSONPB(marshaler)
	if err != nil {
		err = errors.Wrap(err, "unable to marshal payload message into JSON")
		return nil, err
	}

	payloadEvent := make(map[string]interface{})
	if err := json.Unmarshal(payloadJSONData, &payloadEvent); err != nil {
		err = errors.Wrap(err, "unable to unmarshal envelope into map")
		return nil, err
	}

	wholeEvent[mapName] = payloadEvent

	// Marshal back into []byte
	out, err := json.Marshal(wholeEvent)
	if err != nil {
		err = errors.Wrap(err, "unable to marshal resulting event into JSON")
		return nil, err
	}

	return out, nil
}

// GetAllMessageDescriptorsInFDS returns all file descriptors from a set.
// The slice of descriptors is used for dynamic.AnyResolver to handle google.protobuf.Any typed fields
func GetAllMessageDescriptorsInFDS(fds *dpb.FileDescriptorSet) ([]*desc.FileDescriptor, error) {
	allDescriptors := make([]*desc.FileDescriptor, 0)

	descriptors, err := desc.CreateFileDescriptorsFromSet(fds)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create file descriptor set")
	}

	for _, d := range descriptors {
		allDescriptors = append(allDescriptors, d)
	}

	return allDescriptors, nil
}

func FindMessageDescriptorInFDS(fds *dpb.FileDescriptorSet, messageName string) (*desc.MessageDescriptor, error) {
	descriptors, err := desc.CreateFileDescriptorsFromSet(fds)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create file descriptor set")
	}

	for _, d := range descriptors {
		types := d.GetMessageTypes()
		for _, md := range types {
			if md.GetFullyQualifiedName() == messageName {
				return md, nil
			}
		}
	}

	return nil, errors.New("message descriptor not found in file descriptor(s)")
}

func readFileDescriptorsV1(files map[string][]string) ([]*desc.FileDescriptor, error) {
	contents := make(map[string]string, 0)
	keys := make([]string, 0)

	for dir, files := range files {
		// cleanup dir
		dir = filepath.Clean(dir)

		for _, f := range files {
			data, err := ioutil.ReadFile(f)
			if err != nil {
				return nil, fmt.Errorf("unable to read file '%s': %s", f, err)
			}

			if !strings.HasSuffix(dir, "/") {
				dir = dir + "/"
			}

			// Strip base path
			relative := strings.Split(f, dir)

			if len(relative) != 2 {
				return nil, fmt.Errorf("unexpected length of split path (%d)", len(relative))
			}

			contents[relative[1]] = string(data)
			keys = append(keys, relative[1])
		}
	}

	var p protoparse.Parser

	p.Accessor = protoparse.FileContentsFromMap(contents)

	fds, err := p.ParseFiles(keys...)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse files")
	}

	return fds, nil
}

func getProtoFiles(dirs []string) (map[string][]string, error) {
	protos := make(map[string][]string, 0)

	for _, dir := range dirs {
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return fmt.Errorf("unable to walk path '%s': %s", dir, err)
			}

			if info.IsDir() {
				// Nothing to do if this is a dir
				return nil
			}

			if strings.HasSuffix(info.Name(), ".proto") {
				if _, ok := protos[dir]; !ok {
					protos[dir] = make([]string, 0)
				}

				protos[dir] = append(protos[dir], path)
			}

			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("error walking the path '%s': %v", dir, err)
		}
	}

	return protos, nil
}

// ProcessDescriptors will return a protobuf file descriptor set that is generated
// from either a bunch of .proto files in a directory, or directly from a .fds/.protoset file
func ProcessDescriptors(pbDirs []string, fdsFile string) (*dpb.FileDescriptorSet, error) {
	if fdsFile != "" {
		return processDescriptorFile(fdsFile)
	}

	return processDescriptorDir(pbDirs)
}

// processDescriptorDir returns a protobuf file descriptor set from .proto files in a directory
func processDescriptorDir(dirs []string) (*dpb.FileDescriptorSet, error) {
	files, err := getProtoFiles(dirs)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get proto files")
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no .proto found in dir(s) '%v'", dirs)
	}

	fds, err := readFileDescriptorsV1(files)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read file descriptors")
	}

	descriptorSet := desc.ToFileDescriptorSet(fds...)

	return descriptorSet, nil
}

// processDescriptorDir returns a protobuf file descriptor set from a .protoset/.fds file
func processDescriptorFile(fdsPath string) (*dpb.FileDescriptorSet, error) {
	fdsBytes, err := ioutil.ReadFile(fdsPath)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read descriptor set file")
	}

	// Unmarshal the descriptor file
	fds := &dpb.FileDescriptorSet{}
	if err := proto.Unmarshal(fdsBytes, fds); err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal descriptor file into file descriptor set")

	}

	return fds, nil
}
