package pb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"
)

// FindMessageDescriptor is a wrapper that will:
//
//   1. Recursively find all .proto files in a directory
//   2. Attempt to read and parse all files as proto FileDescriptors
//   3. Attempt to find the specified "protobufRootMessage" type in the parsed
//      FileDescriptors; if found, return the related MessageDescriptor
//
// With the found MessageDescriptor, we are able to generate new dynamic
// messages via dynamic.NewMessage(..).
func FindMessageDescriptor(protobufDirs []string, protobufRootMessage string) (*desc.MessageDescriptor, error) {
	files, err := getProtoFiles(protobufDirs)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get proto files")
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no .proto found in dir(s) '%v'", protobufDirs)
	}

	fds, err := readFileDescriptorsV1(files)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read file descriptors")
	}

	md, err := FindMessageDescriptorInFDS(fds, protobufRootMessage)
	if err != nil {
		return nil, fmt.Errorf("unable to find message descriptor for message '%s': %s",
			protobufRootMessage, err)
	}

	return md, nil
}

// DecodeProtobufToJSON is a wrapper for decoding/unmarshalling []byte of
// protobuf into a dynamic.Message and then marshalling that into JSON.
func DecodeProtobufToJSON(envelope, payload *dynamic.Message, data []byte, payloadFieldID int32) ([]byte, error) {
	if err := proto.Unmarshal(data, envelope); err != nil {
		return nil, fmt.Errorf("unable to unmarshal protobuf to dynamic message: %s", err)
	}

	// Deep envelope
	if payload == nil {
		jsonData, err := envelope.MarshalJSONIndent()
		if err != nil {
			return nil, fmt.Errorf("unable to marshal decoded message to JSON: %s", err)
		}

		return jsonData, nil
	}

	// Shallow envelope, we need to decode the fieldID with the payload message
	if err := proto.Unmarshal(data, envelope); err != nil {
		return nil, fmt.Errorf("unable to unmarshal protobuf to dynamic message: %s", err)
	}

	untypedPayload := envelope.GetFieldByNumber(int(payloadFieldID))

	payloadData, ok := untypedPayload.([]byte)
	if !ok {
		return nil, errors.New("BUG: unable to type assert payload field to []byte")
	}

	// Get field contents
	if err := proto.Unmarshal(payloadData, payload); err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal shallow envelope payload into dynamic message")
	}

	payloadFD := envelope.FindFieldDescriptor(payloadFieldID)
	if payloadFD == nil {
		return nil, fmt.Errorf("unable to find field descriptor for fieldID '%d'", payloadFieldID)
	}
	mapName := payloadFD.GetJSONName()

	out, err := mergePayloadIntoEnvelope(envelope, payload, mapName)
	if err != nil {
		return nil, err
	}

	return out, nil
}

func mergePayloadIntoEnvelope(envelope, payload *dynamic.Message, mapName string) ([]byte, error) {
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

	payloadJSONData, err := payload.MarshalJSON()
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

func FindMessageDescriptorInFDS(fds []*desc.FileDescriptor, rootMessage string) (*desc.MessageDescriptor, error) {
	for _, fd := range fds {
		md := fd.FindMessage(rootMessage)
		if md != nil {
			return md, nil
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
