package pb

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"
)

func FindMessageDescriptor(protobufDir, protobufRootMessage string) (*desc.MessageDescriptor, error) {
	files, err := getProtoFiles(protobufDir)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get proto files")
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no protofiles found in dir '%s'", protobufDir)
	}

	fds, err := readFileDescriptors(files)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read file descriptors")
	}

	md, err := findMessageDescriptor(fds, protobufRootMessage)
	if err != nil {
		return nil, fmt.Errorf("unable to find message descriptor for message '%s': %s",
			protobufRootMessage, err)
	}

	return md, nil
}

func DecodeProtobufToJSON(m *dynamic.Message, data []byte) ([]byte, error) {
	if err := proto.Unmarshal(data, m); err != nil {
		return nil, fmt.Errorf("unable to unmarshal protobuf to dynamic message: %s", err)
	}

	jsonData, err := m.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("unable to marshal decoded message to JSON: %s", err)
	}

	return jsonData, nil
}

func findMessageDescriptor(fds []*desc.FileDescriptor, rootMessage string) (*desc.MessageDescriptor, error) {
	for _, fd := range fds {
		fullRootMessage := fd.GetPackage() + "." + rootMessage

		md := fd.FindMessage(fullRootMessage)
		if md != nil {
			return md, nil
		}
	}

	return nil, errors.New("message descriptor not found in file descriptor(s)")
}

func readFileDescriptors(files []string) ([]*desc.FileDescriptor, error) {
	contents := make(map[string]string, 0)
	keys := make([]string, 0)

	for _, f := range files {
		data, err := ioutil.ReadFile(f)
		if err != nil {
			return nil, fmt.Errorf("unable to read file '%s': %s", f, err)
		}

		_, fpath := filepath.Split("/some/path/to/remove/file.name")

		contents[fpath] = string(data)
		keys = append(keys, fpath)
	}

	var p protoparse.Parser

	p.Accessor = protoparse.FileContentsFromMap(contents)

	fds, err := p.ParseFiles(keys...)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse files")
	}

	return fds, nil
}

func getProtoFiles(dir string) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, errors.Wrap(err, "unable to ReadDir")
	}

	var protofiles []string

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		if strings.HasSuffix(file.Name(), ".proto") {
			protofiles = append(protofiles, file.Name())
		}
	}

	return protofiles, nil
}
