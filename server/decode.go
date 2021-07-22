package server

import (
	"archive/zip"
	"bytes"
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/jhump/protoreflect/dynamic"

	"github.com/jhump/protoreflect/desc/protoparse"

	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/pb"
)

// DecodeProtobuf decodes a protobuf message to json
func DecodeProtobuf(md *desc.MessageDescriptor, message []byte) ([]byte, error) {
	// Decode message
	decoded, err := pb.DecodeProtobufToJSON(dynamic.NewMessage(md), message)
	if err != nil {
		return nil, errors.Wrap(err, "unable to decode protobuf message")
	}

	return decoded, nil
}

// readFileDescriptors takes in a map of protobuf files and their contents, and pulls file descriptors for each
// TODO: replace with pb.readFileDescriptors
func readFileDescriptors(files map[string]string) ([]*desc.FileDescriptor, error) {
	var keys []string
	for k := range files {
		keys = append(keys, k)
	}

	var p protoparse.Parser
	p.Accessor = protoparse.FileContentsFromMap(files)

	fds, err := p.ParseFiles(keys...)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to parse protobuf file descriptor")
	}

	return fds, nil
}

func ProcessProtobufArchive(rootType string, archive []byte) (*desc.MessageDescriptor, error) {
	files, err := getProtoFilesFromZip(archive)
	if err != nil {
		return nil, err
	}

	fds, err := readFileDescriptors(files)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get file descriptors from archive")
	}

	rootMD, err := pb.FindMessageDescriptorInFDS(fds, rootType)
	if err != nil {
		return nil, errors.Wrap(err, "unable to find root message descriptor")
	}

	return rootMD, nil
}

// getProtoFilesFromZip reads all proto files from a zip archive
// TODO: make output compatible with pb.readFileDescriptors's map[string][]string
func getProtoFilesFromZip(archive []byte) (map[string]string, error) {

	files := make(map[string]string)

	zipReader, err := zip.NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse archive")
	}

	// Read all the files from zip archive
	for _, zipFile := range zipReader.File {
		if zipFile.FileInfo().IsDir() {
			continue
		}

		_, file := filepath.Split(zipFile.Name)

		unzippedFileBytes, err := readZipFile(zipFile)
		if err != nil {
			return nil, errors.Wrap(err, "failed to process archive")
		}

		if strings.HasSuffix(file, ".proto") {
			files[zipFile.Name] = string(unzippedFileBytes)
		}
	}

	return files, nil
}

// readZipFile reads the contents of a zip archive into a []byte
func readZipFile(zf *zip.File) ([]byte, error) {
	f, err := zf.Open()
	if err != nil {
		return nil, errors.Wrap(err, "failed to open zip file")
	}
	defer f.Close()
	return ioutil.ReadAll(f)
}
