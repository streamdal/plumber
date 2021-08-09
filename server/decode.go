package server

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io/ioutil"
	"math"
	"path/filepath"
	"strings"

	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"

	"github.com/jhump/protoreflect/dynamic"

	"github.com/jhump/protoreflect/desc/protoparse"

	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/pb"
)

// generateMD returns the root type message descriptor from an encoding options message
func generateMD(opts *encoding.Options) (*desc.MessageDescriptor, error) {
	pbOptions := opts.GetProtobuf()
	if pbOptions == nil {
		// Not protobuf encoding/decoding request, nothing to do
		return nil, nil
	}

	// Get Message descriptor from zip file
	md, err := ProcessProtobufArchive(pbOptions.RootType, pbOptions.ZipArchive)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse protobuf zip")
	}

	return md, nil
}

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
	p.LookupImport = func(f string) (*desc.FileDescriptor, error) {
		// TODO: this
		fmt.Println("looping up import: " + f)

		for k, v := range files {
			// Most likely scenario
			if k == f {
				// do proto
			}

			if strings.HasSuffix(k, f) {
				// do proto
			}

		}

		return nil, nil
	}
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

	files = truncateProtoDirectories(files)

	for k := range files {
		fmt.Println(k)
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

// truncateProtoDirectories attempts to locate a .poroto file in the shortest path of a directory tree so that
// import paths work correctly
func truncateProtoDirectories(files map[string]string) map[string]string {
	var rootProtoLocation string
	depth := math.MaxInt32

	for filePath := range files {
		dirsDeep := strings.Count(filePath, "/")
		if dirsDeep < depth {
			depth = dirsDeep
			rootProtoLocation = filepath.Dir(filePath)
		}
	}

	cleaned := make(map[string]string)

	for filePath, contents := range files {
		newPath := strings.Replace(filePath, rootProtoLocation+"/", "", 1)
		cleaned[newPath] = contents
	}

	return cleaned
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
