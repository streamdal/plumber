package pb

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
)

// GetMessageDescriptor is a protobuf-specific function that returns a message
// descriptor using either the provided stored schema ID or using the provided
// protobuf zip file and root type.
//
// Either cachedSchemaOptions or pbSettings can be nil (but not both).
//
// NOTE: The signature for this function is unfortunately a bit funky - this is
// because we would _prefer_ to get a schemaID & a persistent config, but that
// causes import cycle errors.
//
// So, the expectation for the usage of this func is to call it as follows:
//
// GetMessageDescriptor(persistentConfig.GetSchema(schemaID), req.Opts.EncodeSettings.ProtobufSettings)
func GetMessageDescriptor(latestVersion *protos.SchemaVersion, pbSettings *encoding.ProtobufSettings) (*desc.MessageDescriptor, error) {
	// Stored schema settings take precedence
	if latestVersion != nil {

		cachedPbSettings := latestVersion.GetProtobufSettings()

		if cachedPbSettings == nil {
			return nil, errors.New("unexpected: protobuf settings are nil in stored schema")
		}

		md, err := GetMDFromDescriptorBlob(cachedPbSettings.XMessageDescriptor, cachedPbSettings.ProtobufRootMessage)
		if err != nil {
			return nil, errors.Wrap(err, "unable to get md from descriptor blob")
		}
		if md == nil {
			return nil, errors.New("unable to decode message descriptor")
		}

		return md, nil
	}

	// No cached pb settings - try pbSettings
	if pbSettings == nil {
		return nil, errors.New("cannot get descriptors - both cached schema options and protobuf settings are nil")
	}

	fds, _, err := GetFDFromArchive(pbSettings.Archive, "")
	if err != nil {
		return nil, errors.Wrap(err, "cannot get fd from archive")
	}

	md, err := GetMDFromDescriptors(fds, pbSettings.ProtobufRootMessage)
	if err != nil {
		return nil, errors.Wrap(err, "cannot get md from descriptors")
	}

	if md == nil {
		return nil, errors.New("unable to decode message descriptor")
	}

	return md, nil
}

// GetMDFromDescriptors takes a stored schema's file descriptorset blob and returns the necessary
// message descriptor for the given rootType
func GetMDFromDescriptors(fds []*desc.FileDescriptor, rootType string) (*desc.MessageDescriptor, error) {
	rootFD := FindRootDescriptor(rootType, fds)
	if rootFD == nil {
		return nil, fmt.Errorf("message type '%s' not found in file descriptors", rootType)
	}

	md := rootFD.FindMessage(rootType)
	if md == nil {
		return nil, fmt.Errorf("unable to find '%s' message in file descriptors", rootType)
	}

	return md, nil
}

// GetMDFromDescriptorBlob takes a stored schemas's file descriptorset blob and returns the necessary
// message descriptor for the given rootType
func GetMDFromDescriptorBlob(blob []byte, rootType string) (*desc.MessageDescriptor, error) {
	fds := &descriptor.FileDescriptorSet{}
	if err := proto.Unmarshal(blob, fds); err != nil {
		return nil, errors.Wrap(err, "unable to decode file descriptor set")
	}

	fd, err := desc.CreateFileDescriptorFromSet(fds)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create fd from fds")
	}

	md := fd.FindMessage(rootType)
	if md == nil {
		return nil, fmt.Errorf("unable to find '%s' message in file descriptors", rootType)
	}

	return md, nil
}

// readFileDescriptorsV1 takes in a map of protobuf files and their contents
// and pulls file descriptors for each
// TODO: replace with pb.readFileDescriptorsV1
func readFileDescriptorsV2(files map[string]string) ([]*desc.FileDescriptor, error) {
	var keys []string
	for k := range files {
		keys = append(keys, k)
	}

	var p protoparse.Parser
	p.InferImportPaths = true

	// Custom accessor in order to handle import path differences.
	// Ex: import "events/collect.proto" vs import "collect.proto"
	p.Accessor = func(f string) (io.ReadCloser, error) {
		for k, v := range files {
			if k == f || strings.HasSuffix(k, f) {
				return io.NopCloser(strings.NewReader(v)), nil
			}
		}
		return nil, fmt.Errorf("unable to find import '%s'", f)
	}

	fds, err := p.ParseFiles(keys...)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to parse protobuf file descriptor")
	}

	return fds, nil
}

// GetFDFromArchive is the main entry point for processing of a zip archive of protobuf definitions
func GetFDFromArchive(archive []byte, rootDir string) ([]*desc.FileDescriptor, map[string]string, error) {
	// Get files from zip
	files, err := getProtoFilesFromZip(archive)
	if err != nil {
		return nil, nil, err
	}

	// Clean path if necessary
	if rootDir != "" {
		files = truncateProtoDirectories(files, rootDir)
	}

	// Generate file descriptors
	fds, err := readFileDescriptorsV2(files)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to get file descriptors from archive")
	}

	return fds, files, nil
}

func FindRootDescriptor(rootType string, fds []*desc.FileDescriptor) *desc.FileDescriptor {
	for _, fd := range fds {
		messageDescriptor := fd.FindMessage(rootType)
		if messageDescriptor != nil {
			return fd
		}

	}

	return nil
}

// CreateBlob marshals a root descriptor for storage
func CreateBlob(fds []*desc.FileDescriptor, rootType string) ([]byte, error) {
	rootFD := FindRootDescriptor(rootType, fds)
	if rootFD == nil {
		return nil, fmt.Errorf("message type '%s' not found in file descriptors", rootType)
	}

	descSet := desc.ToFileDescriptorSet(rootFD)

	protoBytes, err := proto.Marshal(descSet)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal proto root descriptor")
	}

	return protoBytes, nil
}

// truncateProtoDirectories attempts to locate a .proto file in the shortest path of a directory tree so that
// import paths work correctly
func truncateProtoDirectories(files map[string]string, rootDir string) map[string]string {
	if strings.HasSuffix(rootDir, "/") {
		rootDir = strings.TrimSuffix(rootDir, "/")
	}

	cleaned := make(map[string]string)

	var zipBaseDir string

	for filePath, contents := range files {
		// Only need to do this once
		if zipBaseDir == "" {
			// Strip out zip base directory, which will look like "batchcorp-schemas-9789dfg70s980fdsfs"
			parts := strings.Split(filePath, "/")
			zipBaseDir = parts[0] + "/"
		}

		if !strings.Contains(filePath, zipBaseDir+rootDir+"/") {
			continue
		}

		newPath := strings.Replace(filePath, zipBaseDir+rootDir+"/", "", 1)
		cleaned[newPath] = contents
	}

	return cleaned
}

// getProtoFilesFromZip reads all proto files from a zip archive
// TODO: make output compatible with pb.readFileDescriptorsV1's map[string][]string
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

// getLatestSchemaVersion retrieves the most recent schema version
func getLatestSchemaVersion(schema *protos.Schema) *protos.SchemaVersion {
	if schema.Versions == nil || len(schema.Versions) == 0 {
		return nil
	}

	return schema.Versions[len(schema.Versions)-1]
}
