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

	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
)

// getMessageDescriptor returns a message descriptor using either the provided stored schema ID, or
// the provided protobuf zip file and root type
func (p *PlumberServer) getMessageDescriptor(opts *encoding.Options) (*desc.MessageDescriptor, error) {
	// No decode options passed
	if opts == nil {
		return nil, nil
	}

	if opts.Type != encoding.Type_PROTOBUF {
		return nil, nil
	}

	// Using passed protobuf zip file and root type
	if opts.SchemaId == "" {
		pbOptions := opts.GetProtobuf()
		fds, _, err := GetFDFromArchive(pbOptions.ZipArchive, "")
		if err != nil {
			return nil, err
		}

		md, err := GetMDFromDescriptors(fds, pbOptions.RootType)
		if err != nil {
			return nil, err
		}
		if md == nil {
			return nil, errors.New("unable to decode message descriptor")
		}
	}

	// Using stored schema
	schema := p.PersistentConfig.GetSchema(opts.SchemaId)
	if schema == nil {
		return nil, fmt.Errorf("schema '%s' not found", opts.SchemaId)
	}

	md, err := GetMDFromDescriptorBlob(schema.MessageDescriptor, schema.RootType)
	if err != nil {
		return nil, err
	}
	if md == nil {
		return nil, errors.New("unable to decode message descriptor")
	}

	return md, nil
}

// GetMDFromDescriptors takes a stored sceham's file descriptorset blob and returns the necessary
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

// readFileDescriptors takes in a map of protobuf files and their contents, and pulls file descriptors for each
// TODO: replace with pb.readFileDescriptors
func readFileDescriptors(files map[string]string) ([]*desc.FileDescriptor, error) {
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
	fds, err := readFileDescriptors(files)
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

func ProcessProtobufArchive(rootType string, files map[string]string) (*desc.FileDescriptor, map[string]string, error) {
	fds, err := readFileDescriptors(files)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to get file descriptors from archive")
	}

	rootFD := FindRootDescriptor(rootType, fds)
	if rootFD == nil {
		return nil, nil, errors.New("root type is missing from archive")
	}

	return rootFD, files, nil
}

// truncateProtoDirectories attempts to locate a .proto file in the shortest path of a directory tree so that
// import paths work correctly
func truncateProtoDirectories(files map[string]string, rootDir string) map[string]string {
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
