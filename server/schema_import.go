package server

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"

	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/serializers"
	"github.com/batchcorp/plumber/validate"
)

// importGithub imports a github repo as a schema
// TODO: types other than protobuf
func (s *Server) importGithub(ctx context.Context, req *protos.ImportGithubRequest) (*protos.Schema, error) {

	var schema *protos.Schema
	var err error

	zipfile, err := s.GithubService.GetRepoArchive(ctx, s.PersistentConfig.GitHubToken, req.GithubUrl)
	if err != nil {
		return nil, err
	}

	switch req.Type {
	case protos.SchemaType_SCHEMA_TYPE_PROTOBUF:
		schema, err = importGithubProtobuf(zipfile, req)
	case protos.SchemaType_SCHEMA_TYPE_AVRO:
		schema, err = importGithubAvro(zipfile, req)
	default:
		err = validate.ErrInvalidGithubSchemaType
	}

	if err != nil {
		return nil, err
	}

	return schema, nil
}

// importGithubProtobuf is used to import a protobuf schema from a GitHub repository
func importGithubProtobuf(zipfile []byte, req *protos.ImportGithubRequest) (*protos.Schema, error) {
	if len(zipfile) == 0 {
		return nil, errors.New("zipfile cannot be empty")
	}

	if req == nil {
		return nil, errors.New("request cannot be nil")
	}

	settings := req.GetProtobufSettings()

	if settings == nil {
		return nil, errors.New("protobuf settings cannot be nil")
	}

	fds, files, err := pb.GetFDFromArchive(zipfile, settings.XProtobufRootDir)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse protobuf zip")
	}

	mdBlob, err := pb.CreateBlob(fds, settings.ProtobufRootMessage)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create message descriptor blob")
	}

	settings.XMessageDescriptor = mdBlob

	return &protos.Schema{
		Id:    uuid.NewV4().String(),
		Name:  req.Name,
		Type:  protos.SchemaType_SCHEMA_TYPE_PROTOBUF,
		Files: files,
		Settings: &protos.Schema_ProtobufSettings{
			ProtobufSettings: settings,
		},
	}, nil
}

// importGithubAvro is used to import an avro schema from a GitHub repository
func importGithubAvro(zipfile []byte, req *protos.ImportGithubRequest) (*protos.Schema, error) {
	if len(zipfile) == 0 {
		return nil, errors.New("zipfile cannot be empty")
	}

	if req == nil {
		return nil, errors.New("request cannot be nil")
	}

	// Find .aVSC FILE
	files, err := serializers.GetAvroFileFromArchive(zipfile)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read repository")
	}

	var schemaData []byte

	// Get the first found .avsc file. This is a map, so range and break after first file
	// TODO: do we need to, and can we support multiple schema files per repo?
	for _, avscFile := range files {
		// Avro schema is just JSON, nothing to decode here.
		// The schema byte slice is then passed to serializers.AvroDecode() which does the actual decoding
		schemaData = []byte(avscFile)
		break
	}

	return &protos.Schema{
		Id:    uuid.NewV4().String(),
		Name:  req.Name,
		Type:  protos.SchemaType_SCHEMA_TYPE_AVRO,
		Files: files,
		Settings: &protos.Schema_AvroSettings{
			AvroSettings: &encoding.AvroSettings{
				Schema: schemaData,
			},
		},
	}, nil
}

// importLocal is used to import a schema from the UI
func (s *Server) importLocal(req *protos.ImportLocalRequest) (*protos.Schema, error) {
	var schema *protos.Schema
	var err error

	switch req.Type {
	case protos.SchemaType_SCHEMA_TYPE_PROTOBUF:
		schema, err = importLocalProtobuf(req)
	case protos.SchemaType_SCHEMA_TYPE_AVRO:
		schema, err = importLocalAvro(req)
	default:
		err = fmt.Errorf("unknown schema type: %d", req.Type)
	}

	if err != nil {
		return nil, err
	}

	return schema, nil
}

// importLocalProtobuf is used to import a protobuf schema from the UI
func importLocalProtobuf(req *protos.ImportLocalRequest) (*protos.Schema, error) {
	if req == nil {
		return nil, errors.New("request cannot be nil")
	}

	if len(req.ZipArchive) == 0 {
		return nil, errors.New("zip archive cannot be empty")
	}

	settings := req.GetProtobufSettings()

	if settings == nil {
		return nil, errors.New("protobuf settings cannot be nil")
	}

	fds, files, err := pb.GetFDFromArchive(req.ZipArchive, "")
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse protobuf zip")
	}

	mdBlob, err := pb.CreateBlob(fds, settings.ProtobufRootMessage)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create message descriptor blob")
	}

	settings.XMessageDescriptor = mdBlob

	return &protos.Schema{
		Id:    uuid.NewV4().String(),
		Name:  req.Name,
		Type:  protos.SchemaType_SCHEMA_TYPE_PROTOBUF,
		Files: files,
		Settings: &protos.Schema_ProtobufSettings{
			ProtobufSettings: settings,
		},
	}, nil
}

// importLocalAvro is used to import an avro schema from the UI
func importLocalAvro(req *protos.ImportLocalRequest) (*protos.Schema, error) {
	if req == nil {
		return nil, errors.New("request cannot be nil")
	}

	if len(req.ZipArchive) == 0 {
		return nil, errors.New("zip archive cannot be empty")
	}

	// Find .aVSC FILE
	files, err := serializers.GetAvroFileFromArchive(req.ZipArchive)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read repository")
	}

	var schemaData []byte

	// Get the first found .avsc file. This is a map, so range and break after first file
	// TODO: do we need to, and can we support multiple schema files per repo?
	for _, avscFile := range files {
		// Avro schema is just JSON, nothing to decode here.
		// The schema byte slice is then passed to serializers.AvroDecode() which does the actual decoding
		schemaData = []byte(avscFile)
		break
	}

	return &protos.Schema{
		Id:    uuid.NewV4().String(),
		Name:  req.Name,
		Type:  protos.SchemaType_SCHEMA_TYPE_AVRO,
		Files: files,
		Settings: &protos.Schema_AvroSettings{
			AvroSettings: &encoding.AvroSettings{
				Schema: schemaData,
			},
		},
	}, nil
}
