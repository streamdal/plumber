package server

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/validate"
)

// importGithub imports a github repo or file as a schema
func (s *Server) importGithub(ctx context.Context, req *protos.ImportGithubRequest) (*protos.Schema, error) {

	var schema *protos.Schema
	var err error

	switch req.Type {
	case protos.SchemaType_SCHEMA_TYPE_PROTOBUF:
		schema, err = s.importGithubProtobuf(ctx, req)
	case protos.SchemaType_SCHEMA_TYPE_AVRO:
		schema, err = s.importGithubAvro(ctx, req)
	case protos.SchemaType_SCHEMA_TYPE_JSONSCHEMA:
		schema, err = s.importGithubJSONSchema(ctx, req)
	default:
		err = validate.ErrInvalidGithubSchemaType
	}

	if err != nil {
		return nil, err
	}

	return schema, nil
}

// importGithubProtobuf is used to import a protobuf schema from a GitHub repository
func (s *Server) importGithubProtobuf(ctx context.Context, req *protos.ImportGithubRequest) (*protos.Schema, error) {
	zipFile, err := s.GithubService.GetRepoArchive(ctx, s.PersistentConfig.GitHubToken, req.GithubUrl)
	if err != nil {
		return nil, err
	}

	if len(zipFile) == 0 {
		return nil, errors.New("zipFile cannot be empty")
	}

	if req == nil {
		return nil, errors.New("request cannot be nil")
	}

	settings := req.GetProtobufSettings()

	if settings == nil {
		return nil, errors.New("protobuf settings cannot be nil")
	}

	fds, files, err := pb.GetFDFromArchive(zipFile, settings.XProtobufRootDir)
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
func (s *Server) importGithubAvro(ctx context.Context, req *protos.ImportGithubRequest) (*protos.Schema, error) {
	if req == nil {
		return nil, errors.New("request cannot be nil")
	}

	schemaData, fileName, err := s.GithubService.GetRepoFile(ctx, s.PersistentConfig.GitHubToken, req.GithubUrl)
	if err != nil {
		return nil, err
	}

	return &protos.Schema{
		Id:   uuid.NewV4().String(),
		Name: req.Name,
		Type: protos.SchemaType_SCHEMA_TYPE_AVRO,
		Files: map[string]string{
			fileName: string(schemaData),
		},
		Settings: &protos.Schema_AvroSettings{
			AvroSettings: &encoding.AvroSettings{
				Schema: schemaData,
			},
		},
	}, nil
}
func (s *Server) importGithubJSONSchema(ctx context.Context, req *protos.ImportGithubRequest) (*protos.Schema, error) {
	if len(req.GithubUrl) == 0 {
		return nil, errors.New("Github URL cannot be empty")
	}

	schemaData, fileName, err := s.GithubService.GetRepoFile(ctx, s.PersistentConfig.GitHubToken, req.GithubUrl)
	if err != nil {
		return nil, err
	}

	return &protos.Schema{
		Id:   uuid.NewV4().String(),
		Name: req.Name,
		Type: protos.SchemaType_SCHEMA_TYPE_JSONSCHEMA,
		Files: map[string]string{
			fileName: string(schemaData),
		},
		Settings: &protos.Schema_JsonSchemaSettings{
			JsonSchemaSettings: &encoding.JSONSchemaSettings{
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
	case protos.SchemaType_SCHEMA_TYPE_JSONSCHEMA:
		schema, err = importLocalJSONSchema(req)
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

	if len(req.FileContents) == 0 {
		return nil, errors.New("zip archive cannot be empty")
	}

	settings := req.GetProtobufSettings()

	if settings == nil {
		return nil, errors.New("protobuf settings cannot be nil")
	}

	fds, files, err := pb.GetFDFromArchive(req.FileContents, "")
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

	if len(req.FileContents) == 0 {
		return nil, errors.New("zip archive cannot be empty")
	}

	return &protos.Schema{
		Id:   uuid.NewV4().String(),
		Name: req.Name,
		Type: protos.SchemaType_SCHEMA_TYPE_AVRO,
		Files: map[string]string{
			req.FileName: string(req.FileContents),
		},
		Settings: &protos.Schema_AvroSettings{
			AvroSettings: &encoding.AvroSettings{
				Schema: req.FileContents,
			},
		},
	}, nil
}

func importLocalJSONSchema(req *protos.ImportLocalRequest) (*protos.Schema, error) {
	if req == nil {
		return nil, errors.New("request cannot be nil")
	}

	if len(req.FileContents) == 0 {
		return nil, errors.New("zip archive cannot be empty")
	}

	return &protos.Schema{
		Id:   uuid.NewV4().String(),
		Name: req.Name,
		Type: protos.SchemaType_SCHEMA_TYPE_JSONSCHEMA,
		Files: map[string]string{
			req.FileName: string(req.FileContents),
		},
		Settings: &protos.Schema_JsonSchemaSettings{
			JsonSchemaSettings: &encoding.JSONSchemaSettings{
				Schema: req.FileContents,
			},
		},
	}, nil
}
