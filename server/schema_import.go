package server

import (
	"context"
	"fmt"

	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/validate"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
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
		err = errors.New("not implemented")
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

// importLocal is used to import a schema from the UI
func (s *Server) importLocal(req *protos.ImportLocalRequest) (*protos.Schema, error) {
	var schema *protos.Schema
	var err error

	switch req.Type {
	case protos.SchemaType_SCHEMA_TYPE_PROTOBUF:
		schema, err = importLocalProtobuf(req)
	case protos.SchemaType_SCHEMA_TYPE_AVRO:
		// TODO
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
