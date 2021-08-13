package server

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
)

// importGithub imports a github repo as a schema
// TODO: types other than protobuf
func (p *PlumberServer) importGithub(ctx context.Context, req *protos.ImportGithubRequest) (*protos.Schema, error) {
	var schema *protos.Schema
	var err error

	zipfile, err := p.GithubService.GetRepoArchive(ctx, p.PersistentConfig.GitHubToken, req.GithubUrl)
	if err != nil {
		return nil, err
	}

	switch req.Type {
	case encoding.Type_PROTOBUF:
		schema, err = importGithubProtobuf(zipfile, req)
	case encoding.Type_AVRO:
	// TODO
	default:
		err = errors.New("only protobuf and avro schemas can be imported from github")
	}

	if err != nil {
		return nil, err
	}

	return schema, nil
}

// importGithubProtobuf is used to import a protobuf schema from a GitHub repository
func importGithubProtobuf(zipfile []byte, req *protos.ImportGithubRequest) (*protos.Schema, error) {
	fds, files, err := GetFDFromArchive(zipfile, req.RootDir)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse protobuf zip")
	}

	blob, err := CreateBlob(fds, req.RootType)

	return &protos.Schema{
		Id:                uuid.NewV4().String(),
		Name:              req.Name,
		Type:              encoding.Type_PROTOBUF,
		Files:             files,
		RootType:          req.RootType,
		MessageDescriptor: blob,
	}, nil
}

// importLocal is used to import a schema from the UI
func (p *PlumberServer) importLocal(req *protos.ImportLocalRequest) (*protos.Schema, error) {
	var schema *protos.Schema
	var err error

	switch req.Type {
	case encoding.Type_PROTOBUF:
		schema, err = importLocalProtobuf(req)
	case encoding.Type_AVRO:
		// TODO
	case encoding.Type_JSON:
		// TODO
	default:
		err = fmt.Errorf("unknown encoding type: %d", req.Type)
	}

	if err != nil {
		return nil, err
	}

	return schema, nil
}

// importLocalProtobuf is used to import a protobuf schema from the UI
func importLocalProtobuf(req *protos.ImportLocalRequest) (*protos.Schema, error) {
	fds, files, err := GetFDFromArchive(req.ZipArchive, "")
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse protobuf zip")
	}

	blob, err := CreateBlob(fds, req.RootType)
	if err != nil {
		return nil, err
	}

	return &protos.Schema{
		Id:                uuid.NewV4().String(),
		Name:              req.Name,
		Type:              encoding.Type_PROTOBUF,
		Files:             files,
		RootType:          req.RootType,
		MessageDescriptor: blob,
	}, nil
}
