package plumber

import (
	"fmt"
	"net"
	"sync"

	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber/server"
)

func (p *Plumber) Serve() error {
	lis, err := net.Listen("tcp", p.Options.Server.ListenAddress)
	if err != nil {
		return fmt.Errorf("unable to listen on '%s': %s", p.Options.Server.ListenAddress, err)
	}

	var opts []grpc.ServerOption

	grpcServer := grpc.NewServer(opts...)

	// Each plumber instance needs an ID. Set one and save
	if p.PersistentConfig.PlumberID == "" {
		p.PersistentConfig.PlumberID = uuid.NewV4().String()
		p.PersistentConfig.Save()
	}

	plumberServer := &server.PlumberServer{
		PersistentConfig: p.PersistentConfig,
		AuthToken:        p.Options.Server.AuthToken,
		ConnectionsMutex: &sync.RWMutex{},
		Reads:            make(map[string]*server.Read),
		ReadsMutex:       &sync.RWMutex{},
		Log:              logrus.WithField("pkg", "plumber/server.go"),
	}

	protos.RegisterPlumberServerServer(grpcServer, plumberServer)

	p.log.Infof("gRPC server listening on: %s", p.Options.Server.ListenAddress)
	p.log.Infof("Plumber Instance ID: %s", p.PersistentConfig.PlumberID)

	if err := grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("unable to start gRPC server: %s", err)
	}

	return nil
}
