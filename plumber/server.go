package plumber

import (
	"fmt"
	"net"
	"sync"

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

	if err := grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("unable to start gRPC server: %s", err)
	}

	return nil
}
