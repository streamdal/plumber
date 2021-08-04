package plumber

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/batchcorp/plumber/embed/etcd"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber/server"
)

// RunServer is a wrapper for validating server options, starting embedded etcd
// and starting the gRPC server.
func (p *Plumber) RunServer() error {
	if err := p.startEtcd(); err != nil {
		return errors.Wrap(err, "unable to start embedded etcd")
	}

	// Blocks until exit
	if err := p.runServer(); err != nil {
		return errors.Wrap(err, "unable to run server")
	}

	return nil
}

func (p *Plumber) startEtcd() error {
	e, err := etcd.New(p.Config.Options.Server)
	if err != nil {
		return errors.Wrap(err, "unable to instantiate etcd")
	}

	// TODO: Different context? (etcd)
	if err := e.Start(context.Background()); err != nil {
		return errors.Wrap(err, "unable to start embedded etcd")
	}

	p.Etcd = e

	return nil
}

func (p *Plumber) runServer() error {
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
		RelaysMutex:      &sync.RWMutex{},
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
