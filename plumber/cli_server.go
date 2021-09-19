package plumber

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber/embed/etcd"
	"github.com/batchcorp/plumber/github"
	"github.com/batchcorp/plumber/server"
)

// RunServer is a wrapper for starting embedded etcd and starting the gRPC server.
func (p *Plumber) RunServer() error {
	p.log.Info("starting embedded etcd")

	if err := p.startEtcd(); err != nil {
		return errors.Wrap(err, "unable to start embedded etcd")
	}

	p.log.Info("starting gRPC server")

	// Blocks
	if err := p.runServer(); err != nil {
		return errors.Wrap(err, "unable to run server")
	}

	return nil
}

func (p *Plumber) startEtcd() error {
	e, err := etcd.New(p.Config.CLIOptions.Server, p.PersistentConfig)
	if err != nil {
		return errors.Wrap(err, "unable to instantiate etcd")
	}

	if err := e.Start(p.Config.ServiceShutdownCtx); err != nil {
		return errors.Wrap(err, "unable to start embedded etcd")
	}

	p.Etcd = e

	if err := e.PopulateCache(); err != nil {
		p.log.Errorf("Unable to load data from etcd: %s", err)
	}

	return nil
}

func (p *Plumber) runServer() error {
	lis, err := net.Listen("tcp", p.CLIOptions.Server.GrpcListenAddress)
	if err != nil {
		return fmt.Errorf("unable to listen on '%s': %s", p.CLIOptions.Server.GrpcListenAddress, err)
	}

	var opts []grpc.ServerOption

	grpcServer := grpc.NewServer(opts...)

	// Each plumber instance needs an ID. Set one and save
	if p.PersistentConfig.PlumberID == "" {
		p.PersistentConfig.PlumberID = uuid.NewV4().String()
		if err := p.PersistentConfig.Save(); err != nil {
			p.log.Fatalf("unable to save persistent config: %s", err)
		}
	}

	gh, err := github.New()
	if err != nil {
		return errors.Wrap(err, "unable to create GitHub service instance")
	}

	plumberServer := &server.Server{
		PersistentConfig: p.PersistentConfig,
		AuthToken:        p.CLIOptions.Server.AuthToken,
		ConnectionsMutex: &sync.RWMutex{},
		Reads:            make(map[string]*server.Read),
		ReadsMutex:       &sync.RWMutex{},
		RelaysMutex:      &sync.RWMutex{}, // TODO: Are these used?
		GithubService:    gh,
		Log:              logrus.WithField("pkg", "plumber/cli_server.go"),
		Etcd:             p.Etcd,
	}

	protos.RegisterPlumberServerServer(grpcServer, plumberServer)

	go p.watchServiceShutdown(grpcServer)

	p.log.Infof("gRPC server listening on: %s", p.CLIOptions.Server.GrpcListenAddress)
	p.log.Infof("Plumber Instance ID: %s", p.PersistentConfig.PlumberID)

	if err := grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("unable to start gRPC server: %s", err)
	}

	return nil
}

func (p *Plumber) watchServiceShutdown(grpcServer *grpc.Server) {
	<-p.ServiceShutdownCtx.Done()

	p.log.Debug("received shutdown request in gRPC server via ServiceShutdownCtx")

	// Give etcd a few seconds to shutdown gracefully
	time.Sleep(10 * time.Second)

	grpcServer.Stop()
}
