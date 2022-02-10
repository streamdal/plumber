package plumber

import (
	"fmt"
	"net"
	"time"

	"github.com/batchcorp/plumber/bus"
	"github.com/nakabonne/tstorage"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/batchcorp/plumber/api"
	"github.com/batchcorp/plumber/options"

	"github.com/batchcorp/plumber-schemas/build/go/protos"

	"github.com/batchcorp/plumber/github"
	"github.com/batchcorp/plumber/server"
	"github.com/batchcorp/plumber/stats"
	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/vcservice"
)

// RunServer is a wrapper for starting embedded etcd and starting the gRPC server.
func (p *Plumber) RunServer() error {
	p.log.Info("starting HTTP server")

	// Launch HTTP server
	go func() {
		if err := api.Start(p.CLIOptions.Server.HttpListenAddress, options.VERSION); err != nil {
			logrus.Fatalf("unable to start API server: %s", err)
		}
	}()

	p.log.Info("starting bus")

	var b bus.IBus

	if p.Config.CLIOptions.Server.EnableCluster {
		var err error

		b, err = bus.New(&bus.Config{
			ServerOptions:    p.CLIOptions.Server,
			PersistentConfig: p.PersistentConfig,
			Actions:          p.Actions,
		})

		if err != nil {
			return errors.Wrap(err, "unable to setup bus bus")
		}

		if err := b.Start(p.ServiceShutdownCtx); err != nil {
			return errors.Wrap(err, "unable to start bus consumers")
		}
	}

	p.Bus = b

	p.log.Info("starting gRPC server")

	// Blocks
	if err := p.runGRPCServer(); err != nil {
		return errors.Wrap(err, "unable to run server")
	}

	return nil
}

func (p *Plumber) runGRPCServer() error {
	lis, err := net.Listen("tcp", p.CLIOptions.Server.GrpcListenAddress)
	if err != nil {
		return fmt.Errorf("unable to listen on '%s': %s", p.CLIOptions.Server.GrpcListenAddress, err)
	}

	var opts []grpc.ServerOption

	grpcServer := grpc.NewServer(opts...)

	// Each plumber node needs a unique ID
	p.PersistentConfig.PlumberID = p.CLIOptions.Server.NodeId

	// Only start if we have an authentication token for the service
	var vcService vcservice.IVCService
	if p.PersistentConfig.VCServiceToken != "" {
		//var err error
		//vcService, err = vcservice.New(&vcservice.Config{
		//	EtcdService:      p.Bus,
		//	PersistentConfig: p.PersistentConfig,
		//	ServerOptions:    p.CLIOptions.Server,
		//})
		//if err != nil {
		//	return errors.Wrap(err, "unable to create VCService service instance")
		//}
	}

	ghService, err := github.New()
	if err != nil {
		return errors.Wrap(err, "unable to start GitHub service")
	}

	storage, err := tstorage.NewStorage(
		tstorage.WithDataPath(p.CLIOptions.Server.StatsDatabasePath),
		tstorage.WithTimestampPrecision(tstorage.Seconds),
		tstorage.WithRetention(time.Hour*24*7),
	)
	if err != nil {
		return errors.Wrap(err, "unable to initialize time series storage")
	}

	statsService, err := stats.New(&stats.Config{
		FlushInterval:      util.DurationSec(p.CLIOptions.Server.StatsFlushIntervalSeconds),
		ServiceShutdownCtx: p.ServiceShutdownCtx,
		Storage:            storage,
	})
	if err != nil {
		return errors.Wrap(err, "unable to create stats service")
	}

	// Commented out because uierrors was designed for etcd that has additional
	// read controls (like sort & limit) that aren't available in NATS KV.
	//
	//errorsService, err := uierrors.New(&uierrors.Config{
	//	EtcdService: p.Bus,
	//})
	//if err != nil {
	//	return errors.Wrap(err, "unable to create uierrors service")
	//}

	plumberServer := &server.Server{
		Actions:          p.Actions,
		PersistentConfig: p.PersistentConfig,
		AuthToken:        p.CLIOptions.Server.AuthToken,
		VCService:        vcService,
		GithubService:    ghService,
		StatsService:     statsService,
		Log:              logrus.WithField("pkg", "plumber/cli_server.go"),
		Bus:              p.Bus,
		CLIOptions:       p.CLIOptions,
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
