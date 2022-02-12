package plumber

import (
	"fmt"
	"net"
	"time"

	"github.com/batchcorp/plumber/bus"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/batchcorp/plumber-schemas/build/go/protos"

	"github.com/batchcorp/plumber/api"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/server"
)

// RunServer is a wrapper for starting embedded etcd and starting the gRPC server.
func (p *Plumber) RunServer() error {
	mode := "standalone"

	if p.Config.CLIOptions.Server.EnableCluster {
		mode = "cluster"
	}

	p.log.Infof("starting plumber server in '%s' mode...", mode)

	if err := p.relaunchRelays(); err != nil {
		return errors.Wrap(err, "failed to relaunch relays")
	}

	// TODO: Re-launch active tunnels
	if err := p.relaunchDynamic(); err != nil {
		return errors.Wrap(err, "failed to relaunch tunnels")
	}

	// Launch HTTP server
	go func() {
		if _, err := api.Run(p.CLIOptions.Server.HttpListenAddress, options.VERSION); err != nil {
			logrus.Fatalf("unable to start API server: %s", err)
		}
	}()

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
	} else {
		b = &bus.NoOpBus{}
	}

	p.Bus = b

	// Launch gRPC server (blocks)
	if err := p.runGRPCServer(); err != nil {
		p.log.Fatalf("unable to run gRPC server: %s", err)
	}

	return nil
}

func (p *Plumber) relaunchRelays() error {
	for relayID, relay := range p.PersistentConfig.Relays {
		// We want to run both active and inactive relays through CreateRelay
		// as we need to create backends, create shutdown context, channels, etc.
		// CreateRelay will only "start" active relays.
		r, err := p.Actions.CreateRelay(p.ServiceShutdownCtx, relay.Options)
		if err != nil {
			return errors.Wrapf(err, "unable to create relay '%s'", relayID)
		}

		if relay.Active {
			p.log.Infof("Relay '%s' re-started", relayID)
		} else {
			p.log.Debugf("Relay '%s' is inactive - not relaunching", relayID)
		}

		p.PersistentConfig.SetRelay(relayID, r)
		p.PersistentConfig.Save()
	}

	return nil
}

// TODO: Implement
func (p *Plumber) relaunchDynamic() error {
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

	plumberServer := &server.Server{
		Actions:          p.Actions,
		PersistentConfig: p.PersistentConfig,
		AuthToken:        p.CLIOptions.Server.AuthToken,
		Bus:              p.Bus,
		Log:              logrus.WithField("pkg", "plumber/cli_server.go"),
		CLIOptions:       p.CLIOptions,
	}

	protos.RegisterPlumberServerServer(grpcServer, plumberServer)

	go p.watchServiceShutdown(grpcServer)

	p.log.Debugf("starting gRPC server on %s", p.CLIOptions.Server.GrpcListenAddress)

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
