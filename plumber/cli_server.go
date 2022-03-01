package plumber

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/hashicorp/yamux"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber/api"
	"github.com/batchcorp/plumber/bus"
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

	// Launch HTTP server
	srv, err := api.Start(p.CLIOptions.Server.HttpListenAddress, options.VERSION)
	if err != nil {
		logrus.Fatalf("unable to start API server: %s", err)
	}

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

	lis, err := net.Listen("tcp", p.CLIOptions.Server.GrpcListenAddress)
	if err != nil {
		return fmt.Errorf("unable to listen on '%s': %s", p.CLIOptions.Server.GrpcListenAddress, err)
	}

	// Launch gRPC server
	if err := p.startGRPCServer(lis, p.CLIOptions.Server.GrpcListenAddress); err != nil {
		p.log.Fatalf("unable to run gRPC server: %s", err)
	}

	p.log.Info("plumber server started")

	// Keep after startGRPCServer(). If dProxy is unreachable on start, these will block for a while
	// and prevent gRPC server from starting.
	if err := p.relaunchRelays(); err != nil {
		p.log.Error(errors.Wrap(err, "failed to relaunch relays"))
	}

	if err := p.relaunchTunnels(); err != nil {
		p.log.Error(errors.Wrap(err, "failed to relaunch tunnels"))
	}

	go p.runRemoteControl()

	// Wait for shutdown
	select {
	case <-p.ServiceShutdownCtx.Done():
		p.log.Debug("service shutdown initiated")

		timeoutCtx, _ := context.WithTimeout(context.Background(), time.Second*5)

		if err := srv.Shutdown(timeoutCtx); err != nil {
			p.log.Errorf("API server shutdown failed: %s", err)
		}
	}

	return nil
}

func (p *Plumber) runRemoteControl() {
	// TODO: add CLI flag to enable remote control
	if !p.Config.CLIOptions.Server.RemoteControlEnabled {
		return
	}

	p.log.Debug("starting remote control server...")

	foremanAddr := p.Config.CLIOptions.Server.RemoteControlAddress

	conn, err := net.DialTimeout("tcp", foremanAddr, time.Second*5)
	if err != nil {
		p.log.Errorf("error dialing: %s", err)
		return
	}

	srvConn, err := yamux.Client(conn, yamux.DefaultConfig())
	if err != nil {
		p.log.Errorf("couldn't create yamux server: %s", err)
		return
	}

	// TODO: flag for foreman service address
	gconn, err := grpc.Dial(foremanAddr, grpc.WithInsecure(), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return srvConn.Open()
	}))
	if err != nil {
		p.log.Errorf("failed to create grpc client: %s", err)
		conn.Close()
		return
	}

	client := protos.NewForemanClientClient(gconn)
	authResp, err := client.Register(context.Background(), &protos.RegisterRequest{
		// TODO: how do we set this? Flag?
		ApiToken:     "batchsh_319041f4b82fb7c0fe04b2598449a3e07effe66c2af5d54d13d6f6b1d2bb", // TODO: store in config
		ClusterId:    p.PersistentConfig.ClusterID,
		PlumberToken: p.Config.CLIOptions.Server.AuthToken,
	})
	if err != nil {
		p.log.Errorf("failed to register with remote control server. remove control not available: %s", err)
		conn.Close()
		gconn.Close()
		return
	}

	p.log.Debugf("register response: %#v\n", authResp)

	if err := p.startGRPCServer(srvConn, foremanAddr); err != nil {
		p.log.Fatalf("unable to run remote control gRPC server: %s", err)
	}

	return
}

func (p *Plumber) watchForForemanDisconnect(sess *yamux.Session) {
	ch := sess.CloseChan()

	<-ch

	p.log.Info("Lost connection to remote control server. Remote control not available")

	// TODO: handle reconnects somehow?
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

func (p *Plumber) relaunchTunnels() error {
	for tunnelID, tunnel := range p.PersistentConfig.Tunnels {
		// We want to "create" both active and inactive relays through CreateTunnel
		// as we need to create backends, create shutdown context, channels, etc.
		d, err := p.Actions.CreateTunnel(p.ServiceShutdownCtx, tunnel.Options)
		if err != nil {
			return errors.Wrapf(err, "unable to create tunnel '%s'", tunnelID)
		}

		if tunnel.Active {
			p.log.Infof("Tunnel '%s' re-started", tunnelID)
		} else {
			p.log.Debugf("Tunnel '%s' is inactive - not relaunching", tunnelID)
		}

		p.PersistentConfig.SetTunnel(tunnelID, d)
		p.PersistentConfig.Save()
	}

	return nil

}

func (p *Plumber) startGRPCServer(listener net.Listener, addr string) error {
	var opts []grpc.ServerOption

	grpcServer := grpc.NewServer(opts...)

	// Each plumber node needs a unique ID
	p.PersistentConfig.PlumberID = p.CLIOptions.Server.NodeId
	p.PersistentConfig.ClusterID = p.CLIOptions.Server.ClusterId

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

	errCh := make(chan error, 1)

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			errCh <- errors.Wrap(err, "unable to start gRPC server")
		}
	}()

	p.log.Debugf("started gRPC server on %s", addr)

	afterCh := time.After(5 * time.Second)

	select {
	case <-afterCh:
		return nil
	case err := <-errCh:
		return err
	}
}

func (p *Plumber) watchServiceShutdown(grpcServer *grpc.Server) {
	<-p.ServiceShutdownCtx.Done()

	p.log.Debug("received shutdown request in gRPC server via ServiceShutdownCtx")

	// Hack: Give anything in flight a moment to shutdown
	time.Sleep(5 * time.Second)

	grpcServer.Stop()
}
