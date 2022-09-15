package plumber

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/hashicorp/yamux"
	"github.com/pkg/errors"
	"github.com/posthog/posthog-go"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

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

	// Running in a goroutine to prevent blocking of server startup due to possible long connect timeouts
	go func() {
		if err := p.relaunchRelays(); err != nil {
			p.log.Error(errors.Wrap(err, "failed to relaunch relays"))
		}

		if err := p.relaunchTunnels(); err != nil {
			p.log.Error(errors.Wrap(err, "failed to relaunch tunnels"))
		}
	}()

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

func (p *Plumber) runRemoteControl() bool {
	if !p.CLIOptions.Server.RemoteControlEnabled {
		return true
	}

	if p.CLIOptions.Server.RemoteControlApiToken == "" {
		p.log.Fatal("Remote control requires --remote-control-api-token or PLUMBER_REMOTE_CONTROL_API_TOKEN to be specified")
	}

	p.log.Debug("starting remote control server...")

	foremanAddr := p.Config.CLIOptions.Server.RemoteControlAddress

	// Establish a regular TCP connection to the foreman service
	conn, err := net.DialTimeout("tcp", foremanAddr, time.Second*5)
	if err != nil {
		p.log.Errorf("failed to register with remote control server. remote control not available: %s", err)
		return false
	}

	// Establish two-way communication with the foreman service
	// We will talk gRPC to Foreman, and Foreman will talk gRPC to us
	foremanConn, err := yamux.Client(conn, yamux.DefaultConfig())
	if err != nil {
		p.log.Errorf("couldn't create yamux server: %s", err)
		return false
	}

	dialOpts := make([]grpc.DialOption, 0)

	if p.Config.CLIOptions.Server.RemoteControlDisableTls {
		dialOpts = append(dialOpts, grpc.WithInsecure())
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(
			&tls.Config{
				InsecureSkipVerify: true,
			},
		)))
	}

	dialOpts = append(dialOpts, grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return foremanConn.Open()
	}))

	// Establish Plumber -> Foreman gRPC connection over our yamux session
	foremanGRPCConn, err := grpc.Dial(foremanAddr, dialOpts...)
	if err != nil {
		p.log.Errorf("failed to create grpc client: %s", err)
		conn.Close()
		return false
	}

	// Register ourselves with the foreman service
	client := protos.NewForemanClientClient(foremanGRPCConn)
	authResp, err := client.Register(context.Background(), &protos.RegisterRequest{
		ApiToken:     p.Config.CLIOptions.Server.RemoteControlApiToken,
		ClusterId:    p.Config.CLIOptions.Server.ClusterId,
		PlumberToken: p.Config.CLIOptions.Server.AuthToken,
		NodeId:       p.Config.CLIOptions.Server.NodeId,
	})
	if err != nil {
		p.log.Errorf("failed to register with remote control server. remote control not available: %s", err)
		conn.Close()
		foremanGRPCConn.Close()
		return false
	}

	if !authResp.Success {
		p.log.Errorf("failed to register with remote control server. remote control not available: %s", authResp.Message)
		conn.Close()
		foremanGRPCConn.Close()
		return false
	}

	// Start a second plumber gRPC server over the yamux session so that
	// Foreman can send gRPC calls to this Plumber instance
	if err := p.startGRPCServer(foremanConn, foremanAddr); err != nil {
		p.log.Fatalf("unable to run remote control gRPC server: %s", err)
	}

	// Monitor for connection loss and attempt to reconnect
	go p.watchForForemanDisconnect(foremanConn)

	return true
}

// watchForForemanDisconnect watches for a disconnect from the foreman plumber control service
// If it detects a disconnect, it will attempt to continually retry the connection
func (p *Plumber) watchForForemanDisconnect(sess *yamux.Session) {
	ch := sess.CloseChan()

	// Block until connection is closed/lost
	<-ch

	// Try to reconnect w/backoff
	var i int
	for {
		retry := ForemanReconnectPolicy.Duration(i)
		p.log.Errorf("No connection to remote control server. Remote control not available, attempting "+
			"reconnect in %s", retry.String())
		time.Sleep(retry)

		if ok := p.runRemoteControl(); ok {
			p.log.Info("Reconnected successfully to remote control server")
			// Reconnected successfully, exit out of this goroutine
			// Another one will be started for the new connection in runRemoteControl()
			return
		}

		i++
	}
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

	p.Telemetry.Enqueue(posthog.Capture{
		Event:      "command_server",
		DistinctId: p.PersistentConfig.ClusterID,
		Properties: map[string]interface{}{
			"cluster_id":             p.CLIOptions.Server.ClusterId,
			"node_id":                p.CLIOptions.Server.NodeId,
			"use_tls":                p.CLIOptions.Server.UseTls,
			"enable_cluster":         p.CLIOptions.Server.EnableCluster,
			"tls_skip_verify":        p.CLIOptions.Server.TlsSkipVerify,
			"remote_control_enabled": p.CLIOptions.Server.RemoteControlEnabled,
		},
	})

	p.log.Debugf("starting gRPC server on %s", p.CLIOptions.Server.GrpcListenAddress)

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
