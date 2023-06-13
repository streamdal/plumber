package plumber

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"github.com/posthog/posthog-go"
	"github.com/relistan/go-director"
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
	if err := p.downloadWasmUpdates(p.ServiceShutdownCtx); err != nil {
		return errors.Wrap(err, "unable to download wasm updates")
	}

	mode := "standalone"

	if p.Config.CLIOptions.Server.EnableCluster {
		mode = "cluster"
	}

	p.log.Infof("starting plumber server in '%s' mode...", mode)

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
	if err := p.startGRPCServer(); err != nil {
		p.log.Fatalf("unable to run gRPC server: %s", err)
	}

	p.log.Info("plumber server started")

	// Launch HTTP server
	srv, err := api.Start(&api.Config{
		PersistentConfig: p.PersistentConfig,
		Bus:              b,
		ListenAddress:    p.CLIOptions.Server.HttpListenAddress,
		Version:          options.VERSION,
	})
	if err != nil {
		logrus.Fatalf("unable to start API server: %s", err)
	}

	// Running in a goroutine to prevent blocking of server startup due to possible long connect timeouts
	go func() {
		if err := p.relaunchRelays(); err != nil {
			p.log.Error(errors.Wrap(err, "failed to relaunch relays"))
		}

		if err := p.relaunchTunnels(); err != nil {
			p.log.Error(errors.Wrap(err, "failed to relaunch tunnels"))
		}
	}()

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

func (p *Plumber) startGRPCServer() error {
	lis, err := net.Listen("tcp", p.CLIOptions.Server.GrpcListenAddress)
	if err != nil {
		return fmt.Errorf("unable to listen on '%s': %s", p.CLIOptions.Server.GrpcListenAddress, err)
	}

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
		KV:               p.PersistentConfig.KV,
		DataAlerts:       make(chan *protos.SendRuleNotificationRequest, 100),
		ShutdownCtx:      p.ServiceShutdownCtx,
	}

	protos.RegisterPlumberServerServer(grpcServer, plumberServer)

	// Check for WASM updates every few hours
	looper := director.NewTimedLooper(director.FOREVER, WasmUpdateInterval, make(chan error, 1))
	go p.pollForWASMUpdates(looper)

	go p.watchServiceShutdown(grpcServer)
	go plumberServer.StartRuleAlerts()

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
		if err := grpcServer.Serve(lis); err != nil {
			errCh <- errors.Wrap(err, "unable to start gRPC server")
		}
	}()

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

func (p *Plumber) downloadWasmUpdates(ctx context.Context) error {
	p.log.Debugf("Checking for wasm updates")

	updateWasm, err := p.PersistentConfig.WasmUpdateExists(ctx, http.DefaultClient)
	if err != nil {
		return errors.Wrap(err, "unable to check for wasm updates")
	}

	if !updateWasm {
		p.log.Debugf("No WASM updates found")
		return nil
	}

	wasmVersion, err := p.PersistentConfig.PullLatestWASMRelease(ctx, http.DefaultClient)
	if err != nil {
		return errors.Wrap(err, "unable to pull latest WASM release")
	}

	p.log.Debugf("WASM updates downloaded '%s'", wasmVersion)

	return nil
}

func (p *Plumber) pollForWASMUpdates(looper director.Looper) {
	p.log.Debug("Starting WASM update polling")

	var quit bool
	looper.Loop(func() error {
		// Give looper time to quit
		if quit {
			time.Sleep(time.Millisecond * 100)
			return nil
		}

		select {
		case <-p.ServiceShutdownCtx.Done():
			quit = true
			looper.Quit()
			p.log.Debug("WASM update polling stopped")
			return nil
		default:
			// NOOP
		}

		if err := p.downloadWasmUpdates(p.ServiceShutdownCtx); err != nil {
			p.log.Errorf("unable to download WASM updates: %s", err)
		}

		return nil
	})
}
