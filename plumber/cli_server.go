package plumber

import (
	"fmt"
	"net"
	"time"

	"github.com/batchcorp/plumber/api"
	"github.com/batchcorp/plumber/options"
	"github.com/nakabonne/tstorage"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/batchcorp/plumber-schemas/build/go/protos"

	"github.com/batchcorp/plumber/embed/etcd"
	"github.com/batchcorp/plumber/github"
	"github.com/batchcorp/plumber/monitor"
	"github.com/batchcorp/plumber/server"
	"github.com/batchcorp/plumber/stats"
	"github.com/batchcorp/plumber/uierrors"
	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/vcservice"
)

// RunServer is a wrapper for starting embedded etcd and starting the gRPC server.
func (p *Plumber) RunServer() error {
	p.log.Info("starting embedded etcd")

	if err := p.startEtcd(); err != nil {
		return errors.Wrap(err, "unable to start embedded etcd")
	}

	m, err := monitor.New(p.Etcd.Client(), p.Config.CLIOptions.Server.NodeId)
	if err != nil {
		return errors.Wrap(err, "unable to create monitor instance")
	}

	p.log.Info("starting leader election for alerts")

	leaderChan := make(chan *monitor.ElectLeaderStatus, 1)

	alertsLeaderPath := fmt.Sprintf("/%s/monitor/leader", p.CLIOptions.Server.ClusterId)

	go m.RunElectLeader(p.ServiceShutdownCtx, leaderChan, alertsLeaderPath)

	// Bail out if initial election ran into errors
	timeoutCh := time.After(5 * time.Second)

	select {
	case status := <-leaderChan:
		if status.Err != nil {
			return errors.Wrap(status.Err, "unable to complete leader election")
		}

		// It is OK if we didn't get elected as leader - we only need to make
		// sure that leader election worked without error.
		p.log.Debugf("leader election process complete; elected leader '%s'", status.NodeID)
	case <-timeoutCh:
		// Timeout hit - no errors, all is well
		break
	}

	// Launch HTTP server
	go func() {
		if err := api.Start(p.CLIOptions.Server.HttpListenAddress, options.VERSION); err != nil {
			logrus.Fatalf("unable to start API server: %s", err)
		}
	}()

	p.log.Info("starting gRPC server")

	// Blocks
	if err := p.runServer(); err != nil {
		return errors.Wrap(err, "unable to run server")
	}

	return nil
}

func (p *Plumber) startEtcd() error {
	e, err := etcd.New(p.Config.CLIOptions.Server, p.PersistentConfig, p.Actions)
	if err != nil {
		return errors.Wrap(err, "unable to instantiate etcd")
	}

	if err := e.Start(p.Config.ServiceShutdownCtx); err != nil {
		return errors.Wrap(err, "unable to start embedded etcd")
	}

	p.Etcd = e

	p.log.Debugf("embedded etcd listener address: %s", p.Config.CLIOptions.Server.ListenerClientUrl)

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

	// Each plumber node needs a unique ID
	p.PersistentConfig.PlumberID = p.CLIOptions.Server.NodeId

	// Only start if we have an authentication token for the service
	var vcService vcservice.IVCService
	if p.PersistentConfig.VCServiceToken != "" {
		//var err error
		//vcService, err = vcservice.New(&vcservice.Config{
		//	EtcdService:      p.Etcd,
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
		tstorage.WithDataPath("./.plumber/"+p.CLIOptions.Server.StatsDatabasePath),
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

	errorsService, err := uierrors.New(&uierrors.Config{
		EtcdService: p.Etcd,
	})
	if err != nil {
		return errors.Wrap(err, "unable to create uierrors service")
	}

	if err != nil {
		return errors.Wrap(err, "unable to create actions service")
	}

	plumberServer := &server.Server{
		Actions:          p.Actions,
		PersistentConfig: p.PersistentConfig,
		AuthToken:        p.CLIOptions.Server.AuthToken,
		VCService:        vcService,
		GithubService:    ghService,
		StatsService:     statsService,
		ErrorsService:    errorsService,
		Log:              logrus.WithField("pkg", "plumber/cli_server.go"),
		Etcd:             p.Etcd,
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
