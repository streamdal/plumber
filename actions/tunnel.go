package actions

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber/backends"
	"github.com/batchcorp/plumber/prometheus"
	"github.com/batchcorp/plumber/server/types"
	"github.com/batchcorp/plumber/validate"
)

func (a *Actions) CreateTunnel(reqCtx context.Context, tunnelOpts *opts.TunnelOptions) (*types.Tunnel, error) {
	if err := validate.TunnelOptionsForServer(tunnelOpts); err != nil {
		return nil, errors.Wrap(err, "unable to validate tunnel options")
	}

	// Get stored connection information
	conn := a.cfg.PersistentConfig.GetConnection(tunnelOpts.ConnectionId)
	if conn == nil {
		return nil, validate.ErrConnectionNotFound
	}

	// Try to create a backend from given connection options
	be, err := backends.New(conn.Connection)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create backend")
	}

	// Used to shutdown tunnel on StopTunnel() gRPC call
	shutdownCtx, shutdownFunc := context.WithCancel(context.Background())

	d := &types.Tunnel{
		Active:           false,
		Id:               tunnelOpts.XTunnelId,
		CancelCtx:        shutdownCtx,
		CancelFunc:       shutdownFunc,
		Backend:          be,
		Options:          tunnelOpts,
		PlumberClusterID: a.cfg.PersistentConfig.ClusterID,
	}

	// If a tunnel is in the process of starting and it gets deleted, we must have
	// CancelFunc and CancelCtx set so that DeleteTunnel() can trigger
	a.cfg.PersistentConfig.SetTunnel(tunnelOpts.XTunnelId, d)

	// Run the tunnel if it's active on other plumber instances
	if tunnelOpts.XActive {
		// This will block for 5 seconds
		if err := d.StartTunnel(5 * time.Second); err != nil {
			return nil, errors.Wrap(err, "unable to start tunnel")
		}

		d.Active = true
		d.Options.XActive = true

		// Update metrics
		prometheus.IncrPromGauge(prometheus.PlumberTunnels)
	}

	a.cfg.PersistentConfig.SetTunnel(tunnelOpts.XTunnelId, d)
	a.cfg.PersistentConfig.Save()

	return d, nil
}

func (a *Actions) ResumeTunnel(ctx context.Context, tunnelID string) (*types.Tunnel, error) {
	d := a.cfg.PersistentConfig.GetTunnel(tunnelID)
	if d == nil {
		return nil, errors.New("tunnel does not exist")
	}

	// New contexts
	ctx, cancelFunc := context.WithCancel(context.Background())
	d.CancelCtx = ctx
	d.CancelFunc = cancelFunc

	// New backend connection
	conn := a.cfg.PersistentConfig.GetConnection(d.Options.ConnectionId)
	if conn == nil {
		return nil, validate.ErrConnectionNotFound
	}

	be, err := backends.New(conn.Connection)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create backend connection")
	}
	d.Backend = be

	if err := d.StartTunnel(5 * time.Second); err != nil {
		return nil, errors.Wrap(err, "unable to start tunnel")
	}

	d.Active = true
	d.Options.XActive = true

	// Update metrics
	prometheus.IncrPromGauge(prometheus.PlumberTunnels)

	a.cfg.PersistentConfig.SetTunnel(tunnelID, d)
	a.cfg.PersistentConfig.Save()

	return d, nil
}

func (a Actions) StopTunnel(ctx context.Context, tunnelID string) (*types.Tunnel, error) {
	d := a.cfg.PersistentConfig.GetTunnel(tunnelID)
	if d == nil {
		return nil, errors.New("Tunnel replay does not exist")
	}

	if !d.Active {
		return nil, errors.New("Tunnel replay is not active")
	}

	// Stop grpc client connection so we no longer receive messages from dProxy
	d.CancelFunc()
	d.Active = false
	d.Options.XActive = false

	// Give it a sec
	time.Sleep(time.Second)

	// Close gRPC connection to dProxy and backend connection to user's message bus
	d.Close()

	// Update metrics
	prometheus.DecrPromGauge(prometheus.PlumberTunnels)

	a.cfg.PersistentConfig.SetTunnel(tunnelID, d)
	a.cfg.PersistentConfig.Save()

	return d, nil
}

func (a *Actions) UpdateTunnel(ctx context.Context, tunnelID string, tunnelOpts *opts.TunnelOptions) (*types.Tunnel, error) {
	d := a.cfg.PersistentConfig.GetTunnel(tunnelID)
	if d == nil {
		return nil, errors.New("tunnel does not exist")
	}

	if d.Active {
		// Close existing tunnel
		d.CancelFunc()
		d.Active = false
		d.Options.XActive = false
		d.Close()

		// Give it a sec to close out connections and goroutines
		time.Sleep(time.Second)
	}

	d.Options = tunnelOpts

	// New contexts
	ctx, cancelFunc := context.WithCancel(context.Background())
	d.CancelCtx = ctx
	d.CancelFunc = cancelFunc

	// New backend connection
	conn := a.cfg.PersistentConfig.GetConnection(d.Options.ConnectionId)
	if conn == nil {
		return nil, validate.ErrConnectionNotFound
	}

	be, err := backends.New(conn.Connection)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create backend connection")
	}
	d.Backend = be

	if tunnelOpts.XActive == true {
		if err := d.StartTunnel(5 * time.Second); err != nil {
			return nil, errors.Wrap(err, "unable to start tunnel")
		}

		d.Active = true
		d.Options.XActive = true
	}

	// Update in-memory config
	a.cfg.PersistentConfig.SetTunnel(tunnelID, d)
	a.cfg.PersistentConfig.Save()

	// Update metrics
	prometheus.IncrPromGauge(prometheus.PlumberTunnels)

	return d, nil
}

func (a *Actions) DeleteTunnel(ctx context.Context, tunnelID string) error {
	tunnelCfg := a.cfg.PersistentConfig.GetTunnel(tunnelID)
	if tunnelCfg == nil {
		return errors.New("tunnel does not exist")
	}

	// Stop grpc client connection so we no longer receive messages from dProxy
	if tunnelCfg.Active {
		// Cancel reader worker
		tunnelCfg.CancelFunc()

		// Give it a sec to finish
		time.Sleep(time.Second)

		// Clean up gRPC connection to dProxy and connection to client's backend message bus
		tunnelCfg.Close()
	}

	// Delete in memory
	a.cfg.PersistentConfig.DeleteTunnel(tunnelID)
	a.cfg.PersistentConfig.Save()

	// Update metrics
	prometheus.DecrPromGauge(prometheus.PlumberTunnels)

	return nil
}
