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

func (a *Actions) CreateDynamic(ctx context.Context, dynamicOpts *opts.DynamicOptions) (*types.Dynamic, error) {
	if err := validate.DynamicOptionsForServer(dynamicOpts); err != nil {
		return nil, errors.Wrap(err, "unable to validate dynamic options")
	}

	// Get stored connection information
	conn := a.cfg.PersistentConfig.GetConnection(dynamicOpts.ConnectionId)
	if conn == nil {
		return nil, validate.ErrConnectionNotFound
	}

	// Try to create a backend from given connection options
	be, err := backends.New(conn.Connection)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create backend")
	}

	// Used to shutdown dynamic replay on StopDynamic() gRPC call
	shutdownCtx, shutdownFunc := context.WithCancel(context.Background())

	d := &types.Dynamic{
		Active:     false,
		Id:         dynamicOpts.XDynamicId,
		CancelCtx:  shutdownCtx,
		CancelFunc: shutdownFunc,
		Backend:    be,
		Options:    dynamicOpts,
	}

	// Start the dynamic replay if it's active on other plumber instances
	if dynamicOpts.XActive {
		if err := d.StartDynamic(5 * time.Second); err != nil {
			return nil, errors.Wrap(err, "unable to start replay")
		}

		d.Active = true
		d.Options.XActive = true

		// Update metrics
		prometheus.IncrPromGauge(prometheus.PlumberDynamicReplays)
	}

	a.cfg.PersistentConfig.SetDynamic(dynamicOpts.XDynamicId, d)

	return d, nil
}

func (a *Actions) ResumeDynamic(ctx context.Context, dynamicID string) (*types.Dynamic, error) {
	d := a.cfg.PersistentConfig.GetDynamic(dynamicID)
	if d == nil {
		return nil, errors.New("dynamic replay does not exist")
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

	if err := d.StartDynamic(5 * time.Second); err != nil {
		return nil, errors.Wrap(err, "unable to start dynamic replay")
	}

	d.Active = true
	d.Options.XActive = true

	// Update metrics
	prometheus.IncrPromGauge(prometheus.PlumberDynamicReplays)

	return d, nil
}

func (a Actions) StopDynamic(ctx context.Context, dynamicID string) error {
	d := a.cfg.PersistentConfig.GetDynamic(dynamicID)
	if d == nil {
		return errors.New("Dynamic replay does not exist")
	}

	if !d.Active {
		return errors.New("Dynamic replay is not active")
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
	prometheus.DecrPromGauge(prometheus.PlumberDynamicReplays)

	return nil
}

func (a *Actions) UpdateDynamic(ctx context.Context, dynamicID string, dynamicOpts *opts.DynamicOptions) (*types.Dynamic, error) {
	d := a.cfg.PersistentConfig.GetDynamic(dynamicID)
	if d == nil {
		return nil, errors.New("dynamic replay does not exist")
	}

	d.Options = dynamicOpts

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

	if err := d.StartDynamic(5 * time.Second); err != nil {
		return nil, errors.Wrap(err, "unable to start dynamic replay")
	}

	d.Active = true
	d.Options.XActive = true

	// Update in-memory config
	a.cfg.PersistentConfig.SetDynamic(dynamicID, d)

	// Update metrics
	prometheus.IncrPromGauge(prometheus.PlumberDynamicReplays)

	return d, nil
}

func (a *Actions) DeleteDynamic(ctx context.Context, dynamicID string) error {
	dynamicReplay := a.cfg.PersistentConfig.GetDynamic(dynamicID)
	if dynamicReplay == nil {
		return errors.New("dynamic replay does not exist")
	}

	// Stop grpc client connection so we no longer receive messages from dProxy
	if dynamicReplay.Active {
		// Cancel reader worker
		dynamicReplay.CancelFunc()

		// Give it a sec to finish
		time.Sleep(time.Second)

		// Clean up gRPC connection to dProxy and connection to client's backend message bus
		dynamicReplay.Close()
	}

	// Delete in memory
	a.cfg.PersistentConfig.DeleteService(dynamicID)

	// Update metrics
	prometheus.DecrPromGauge(prometheus.PlumberDynamicReplays)

	return nil
}
