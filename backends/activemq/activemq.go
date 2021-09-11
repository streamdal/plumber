package activemq

import (
	"context"
	"time"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber/types"
	"github.com/go-stomp/stomp/v3"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	BackendName = "activemq"
)

type ActiveMq struct {
	connCfg  *protos.ConnectionConfig
	connArgs *args.ActiveMQConn
	log      *logrus.Entry
}

func New(connCfg *protos.ConnectionConfig) (*ActiveMq, error) {
	if err := validateConnCfg(connCfg); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	return &ActiveMq{
		connCfg:  connCfg,
		connArgs: connCfg.GetActiveMq(),
		log:      logrus.WithField("backend", "activemq"),
	}, nil
}

func newConn(ctx context.Context, connArgs *args.ActiveMQConn) (*stomp.Conn, error) {
	o := func(*stomp.Conn) error {
		return nil
	}

	doneCh := make(chan struct{}, 1)
	errCh := make(chan error, 1)

	var conn *stomp.Conn
	var err error

	go func() {
		// We are using an aggressive heartbeat because either the library or
		// the activemq server tends to drop connections frequently.
		conn, err = stomp.Dial("tcp", connArgs.Address, o,
			stomp.ConnOpt.HeartBeat(5*time.Second, time.Second))
		if err != nil {
			errCh <- errors.Wrap(err, "unable to connect to backend")
		}

		doneCh <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		return nil, errors.New("context cancelled before backend connected")
	case err := <-errCh:
		return nil, err
	case <-doneCh:
		break
	}

	return conn, nil
}

func (a *ActiveMq) Name() string {
	return BackendName
}

func (a *ActiveMq) Close(ctx context.Context) error {
	return nil
}

func (a *ActiveMq) Test(ctx context.Context) error {
	return errors.New("not implemented")
}

func (a *ActiveMq) Relay(ctx context.Context, relayCh chan interface{}, errorCh chan *types.ErrorMessage) error {
	return types.UnsupportedFeatureErr
}

// getDestination determines the correct string to pass to stomp.Subscribe()
func (a *ActiveMq) getDestination() string {
	if a.baseConnConfig.ActiveMq.Topic != "" {
		return "/topic/" + a.baseConnConfig.ActiveMq.Topic
	}

	return a.baseConnConfig.ActiveMq.Queue
}

func validateConnCfg(cfg *protos.ConnectionConfig) error {
	if cfg == nil {
		return errors.New("connection config cannot be nil")
	}

	if cfg.Conn == nil {
		return errors.New("conn in connection config cannot be nil")
	}

	if cfg.GetActiveMq() == nil {
		return errors.New("connection args cannot be nil")
	}

	return nil
}
