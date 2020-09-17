package relay

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"time"

	"github.com/batchcorp/schemas/build/go/services"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh/terminal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

const (
	DefaultNumRelayers = 10
)

type Relay struct {
	Config *Config
	log    *logrus.Entry
}

type Config struct {
	Token       string
	GRPCAddress string
	NumRelayers int
	RelayCh     chan interface{}
	Timeout     time.Duration // general grpc timeout (used for all grpc calls)
}

func New(relayCfg *Config) (*Relay, error) {
	if err := validateConfig(relayCfg); err != nil {
		return nil, errors.Wrap(err, "unable to complete relay config validation")
	}

	// Verify grpc connection & token
	if err := TestConnection(relayCfg); err != nil {
		return nil, errors.Wrap(err, "unable to complete connection test")
	}

	// JSON formatter for log output if not running in a TTY - colors are fun!
	if !terminal.IsTerminal(int(os.Stderr.Fd())) {
		logrus.SetFormatter(&logrus.JSONFormatter{})
	}

	return &Relay{
		Config: relayCfg,
		log:    logrus.WithField("pkg", "relay"),
	}, nil
}

func validateConfig(cfg *Config) error {
	if cfg == nil {
		return errors.New("Relay config cannot be nil")
	}

	if cfg.Token == "" {
		return errors.New("Token cannot be empty")
	}

	if cfg.GRPCAddress == "" {
		return errors.New("GRPCAddress cannot be empty")
	}

	if cfg.RelayCh == nil {
		return errors.New("RelayCh cannot be nil")
	}

	if cfg.NumRelayers <= 0 {
		logrus.Warningf("NumRelayers cannot be <= 0 - setting to default '%d'", DefaultNumRelayers)
		cfg.NumRelayers = DefaultNumRelayers
	}

	return nil
}

func TestConnection(cfg *Config) error {
	conn, ctx, err := NewConnection(cfg.GRPCAddress, cfg.Token, cfg.Timeout)
	if err != nil {
		return errors.Wrap(err, "unable to create new connection")
	}

	// Call the Test method to verify connectivity
	c := services.NewGRPCCollectorClient(conn)

	if _, err := c.Test(ctx, &services.TestRequest{}); err != nil {
		return errors.Wrap(err, "unable to complete Test request")
	}

	return nil
}

func NewConnection(address, token string, timeout time.Duration) (*grpc.ClientConn, context.Context, error) {
	opts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithTransportCredentials(credentials.NewTLS(
			&tls.Config{
				InsecureSkipVerify: true,
			},
		)),
	}

	dialContext, _ := context.WithTimeout(context.Background(), timeout)

	conn, err := grpc.DialContext(dialContext, address, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to connect to grpc address '%s': %s", address, err)
	}

	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	md := metadata.Pairs("batch.token", token)
	outCtx := metadata.NewOutgoingContext(ctx, md)

	defer cancel()

	return conn, outCtx, nil
}

func (r *Relay) StartRelayers() {
	for i := 0; i != r.Config.NumRelayers; i++ {
		go r.Run(i)
	}
}

func (r *Relay) Run(id int) {
	llog := r.log.WithField("relayId", id)

	llog.Debug("Relayer started")

	// TODO: Should add batching support

	for {
		msg := <-r.Config.RelayCh

		switch v := msg.(type) {
		case *sqs.Message:
			fmt.Printf("Received an SQS message: %v\n", v)
		default:
			fmt.Println("Received unknown message type: ", v)
		}
	}

	llog.Debug("Relayer is exiting")
}
