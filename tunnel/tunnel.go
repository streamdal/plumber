package tunnel

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	"github.com/batchcorp/collector-schemas/build/go/protos/events"
	"github.com/batchcorp/collector-schemas/build/go/protos/services"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

const (
	// DefaultTunnelAddress is the default address that the tunnel pkg will
	// use if an alternate address is not specified
	DefaultTunnelAddress = "dproxy.batch.sh:443"

	// ReconnectSleep determines the length of time to wait between reconnect attempts to dProxy
	ReconnectSleep = time.Second * 5

	DefaultGRPCTimeoutSeconds = 5
)

var (
	ErrNotAuthorized = errors.New("not authorized")
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 . ITunnel
type ITunnel interface {
	Start(ctx context.Context, bus string, errorCh chan<- *records.ErrorRecord) error
	Read() chan *events.Outbound
	Close() error
}

type Client struct {
	Client            services.DProxyClient
	Conn              *grpc.ClientConn
	Token             string
	log               *logrus.Entry
	PlumberClusterID  string
	OutboundMessageCh chan *events.Outbound

	Options *opts.TunnelOptions
}

// New validates CLI options and returns a new Client struct
func New(opts *opts.TunnelOptions, plumberClusterID string) (ITunnel, error) {
	if err := validateTunnelOptions(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate tunnel options")
	}

	grpcConnTimeout := time.Duration(opts.XGrpcTimeoutSeconds) * time.Second

	ctx, _ := context.WithTimeout(context.Background(), grpcConnTimeout)

	conn, err := grpc.DialContext(ctx, opts.XGrpcAddress, getDialOptions(opts)...)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to open connection to %s", opts.XGrpcAddress)
	}

	dClient := &Client{
		Client:            services.NewDProxyClient(conn),
		Conn:              conn,
		Token:             opts.ApiToken,
		OutboundMessageCh: make(chan *events.Outbound, 1),
		Options:           opts,
		PlumberClusterID:  plumberClusterID,
		log:               logrus.WithField("pkg", "tunnel"),
	}

	return dClient, nil
}

func validateTunnelOptions(opts *opts.TunnelOptions) error {
	if opts == nil {
		return errors.New("opts cannot be nil")
	}

	if opts.ApiToken == "" {
		return errors.New("api token cannot be empty")
	}

	if opts.XGrpcAddress == "" {
		opts.XGrpcAddress = DefaultTunnelAddress
	}

	if opts.XGrpcTimeoutSeconds == 0 {
		opts.XGrpcTimeoutSeconds = DefaultGRPCTimeoutSeconds
	}

	return nil
}

func (d *Client) Close() error {
	return d.Conn.Close()
}

func (d *Client) reconnect() error {
	conn, err := grpc.Dial(d.Options.XGrpcAddress, getDialOptions(d.Options)...)
	if err != nil {
		return errors.Wrapf(err, "unable to reconnect to %s", d.Options.XGrpcAddress)
	}

	d.Conn = conn

	d.Client = services.NewDProxyClient(conn)
	return nil
}

// getDialOptions returns all necessary grpc dial options to connect to dProxy
func getDialOptions(opts *opts.TunnelOptions) []grpc.DialOption {
	dialOpts := []grpc.DialOption{grpc.WithBlock()}

	if opts.XGrpcInsecure {
		dialOpts = append(dialOpts, grpc.WithInsecure())
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(
			&tls.Config{
				InsecureSkipVerify: true,
			},
		)))
	}

	return dialOpts
}

func (d *Client) Read() chan *events.Outbound {
	return d.OutboundMessageCh
}

// Start is called by a backend's Tunnel() method. It authorizes the connection
// and begins reading a GRPC stream of responses consisting of Tunnel protobuf
// messages.
func (d *Client) Start(ctx context.Context, bus string, errorCh chan<- *records.ErrorRecord) error {
	go func() {
		var err error
		var stream services.DProxy_ConnectClient

		// TODO: Should be a looper
		for {
			if stream == nil {
				// Try to connect forever
				stream, err = d.connect(ctx, bus)
				if err != nil {
					if err == context.Canceled {
						d.notify(err, "context cancelled during connect", errorCh, logrus.DebugLevel)
						break
					}

					// Error we can probably recover from -> force reconnect after sleep
					stream = nil
					d.log.Error(err)
					time.Sleep(ReconnectSleep)

					continue
				}
			}

			response, err := stream.Recv()
			if err != nil {
				if err.Error() == "rpc error: code = Canceled desc = context canceled" {
					d.notify(err, "context cancelled during recv", errorCh, logrus.DebugLevel)
					break
				}

				if errors.Is(err, io.EOF) {
					// Nicer reconnect messages
					d.log.Warningf("dProxy server is unavailable, retrying in %s...", ReconnectSleep.String())
				} else {
					d.log.Warningf("Error receiving message, retrying in %s: %s", ReconnectSleep.String(), err)
				}

				// Stream is no longer useful. Need to get a new one on reconnect
				stream = nil

				// Attempt reconnect. On the next loop iteration, stream == nil check will be hit, and assuming we've
				// reconnected at that point, a new stream will be opened and authorized
				d.reconnect()

				time.Sleep(ReconnectSleep)
				continue
			}

			if err := d.handleResponse(ctx, response); err != nil {
				if err == ErrNotAuthorized {
					d.notify(err, "API token was not accepted for tunnel - bailing out", errorCh, logrus.DebugLevel)
					break
				}

				d.log.Warningf("error handling response for msg type '%s' (recoverable): %s", response.Type, err)
			}
		}

		d.log.Debug("Start() goroutine exiting")
	}()

	return nil
}

func (d *Client) notify(err error, desc string, errorCh chan<- *records.ErrorRecord, logLevel logrus.Level) {
	if errorCh != nil {
		errRecord := &records.ErrorRecord{
			OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
			Error:               err.Error(),
		}

		if desc != "" {
			errRecord.Metadata = map[string][]byte{
				"desc": []byte(desc),
			}
		}

		errorCh <- errRecord
	}

	var fullErr string

	if desc != "" {
		fullErr = fmt.Sprintf("%s: %s", desc, err.Error())
	} else {
		fullErr = err.Error()
	}

	d.log.Logger.Log(logLevel, fullErr)
}

// connect opens a GRPC connection to dProxy. It is called by Start
func (d *Client) connect(ctx context.Context, bus string) (services.DProxy_ConnectClient, error) {
	authRequest := &events.Tunnel{
		Type: events.Tunnel_AUTH_REQUEST,
		Payload: &events.Tunnel_AuthRequest{
			AuthRequest: &events.AuthRequest{
				Token:            d.Token,
				MessageBus:       bus,
				PlumberClusterId: d.PlumberClusterID,
				Name:             d.Options.Name,
				DynamicId:        d.Options.XDynamicId,
			},
		},
	}

	// dProxy needs a unique ID to track the connection
	// This value can be anything, so long as it is unique
	md := metadata.Pairs("plumber-stream-id", uuid.NewV4().String())
	ctx = metadata.NewOutgoingContext(ctx, md)

	return d.Client.Connect(ctx, authRequest)
}

// handleResponse receives a tunnel message and determines which method should handle it based on the payload
func (d *Client) handleResponse(_ context.Context, resp *events.Tunnel) error {
	switch resp.Type {
	case events.Tunnel_AUTH_RESPONSE:
		return d.handleAuthResponse(resp)
	case events.Tunnel_OUTBOUND_MESSAGE:
		return d.handleOutboundMessage(resp)
	case events.Tunnel_REPLAY_EVENT:
		return d.handleReplayEvent(resp)
	case events.Tunnel_UNSET:
		// Noop used by dproxy to keep connection open
		return nil
	default:
		return errors.Errorf("unknown response type: %s", resp.Type)
	}
}

// handleAuthResponse handles a AUTH_RESPONSE payload from a Tunnel protobuf message
func (d *Client) handleAuthResponse(resp *events.Tunnel) error {
	authResponse := resp.GetAuthResponse()
	if authResponse == nil {
		if err := d.Conn.Close(); err != nil {
			d.log.Error("could not cleanly disconnect from server")
		}

		return fmt.Errorf("received invalid auth response from server: %+v", authResponse)
	}

	if authResponse.Authorized == false {
		if err := d.Conn.Close(); err != nil {
			d.log.Error("could not cleanly disconnect from server")
		}

		return ErrNotAuthorized
	}

	d.log.Debug("Connection authorized for replay tunnel")

	return nil
}

// handleOutboundMessage handles a REPLAY_MESSAGE payload from a Tunnel protobuf message
func (d *Client) handleOutboundMessage(resp *events.Tunnel) error {
	d.log.Debugf("received message for replay '%s'", resp.ReplayId)

	outbound := resp.GetOutboundMessage()

	// Ignore
	if outbound.Last {
		return nil
	}

	d.OutboundMessageCh <- outbound

	return nil
}

// handleOutboundMessage handles a REPLAY_MESSAGE payload from a Tunnel protobuf message
func (d *Client) handleReplayEvent(resp *events.Tunnel) error {
	llog := d.log.WithField("replay_id", resp.ReplayId)
	event := resp.GetReplayMessage()

	switch event.Type {
	case events.ReplayEvent_CREATE_REPLAY:
		llog.Debug("Replay starting")
	case events.ReplayEvent_PAUSE_REPLAY:
		llog.Info("Replay paused")
	case events.ReplayEvent_RESUME_REPLAY:
		llog.Info("Replay resuming")
	case events.ReplayEvent_ABORT_REPLAY:
		llog.Error("Replay aborted")
	case events.ReplayEvent_FINISH_REPLAY:
		llog.Info("Replay finished!")
	}

	return nil
}
