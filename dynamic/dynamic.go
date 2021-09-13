package dynamic

import (
	"context"
	"crypto/tls"
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/schemas/build/go/events"
	"github.com/batchcorp/schemas/build/go/services"
)

const (
	// ReconnectSleep determines the length of time to wait between reconnect attempts to dProxy
	ReconnectSleep = time.Second * 5
)

type Client struct {
	Client            services.DProxyClient
	Conn              *grpc.ClientConn
	Token             string
	log               *logrus.Entry
	MessageBus        string
	OutboundMessageCh chan *events.Outbound

	Options *options.Options
}

// New validates CLI options and returns a new Client struct
func New(opts *options.Options, bus string) (*Client, error) {
	ctx, _ := context.WithTimeout(context.Background(), opts.DProxy.GRPCTimeout)

	conn, err := grpc.DialContext(ctx, opts.DProxy.Address, getDialOptions(opts)...)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to open connection to %s", opts.DProxy.Address)
	}

	dClient := &Client{
		Client:            services.NewDProxyClient(conn),
		Conn:              conn,
		Token:             opts.DProxy.APIToken,
		log:               logrus.WithField("pkg", "dproxy"),
		OutboundMessageCh: make(chan *events.Outbound, 1),
		MessageBus:        bus,
		Options:           opts,
	}

	return dClient, nil
}

func (d *Client) reconnect() error {
	conn, err := grpc.Dial(d.Options.DProxy.Address, getDialOptions(d.Options)...)
	if err != nil {
		return errors.Wrapf(err, "unable to open connection to %s", d.Options.DProxy.Address)
	}

	d.Client = services.NewDProxyClient(conn)
	return nil
}

// getDialOptions returns all necessary grpc dial options to connect to dProxy
func getDialOptions(opts *options.Options) []grpc.DialOption {
	dialOpts := []grpc.DialOption{grpc.WithBlock()}

	if opts.DProxy.Insecure {
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

// Start is called by a backend's Dynamic() method. It authorizes the connection and begins reading a GRPC stream of
// responses consisting of DynamicReplay protobuf messages
func (d *Client) Start() {
	d.log.Debug("Starting new dynamic destination client")
	var err error
	var stream services.DProxy_ConnectClient

	for {
		if stream == nil {
			// TODO: exponential backoff
			stream, err = d.authorize()
			if err != nil {
				d.log.Error(err)
				stream = nil
				time.Sleep(ReconnectSleep)
				continue
			}
		}

		response, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				// Nice reconnect messages
				d.log.Errorf("dProxy server is unavailable, retrying in %s...", ReconnectSleep.String())
			} else {
				d.log.Errorf("Error receiving message: %s", err)
			}

			// Stream is no longer useful. Need to get a new one on reconnect
			stream = nil

			// Attempt reconnect. On the next loop iteration, stream == nil check will be hit, and assuming we've
			// reconnected at that point, a new stream will be opened and authorized
			d.reconnect()

			time.Sleep(ReconnectSleep)
			continue
		}

		d.handleResponse(response)
	}
}

// authorize opens a GRPC connection to dProxy. It is called by Start
func (d *Client) authorize() (services.DProxy_ConnectClient, error) {
	authRequest := &services.DynamicReplay{
		Type: services.DynamicReplay_AUTH_REQUEST,
		Payload: &services.DynamicReplay_AuthRequest{
			AuthRequest: &services.AuthRequest{
				Token:      d.Token,
				MessageBus: d.MessageBus,
			},
		},
	}

	return d.Client.Connect(context.TODO(), authRequest)
}

// handleResponse receives a dynamic replay message and determines which method should handle it based on the payload
func (d *Client) handleResponse(resp *services.DynamicReplay) {
	switch resp.Type {
	case services.DynamicReplay_AUTH_RESPONSE:
		d.handleAuthResponse(resp)
	case services.DynamicReplay_OUTBOUND_MESSAGE:
		d.handleOutboundMessage(resp)
	case services.DynamicReplay_REPLAY_EVENT:
		d.handleReplayEvent(resp)
	}
}

// handleAuthResponse handles a AUTH_RESPONSE payload from a DynamicReplay protobuf message
func (d *Client) handleAuthResponse(resp *services.DynamicReplay) {
	authResponse := resp.GetAuthResponse()
	if authResponse == nil {
		if err := d.Conn.Close(); err != nil {
			d.log.Error("could not cleanly disconnect from server")
		}
		d.log.Fatalf("Received invalid authentication response from server")
		return // only here to quiet ide warnings
	}

	if authResponse.Authorized == false {
		if err := d.Conn.Close(); err != nil {
			d.log.Error("could not cleanly disconnect from server")
		}
		d.log.Fatalf("Could not authenticate: %s", authResponse.Message)
		return
	}

	d.log.Info("Connection authorized. Waiting for replay messages...")
}

// handleOutboundMessage handles a REPLAY_MESSAGE payload from a DynamicReplay protobuf message
func (d *Client) handleOutboundMessage(resp *services.DynamicReplay) {
	d.log.Debugf("received message for replay '%s'", resp.ReplayId)

	outbound := resp.GetOutboundMessage()

	// Ignore
	if outbound.Last {
		return
	}

	d.OutboundMessageCh <- outbound
}

// handleOutboundMessage handles a REPLAY_MESSAGE payload from a DynamicReplay protobuf message
func (d *Client) handleReplayEvent(resp *services.DynamicReplay) {
	llog := d.log.WithField("replay_id", resp.ReplayId)
	event := resp.GetReplayMessage()
	switch event.Type {
	case services.ReplayEvent_CREATE_REPLAY:
		llog.Info("Replay starting")
	case services.ReplayEvent_PAUSE_REPLAY:
		llog.Info("Replay paused")
	case services.ReplayEvent_RESUME_REPLAY:
		llog.Info("Replay resuming")
	case services.ReplayEvent_ABORT_REPLAY:
		llog.Error("Replay aborted")
	case services.ReplayEvent_FINISH_REPLAY:
		llog.Info("Replay finished!")
	}
}
