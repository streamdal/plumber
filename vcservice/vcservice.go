package vcservice

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/batchcorp/plumber/config"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/batchcorp/plumber/embed/etcd"
	"github.com/batchcorp/plumber/validate"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
)

const (
	// ReconnectSleep determines the length of time to wait between reconnect attempts to dProxy
	ReconnectSleep = time.Second * 5
)

type Config struct {
	EtcdService      etcd.IEtcd
	ServerOptions    *opts.ServerOptions
	PersistentConfig *config.Config
}

type Client struct {
	*Config
	Client               protos.VCServiceClient
	Conn                 *grpc.ClientConn
	log                  *logrus.Entry
	EventsCh             chan *protos.VCEvent
	Context              context.Context
	CancelFunc           context.CancelFunc
	AttachedStreams      map[string]*AttachedStream
	AttachedStreamsMutex *sync.RWMutex
}

type AttachedStream struct {
	EventsCh chan *protos.VCEvent
}

type IVCService interface {
	AttachStream(id string) *AttachedStream
	DetachStream(id string)
}

// New validates CLI options and returns a new Client struct
func New(cfg *Config) (IVCService, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, errors.Wrap(err, "unable to validate dynamic options")
	}

	grpcConnTimeout := time.Duration(cfg.ServerOptions.VcserviceGrpcTimeoutSeconds) * time.Second

	ctx, _ := context.WithTimeout(context.Background(), grpcConnTimeout)

	conn, err := grpc.DialContext(ctx, cfg.ServerOptions.VcserviceGrpcAddress, getDialOptions(cfg.ServerOptions)...)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to open connection to %s", cfg.ServerOptions.VcserviceGrpcAddress)
	}

	cancelCtx, cancelFunc := context.WithCancel(context.Background())

	// TODO: we need attached clients like backend reads so that we don't block events
	vcClient := &Client{
		Config:               cfg,
		Client:               protos.NewVCServiceClient(conn),
		Conn:                 conn,
		log:                  logrus.WithField("pkg", "vcservice"),
		EventsCh:             make(chan *protos.VCEvent, 1),
		Context:              cancelCtx,
		CancelFunc:           cancelFunc,
		AttachedStreams:      make(map[string]*AttachedStream),
		AttachedStreamsMutex: &sync.RWMutex{},
	}

	return vcClient, nil
}

func validateConfig(cfg *Config) error {
	if cfg.ServerOptions == nil {
		return validate.ErrMissingServerOptions
	}

	if cfg.EtcdService == nil {
		return validate.ErrMissingEtcd
	}

	if cfg.PersistentConfig == nil {
		return validate.ErrMissingConfig
	}

	return nil
}

// getDialOptions returns all necessary grpc dial options to connect to dProxy
func getDialOptions(opts *opts.ServerOptions) []grpc.DialOption {
	dialOpts := []grpc.DialOption{grpc.WithBlock()}

	if opts.VcserviceGrpcInsecure {
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

func (c *Client) reconnect() error {
	conn, err := grpc.Dial(c.ServerOptions.VcserviceGrpcAddress, getDialOptions(c.ServerOptions)...)
	if err != nil {
		return errors.Wrapf(err, "unable to open connection to %s", c.ServerOptions.VcserviceGrpcAddress)
	}

	c.Client = protos.NewVCServiceClient(conn)
	return nil
}

func (c *Client) Start() {
	c.log.Debug("Starting new vc-service client")
	var err error
	var stream protos.VCService_ConnectClient

	for {
		// Allow context to be cancelled for token refreshes
		select {
		case <-c.Context.Done():
			return
		default:
			// NOOP
		}

		if stream == nil {
			// TODO: exponential backoff
			stream, err = c.authorize(c.Context)
			if err != nil {
				c.log.Error(err)
				stream = nil
				time.Sleep(ReconnectSleep)
				continue
			}
		}

		response, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				// Nice reconnect messages
				c.log.Errorf("vc-service server is unavailable, retrying in %s...", ReconnectSleep.String())
			} else {
				c.log.Errorf("Error receiving message: %s", err)
			}

			// Stream is no longer useful. Need to get a new one on reconnect
			stream = nil

			// Attempt reconnect. On the next loop iteration, stream == nil check will be hit, and assuming we've
			// reconnected at that point, a new stream will be opened and authorized
			c.reconnect()

			time.Sleep(ReconnectSleep)
			continue
		}

		// Handle any backend work that needs to be done
		if err := c.handleResponse(response); err != nil {
			c.log.Error("Unable to handle VCEvent: %s", err)
		}

		// Ship message to frontend
		c.AttachedStreamsMutex.RLock()
		for _, as := range c.AttachedStreams {
			as.EventsCh <- response
		}
		c.AttachedStreamsMutex.RUnlock()
	}
}

// authorize opens a GRPC connection to vc-service. It is called by vcservice.Client.Start()
func (c *Client) authorize(ctx context.Context) (protos.VCService_ConnectClient, error) {
	authRequest := &protos.ConnectAuthRequest{
		ApiToken: c.PersistentConfig.VCServiceToken,
	}

	return c.Client.Connect(ctx, authRequest)
}

// handleResponse receives a dynamic replay message and determines which method should handle it based on the payload
func (c *Client) handleResponse(resp *protos.VCEvent) error {
	switch resp.Type {
	case protos.VCEvent_AUTH_RESPONSE:
		return c.handleAuthResponse(resp)
	case protos.VCEvent_NEW_JWT_TOKEN:
		return c.handleNewJWTToken(resp.GetNewJwtToken())
	case protos.VCEvent_GITHUB:
		return c.handleGithubEvent(resp)
	case protos.VCEvent_GITLAB:
		return errors.New("VCEvent_GITLAB is not implemented yet")
	case protos.VCEvent_BITBUCKET:
		return errors.New("VCEvent_BITBUCKET is not implemented yet")
	}

	return fmt.Errorf("unknown VCEvent type '%s'", resp.Type)
}

// handleAuthResponse handles a AUTH_RESPONSE payload from a VCServiceServer protobuf message
func (c *Client) handleAuthResponse(event *protos.VCEvent) error {
	authResponse := event.GetAuthResponse()
	if authResponse == nil {
		if err := c.Conn.Close(); err != nil {
			c.log.Error("could not cleanly disconnect from server")
		}
		return errors.New("Received invalid authentication response from server")
	}

	if authResponse.Authorized == false {
		if err := c.Conn.Close(); err != nil {
			c.log.Error("could not cleanly disconnect from server")
		}

		err := fmt.Errorf("could not authenticate: %s", authResponse.Message)
		c.log.Error(err)
		return err
	}

	c.log.Info("Connection authorized. Waiting for vc events...")

	return nil
}

func (c *Client) handleGithubEvent(event *protos.VCEvent) error {
	payload := event.GetGithubEvent()
	switch payload.GetType() {
	case protos.GithubEvent_INSTALL_CREATED:
		// TODO
		return errors.New("unimplemented")
	case protos.GithubEvent_INSTALL_UPDATED:
		// TODO
		return errors.New("unimplemented")
	case protos.GithubEvent_INSTALL_DELETED:
		// TODO
		return errors.New("unimplemented")
	case protos.GithubEvent_PULL_CREATED:
		// TODO
		return errors.New("unimplemented")
	case protos.GithubEvent_PULL_MERGED:
		// TODO
		return errors.New("unimplemented")
	}

	return fmt.Errorf("unknown GithHub event type '%s'", payload.GetType())
}

func (c *Client) handleNewJWTToken(payload *protos.NewJwtToken) error {

	// Store new token in config and persist update to Etcd
	c.PersistentConfig.VCServiceToken = payload.GetToken()
	c.EtcdService.SaveConfig(context.Background(), c.PersistentConfig)

	// Notify all plumbers of new token
	msg := &etcd.MessageUpdateConfig{
		VCServiceToken: payload.GetToken(),
	}
	if err := c.EtcdService.PublishConfigUpdate(context.Background(), msg); err != nil {
		c.log.Errorf("unable to broadcast new JWT to all plumber instances")
		// Do not fail, other instances will still be able to use existing token for a while
	}

	// Cancel receive loop in Start()
	c.CancelFunc()

	// Fresh context
	c.Context, c.CancelFunc = context.WithCancel(context.Background())

	// Reconnect with new token
	if err := c.reconnect(); err != nil {
		return errors.Wrap(err, "unable to handle new JWT token event")
	}

	// Rerun receive loop
	c.Start()

	return nil
}

// AttachStream adds a vc-service event listener to the AttachedStreams map which will receive a copy
// of all VCEvent messages
func (c *Client) AttachStream(id string) *AttachedStream {
	s := &AttachedStream{EventsCh: make(chan *protos.VCEvent)}

	c.AttachedStreamsMutex.Lock()
	c.AttachedStreams[id] = s
	c.AttachedStreamsMutex.Unlock()

	return s
}

// DetachStream removes a vc-service event listener from AttachedStreams map
func (c *Client) DetachStream(id string) {
	c.AttachedStreamsMutex.Lock()
	defer c.AttachedStreamsMutex.Unlock()
	delete(c.AttachedStreams, id)
}
