// Package server is a wrapper for the Client gRPC API.
// It provides a simple interface to interact with and mock.
package server

import (
	"context"
	"strings"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"

	"github.com/streamdal/streamdal/libs/protos/build/go/protos"

	"github.com/streamdal/streamdal/sdks/go/types"
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 . IServerClient
type IServerClient interface {
	// GetSetPipelinesCommandByService is called in New() in order to get all AttachCommands in a synchronous manner
	// before we allow the client to start processing.
	GetSetPipelinesCommandByService(ctx context.Context, service string) (*protos.GetSetPipelinesCommandsByServiceResponse, error)

	// GetTailStream returns a gRPC client stream used to send TailResponses to the streamdal server
	GetTailStream(ctx context.Context) (protos.Internal_SendTailClient, error)

	// HeartBeat sends a heartbeat to the streamdal server
	HeartBeat(ctx context.Context, req *protos.HeartbeatRequest) error

	// NewAudience announces a new audience to the streamdal server
	NewAudience(ctx context.Context, aud *protos.Audience, sessionID string) error

	// Notify calls to streamdal server to trigger the configured notification rules for the specified step
	Notify(ctx context.Context, pipeline *protos.Pipeline, step *protos.PipelineStep, aud *protos.Audience, payload []byte, conditionType protos.NotifyRequest_ConditionType) error

	// Reconnect closes any open gRPC connection to the streamdal server and re-establishes a new connection
	// This method won't perform retries as that should be determined by the caller
	Reconnect() error

	// Register registers a new client with the streamdal server.
	// This is ran in a goroutine and constantly listens for commands from the streamdal server
	// such as AttachPipeline, DetachPipeline, etc
	Register(ctx context.Context, req *protos.RegisterRequest) (protos.Internal_RegisterClient, error)

	// SendMetrics ships counter(s) to the server
	SendMetrics(ctx context.Context, counters []*types.CounterEntry) error

	// SendSchema sends a schema to the streamdal server
	SendSchema(ctx context.Context, aud *protos.Audience, jsonSchema []byte) error
}

const (
	maxGRPCMessageRecvSize = 100 * 1024 * 1024 // 100MB
	dialTimeout            = time.Second * 5
)

type Client struct {
	ServerAddr string
	Token      string
	Conn       *grpc.ClientConn
	Server     protos.InternalClient
}

// New dials a streamdal GRPC server and returns IServerClient
func New(serverAddr, serverToken string) (*Client, error) {
	conn, err := dialServer(serverAddr)
	if err != nil {
		return nil, errors.Wrap(err, "unable to dial streamdal server")
	}

	return &Client{
		Conn:       conn,
		Server:     protos.NewInternalClient(conn),
		Token:      serverToken,
		ServerAddr: serverAddr,
	}, nil
}

func dialServer(serverAddr string) (*grpc.ClientConn, error) {
	dialCtx, dialCancel := context.WithTimeout(context.Background(), dialTimeout)
	defer dialCancel()

	opts := make([]grpc.DialOption, 0)
	opts = append(opts,
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			// Enable keepalive; ping every 10s
			Time: 10 * time.Second,

			// Close stream if there is still no activity after 30s.
			//
			// NOTE: there should ALWAYS be activity on the streams because the
			// server sends periodic heartbeats to the client AND the client
			// sends periodic heartbeats to the server. If stream is disconnected,
			// client will auto re-establish connectivity with the server.
			Timeout: 30 * time.Second,
		}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxGRPCMessageRecvSize)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	conn, err := grpc.DialContext(dialCtx, serverAddr, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "could not dial GRPC server: %s")
	}

	return conn, nil
}

func (c *Client) Reconnect() error {
	// Don't care about any error here, the connection might be open or closed
	_ = c.Conn.Close()

	conn, err := dialServer(c.ServerAddr)
	if err != nil {
		return errors.Wrap(err, "unable to reconnect to streamdal server")
	}

	c.Conn = conn
	c.Server = protos.NewInternalClient(conn)

	return nil
}

func (c *Client) Notify(
	ctx context.Context,
	pipeline *protos.Pipeline,
	step *protos.PipelineStep,
	aud *protos.Audience,
	payload []byte,
	conditionType protos.NotifyRequest_ConditionType,
) error {
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("auth-token", c.Token))

	req := &protos.NotifyRequest{
		PipelineId:          pipeline.Id,
		Audience:            aud,
		OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
		ConditionType:       conditionType,
		Step:                step,
		Payload:             payload,
	}

	if _, err := c.Server.Notify(ctx, req); err != nil {
		return errors.Wrap(err, "unable to send rule notification")
	}

	return nil
}

func (c *Client) SendMetrics(ctx context.Context, counters []*types.CounterEntry) error {
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("auth-token", c.Token))

	req := &protos.MetricsRequest{
		Metrics: make([]*protos.Metric, 0),
	}

	for _, counter := range counters {
		labels := make(map[string]string)
		for k, v := range counter.Labels {
			labels[k] = v
		}

		req.Metrics = append(req.Metrics, &protos.Metric{
			Name:     string(counter.Name),
			Audience: counter.Audience,
			Value:    float64(counter.Value),
			Labels:   labels,
		})
	}

	if _, err := c.Server.Metrics(ctx, req); err != nil {
		return errors.Wrap(err, "unable to send metrics")
	}

	return nil
}

func (c *Client) Register(ctx context.Context, req *protos.RegisterRequest) (protos.Internal_RegisterClient, error) {
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("auth-token", c.Token))

	return c.Server.Register(ctx, req)
}

func (c *Client) NewAudience(ctx context.Context, aud *protos.Audience, sessionID string) error {
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("auth-token", c.Token))

	aud.ServiceName = strings.ToLower(aud.ServiceName)
	aud.ComponentName = strings.ToLower(aud.ComponentName)
	aud.OperationName = strings.ToLower(aud.OperationName)

	_, err := c.Server.NewAudience(ctx, &protos.NewAudienceRequest{
		Audience:  aud,
		SessionId: sessionID,
	})
	return err
}

func (c *Client) HeartBeat(ctx context.Context, req *protos.HeartbeatRequest) error {
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("auth-token", c.Token))

	_, err := c.Server.Heartbeat(ctx, req)
	return err
}

func (c *Client) GetSetPipelinesCommandByService(ctx context.Context, service string) (*protos.GetSetPipelinesCommandsByServiceResponse, error) {
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("auth-token", c.Token))

	resp, err := c.Server.GetSetPipelinesCommandsByService(ctx, &protos.GetSetPipelinesCommandsByServiceRequest{ServiceName: service})
	if err != nil {
		return nil, errors.Wrap(err, "unable to get set pipeline commands by service")
	}

	return resp, nil
}

func (c *Client) GetTailStream(ctx context.Context) (protos.Internal_SendTailClient, error) {
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("auth-token", c.Token))

	srv, err := c.Server.SendTail(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "unable to talk to streamdal server")
	}

	return srv, nil
}

func (c *Client) SendSchema(ctx context.Context, aud *protos.Audience, jsonSchema []byte) error {
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("auth-token", c.Token))

	req := &protos.SendSchemaRequest{
		Audience: aud,
		Schema: &protos.Schema{
			JsonSchema: jsonSchema,
		},
	}

	_, err := c.Server.SendSchema(ctx, req)
	if err != nil {
		return errors.Wrap(err, "unable to send schema")
	}

	return nil
}
