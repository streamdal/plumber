package queues_stream

import (
	"context"
	"crypto/x509"

	"fmt"

	pb "github.com/kubemq-io/protobuf/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

const (
	defaultMaxSendSize = 1024 * 1024 * 100 //100MB
	defaultMaxRcvSize  = 1024 * 1024 * 100 //100MB

)

type GrpcClient struct {
	opts *Options
	conn *grpc.ClientConn
	pb.KubemqClient
}

func NewGrpcClient(ctx context.Context, op ...Option) (*GrpcClient, error) {
	opts := GetDefaultOptions()
	for _, o := range op {
		o.apply(opts)
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	connOptions := []grpc.DialOption{grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(defaultMaxRcvSize), grpc.MaxCallSendMsgSize(defaultMaxSendSize))}
	if opts.isSecured {
		if opts.certFile != "" {
			creds, err := credentials.NewClientTLSFromFile(opts.certFile, opts.serverOverrideDomain)
			if err != nil {
				return nil, fmt.Errorf("could not load tls cert: %s", err)
			}
			connOptions = append(connOptions, grpc.WithTransportCredentials(creds))
		} else if opts.certData != "" {
			certPool := x509.NewCertPool()
			if !certPool.AppendCertsFromPEM([]byte(opts.certData)) {
				return nil, fmt.Errorf("credentials: failed to append certificates to pool")
			}
			creds := credentials.NewClientTLSFromCert(certPool, opts.serverOverrideDomain)

			connOptions = append(connOptions, grpc.WithTransportCredentials(creds))

		} else {
			return nil, fmt.Errorf("no valid tls security provided")
		}

	} else {
		connOptions = append(connOptions, grpc.WithInsecure())
	}
	address := fmt.Sprintf("%s:%d", opts.host, opts.port)
	g := &GrpcClient{
		opts:         opts,
		conn:         nil,
		KubemqClient: nil,
	}
	connOptions = append(connOptions, grpc.WithUnaryInterceptor(g.setUnaryInterceptor()), grpc.WithStreamInterceptor(g.setStreamInterceptor()))

	var err error
	g.conn, err = grpc.DialContext(ctx, address, connOptions...)
	if err != nil {
		return nil, err
	}
	go func() {

		<-ctx.Done()
		if g.conn != nil {
			_ = g.conn.Close()
		}

	}()
	g.KubemqClient = pb.NewKubemqClient(g.conn)

	if g.opts.checkConnection {
		_, err := g.Ping(ctx)
		if err != nil {
			_ = g.Close()
			return nil, err
		}
		return g, nil
	} else {
		return g, nil
	}

}
func (g *GrpcClient) GlobalClientId() string {
	return g.opts.clientId
}
func (g *GrpcClient) setUnaryInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if g.opts.authToken != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, kubeMQAuthTokenHeader, g.opts.authToken)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func (g *GrpcClient) setStreamInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if g.opts.authToken != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, kubeMQAuthTokenHeader, g.opts.authToken)
		}
		return streamer(ctx, desc, cc, method, opts...)
	}
}
func (g *GrpcClient) Ping(ctx context.Context) (*pb.PingResult, error) {
	return g.KubemqClient.Ping(ctx, &pb.Empty{})
}

func (g *GrpcClient) Close() error {
	err := g.conn.Close()
	if err != nil {
		return err
	}
	return nil
}
