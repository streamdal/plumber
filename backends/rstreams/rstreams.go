package rstreams

import (
	"context"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/types"
)

const BackendName = "redis-streams"

type RedisStreams struct {
	// Base connection options / non-backend-specific options
	connOpts *opts.ConnectionOptions

	// Backend-specific args
	connArgs *args.RedisStreamsConn

	client *redis.Client
	log    *logrus.Entry
}

func New(opts *opts.ConnectionOptions) (*RedisStreams, error) {
	if err := validateBaseConnOpts(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate connection options")
	}

	args := opts.GetRedisStreams()

	client := redis.NewClient(&redis.Options{
		Addr:     args.Address,
		Username: args.Username,
		Password: args.Password,
		DB:       int(args.Database),
	})

	return &RedisStreams{
		connOpts: opts,
		connArgs: args,
		client:   client,
		log:      logrus.WithField("backend", BackendName),
	}, nil
}

func (r *RedisStreams) Name() string {
	return BackendName
}

func (r *RedisStreams) Close(_ context.Context) error {
	r.client.Close()
	return nil
}

func (r *RedisStreams) Test(_ context.Context) error {
	return types.NotImplementedErr
}

func validateBaseConnOpts(opts *opts.ConnectionOptions) error {
	if opts == nil {
		return errors.New("connection config cannot be nil")
	}

	if opts.Conn == nil {
		return errors.New("connection object in connection config cannot be nil")
	}

	redisOpts := opts.GetRedisStreams()
	if redisOpts == nil {
		return errors.New("connection config args cannot be nil")
	}

	if redisOpts.Username != "" && redisOpts.Password == "" {
		return errors.New("missing password (either use only password or fill out both)")
	}

	return nil
}
