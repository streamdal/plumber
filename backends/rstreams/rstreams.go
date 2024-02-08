package rstreams

import (
	"context"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/streamdal/plumber/types"
	"github.com/streamdal/plumber/validate"
)

const BackendName = "redis-streams"

var (
	ErrMissingStream   = errors.New("you must specify at least one stream")
	ErrMissingPassword = errors.New("missing password (either use only password or fill out both)")
)

type RedisStreams struct {
	connOpts *opts.ConnectionOptions
	connArgs *args.RedisStreamsConn
	client   *redis.Client
	log      *logrus.Entry
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
		return validate.ErrMissingConnOpts
	}

	if opts.Conn == nil {
		return validate.ErrMissingConnCfg
	}

	redisOpts := opts.GetRedisStreams()
	if redisOpts == nil {
		return validate.ErrMissingConnArgs
	}

	if redisOpts.Username != "" && redisOpts.Password == "" {
		return ErrMissingPassword
	}

	return nil
}
