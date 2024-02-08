package rpubsub

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

const BackendName = "redis-pubsub"

var (
	ErrMissingChannel  = errors.New("you must specify at least one channel")
	ErrMissingPassword = errors.New("missing password (either use only password or fill out both)")
)

type RedisPubsub struct {
	connOpts *opts.ConnectionOptions
	connArgs *args.RedisPubSubConn
	client   *redis.Client
	log      *logrus.Entry
}

func New(connOpts *opts.ConnectionOptions) (*RedisPubsub, error) {
	if err := validateBaseConnOpts(connOpts); err != nil {
		return nil, errors.Wrap(err, "unable to validate connection options")
	}

	args := connOpts.GetRedisPubsub()

	client := redis.NewClient(&redis.Options{
		Addr:     args.Address,
		Username: args.Username,
		Password: args.Password,
		DB:       int(args.Database),
	})

	return &RedisPubsub{
		connOpts: connOpts,
		connArgs: args,
		client:   client,
		log:      logrus.WithField("backend", BackendName),
	}, nil
}

func (r *RedisPubsub) Name() string {
	return BackendName
}

func (r *RedisPubsub) Close(_ context.Context) error {
	r.client.Close()
	return nil
}

func (r *RedisPubsub) Test(_ context.Context) error {
	return types.NotImplementedErr
}

func validateBaseConnOpts(opts *opts.ConnectionOptions) error {
	if opts == nil {
		return validate.ErrMissingConnOpts
	}

	if opts.Conn == nil {
		return validate.ErrMissingConnCfg
	}

	redisOpts := opts.GetRedisPubsub()
	if redisOpts == nil {
		return validate.ErrMissingConnArgs
	}

	if redisOpts.Username != "" && redisOpts.Password == "" {
		return ErrMissingPassword
	}

	return nil
}
