package rpubsub

import (
	"github.com/go-redis/redis/v8"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
)

type Redis struct {
	Options *options.Options

	client  *redis.Client
	msgDesc *desc.MessageDescriptor
	log     *logrus.Entry
}

func New(opts *options.Options) (*Redis, error) {
	if err := validateOpts(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	return &Redis{
		Options: opts,
		log:     logrus.WithField("backend", "rpubsub"),
	}, nil
}

// TODO: Implement
func validateOpts(opts *options.Options) error {
	return nil
}

func NewClient(opts *options.Options) (*redis.Client, error) {
	return redis.NewClient(&redis.Options{
		Addr:     opts.RedisPubSub.Address,
		Username: opts.RedisPubSub.Username,
		Password: opts.RedisPubSub.Password,
		DB:       opts.RedisPubSub.Database,
	}), nil
}
