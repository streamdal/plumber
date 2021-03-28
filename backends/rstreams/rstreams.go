package rstreams

import (
	"context"

	"github.com/go-redis/redis/v8"
	"github.com/jhump/protoreflect/desc"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
)

type RedisStreams struct {
	Options *cli.Options
	Client  *redis.Client
	MsgDesc *desc.MessageDescriptor
	Context context.Context
	log     *logrus.Entry
}

func NewClient(opts *cli.Options) (*redis.Client, error) {
	return redis.NewClient(&redis.Options{
		Addr:     opts.Redis.Address,
		Username: opts.Redis.Username,
		Password: opts.Redis.Password,
		DB:       opts.Redis.Database,
	}), nil
}
