// Package kv uses 'natty' under the covers; this pkg exists separate from the
// 'bus' package in order to simplify instantiation of the persistent config
// package. Persistent config only needs KV functionality and does not use any
// of the consumer parts of the 'bus'.
package kv

import (
	"context"

	"github.com/batchcorp/natty"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
)

type IKV interface {
	Put(ctx context.Context, bucket, key string, value []byte) error
	Get(ctx context.Context, bucket, key string) ([]byte, error)
	Delete(ctx context.Context, bucket, key string) error
}

type KV struct {
	client  *natty.Natty
	options *opts.ServerOptions
}

func New(serverOptions *opts.ServerOptions) (*KV, error) {
	if err := validateServerOptions(serverOptions); err != nil {
		return nil, errors.Wrap(err, "unable to validate server options")
	}

	// Setup natty
	n, err := natty.New(&natty.Config{
		NoConsumer:        true,
		NatsURL:           serverOptions.NatsUrl,
		Logger:            logrus.WithField("pkg", "kv"),
		UseTLS:            serverOptions.UseTls,
		TLSCACertFile:     serverOptions.TlsCaFile,
		TLSClientCertFile: serverOptions.TlsCertFile,
		TLSClientKeyFile:  serverOptions.TlsKeyFile,
		TLSSkipVerify:     serverOptions.TlsSkipVerify,
	})
	if err != nil {
		return nil, errors.Wrap(err, "unable to setup natty")
	}

	return &KV{
		client:  n,
		options: serverOptions,
	}, nil
}

func (k *KV) Put(ctx context.Context, bucket, key string, value []byte) error {
	return k.client.Put(ctx, bucket, key, value)
}

func (k *KV) Get(ctx context.Context, bucket, key string) ([]byte, error) {
	return k.client.Get(ctx, bucket, key)
}

func (k *KV) Delete(ctx context.Context, bucket, key string) error {
	return k.client.Delete(ctx, bucket, key)
}

func validateServerOptions(cfg *opts.ServerOptions) error {
	if cfg == nil {
		return errors.New("server options is nil")
	}

	if len(cfg.NatsUrl) == 0 {
		return errors.New("nats url cannot be empty")
	}

	return nil
}
