package cli

import (
	"context"

	"github.com/batchcorp/plumber/options"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/server/v3/mvcc/backend"
)

type CLI struct {
	opts *options.Options
}

func New(opts *options.Options) (*CLI, error) {
	// Validate options
	if err := validateOptions(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	// Connect to the destination
	b, err := backend.New(opts.Backend)

	return nil, nil
}

func (c *CLI) Read(ctx context.Context) error {
	return nil
}

func (c *CLI) Write(ctx context.Context) error {
	return nil
}

func (c *CLI) GetLag(ctx context.Context) error {
	return nil
}
