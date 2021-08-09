package cli

import (
	"context"

	"github.com/batchcorp/plumber/options"
)

type CLI struct {
	cfg
}

func New(opts *options.Options) (*CLI, error) {
	// Validate options

	// Connect to the destination

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
