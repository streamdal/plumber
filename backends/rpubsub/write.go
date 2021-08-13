package rpubsub

import (
	"context"
	"fmt"
	"strings"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/pkg/errors"
)

// Write is the entry point function for performing write operations in RedisPubSub.
//
// This is where we verify that the passed args and flags combo makes sense,
// attempt to establish a connection, parse protobuf before finally attempting
// to perform the write.
func (r *Redis) Write(ctx context.Context, errorCh chan *types.ErrorMessage, messages ...*types.WriteMessage) error {
	if err := validateWriteOptions(r.Options); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	for _, msg := range messages {
		if err := r.write(msg.Value); err != nil {
			util.WriteError(r.log, errorCh, err)
		}
	}

	return nil
}

func (r *Redis) write(value []byte) error {
	errs := make([]string, 0)

	for _, ch := range r.Options.RedisPubSub.Channels {
		err := r.client.Publish(context.Background(), ch, value).Err()
		if err != nil {
			errs = append(errs, fmt.Sprintf("failed to publish message to channel '%s': %s", ch, err))
			continue
		}

		r.log.Debugf("Successfully wrote message to '%s'", ch)
	}

	if len(errs) != 0 {
		return errors.New(strings.Join(errs, "; "))
	}

	return nil
}

func validateWriteOptions(opts *options.Options) error {
	return nil
}
