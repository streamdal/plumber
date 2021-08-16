package nats_streaming

import (
	"context"
	"time"

	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	pb2 "github.com/nats-io/stan.go/pb"

	"github.com/batchcorp/plumber/options"
	"github.com/nats-io/stan.go"
	"github.com/pkg/errors"
)

var (
	errMissingChannel    = errors.New("--channel name cannot be empty")
	errInvalidReadOption = errors.New("You may only specify one read option of --last, --all, --seq, --since")
)

func (n *NatsStreaming) Read(ctx context.Context, resultsChan chan *types.ReadMessage, errorChan chan *types.ErrorMessage) error {
	if err := validateReadOptions(n.Options); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	n.log.Info("Listening for message(s) ...")

	count := 1

	// stan.Subscribe is async, use channel to wait to exit
	doneCh := make(chan bool)
	defer close(doneCh)

	subFunc := func(msg *stan.Msg) {
		if n.Options.Read.Verbose {
			n.log.Infof("- %-24s%-6s", "Timestamp", time.Unix(0, msg.Timestamp).UTC().String())
			n.log.Infof("- %-24s%d", "Sequence No.", msg.Sequence)
			n.log.Infof("- %-24s%-6d", "CRC32", msg.CRC32)
			n.log.Infof("- %-24s%-6t", "Redelivered", msg.Redelivered)
			n.log.Infof("- %-24s%-6d", "Redelivery Count", msg.RedeliveryCount)
			n.log.Infof("- %-24s%-6s", "Subject", msg.Subject)
		}

		resultsChan <- &types.ReadMessage{
			Value: msg.Data,
			Metadata: map[string]interface{}{
				"subject":          msg.Subject,
				"reply":            msg.Reply,
				"crc32":            msg.CRC32,
				"timestamp":        msg.Timestamp,
				"redelivered":      msg.Redelivered,
				"redelivery_count": msg.RedeliveryCount,
			},
			ReceivedAt: time.Now().UTC(),
			Raw:        msg,
		}

		count++

		// All read options except --last-received will default to follow mode, otherwise we will cause a panic here
		if !n.Options.Read.Follow && n.Options.NatsStreaming.ReadLastReceived {
			doneCh <- true
		}
	}

	sub, err := n.stanClient.Subscribe(n.Options.NatsStreaming.Channel, subFunc, n.getReadOptions()...)
	if err != nil {
		return errors.Wrap(err, "unable to create subscription")
	}

	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			util.WriteError(n.log, errorChan, errors.Wrap(err, "error unsubscribing"))
		}
	}()

	<-doneCh

	return nil
}

// getReadOptions returns slice of options to pass to Stan.io. Only one read option of --last, --since, --seq, --new--only
// is allowed, so return early once we have a read option
func (n *NatsStreaming) getReadOptions() []stan.SubscriptionOption {
	opts := make([]stan.SubscriptionOption, 0)

	if n.Options.NatsStreaming.DurableSubscription != "" {
		opts = append(opts, stan.DurableName(n.Options.NatsStreaming.DurableSubscription))
	}

	if n.Options.NatsStreaming.AllAvailable {
		opts = append(opts, stan.DeliverAllAvailable())
		return opts
	}

	if n.Options.NatsStreaming.ReadLastReceived {
		opts = append(opts, stan.StartWithLastReceived())
		return opts
	}

	if n.Options.NatsStreaming.ReadSince > 0 {
		opts = append(opts, stan.StartAtTimeDelta(n.Options.NatsStreaming.ReadSince))
		return opts
	}

	if n.Options.NatsStreaming.ReadFromSequence > 0 {
		opts = append(opts, stan.StartAtSequence(n.Options.NatsStreaming.ReadFromSequence))
		return opts
	}

	// Default option is new-only
	opts = append(opts, stan.StartAt(pb2.StartPosition_NewOnly))

	return opts
}

// validateReadOptions ensures the correct CLI options are specified for the read action
func validateReadOptions(opts *options.Options) error {
	if opts.NatsStreaming.Channel == "" {
		return errMissingChannel
	}

	if opts.NatsStreaming.ReadFromSequence > 0 {
		if opts.NatsStreaming.AllAvailable {
			return errInvalidReadOption
		}

		if opts.NatsStreaming.ReadSince > 0 {
			return errInvalidReadOption
		}

		if opts.NatsStreaming.ReadLastReceived {
			return errInvalidReadOption
		}
	}

	if opts.NatsStreaming.AllAvailable {
		if opts.NatsStreaming.ReadFromSequence > 0 {
			return errInvalidReadOption
		}

		if opts.NatsStreaming.ReadSince > 0 {
			return errInvalidReadOption
		}

		if opts.NatsStreaming.ReadLastReceived {
			return errInvalidReadOption
		}
	}

	if opts.NatsStreaming.ReadSince > 0 {
		if opts.NatsStreaming.ReadFromSequence > 0 {
			return errInvalidReadOption
		}

		if opts.NatsStreaming.AllAvailable {
			return errInvalidReadOption
		}

		if opts.NatsStreaming.ReadLastReceived {
			return errInvalidReadOption
		}
	}

	if opts.NatsStreaming.ReadLastReceived {
		if opts.NatsStreaming.ReadFromSequence > 0 {
			return errInvalidReadOption
		}

		if opts.NatsStreaming.AllAvailable {
			return errInvalidReadOption
		}

		if opts.NatsStreaming.ReadSince > 0 {
			return errInvalidReadOption
		}
	}

	return nil
}
