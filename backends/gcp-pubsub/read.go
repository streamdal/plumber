package gcppubsub

import (
	"context"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/pkg/errors"
)

func (g *GCPPubSub) Read(ctx context.Context, resultsChan chan *types.ReadMessage, errorChan chan *types.ErrorMessage) error {
	if err := validateReadOptions(g.Options); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	g.log.Info("Listening for message(s) ...")

	sub := g.client.Subscription(g.Options.GCPPubSub.ReadSubscriptionId)

	// Receive launches several goroutines to exec func, need to use a mutex
	var m sync.Mutex

	count := 1

	// Standard way to cancel Receive in gcp's pubsub
	cctx, cancel := context.WithCancel(ctx)

	if err := sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		m.Lock()
		defer m.Unlock()

		if g.Options.GCPPubSub.ReadAck {
			defer msg.Ack()
		}

		resultsChan <- &types.ReadMessage{
			Value:      msg.Data,
			ReceivedAt: time.Now().UTC(),
			Num:        count,
		}

		count++

		if !g.Options.Read.Follow {
			cancel()
			return
		}
	}); err != nil {
		util.WriteError(g.log, errorChan, errors.Wrap(err, "unable to complete msg receive"))
		return errors.Wrap(err, "unable to complete msg receive")
	}

	g.log.Debug("reader exiting")

	return nil
}

func validateReadOptions(_ *options.Options) error {
	emulator := os.Getenv("PUBSUB_EMULATOR_HOST")
	appCreds := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")

	if emulator == "" && appCreds == "" {
		return errors.New("GOOGLE_APPLICATION_CREDENTIALS or PUBSUB_EMULATOR_HOST must be set")
	}

	return nil
}
