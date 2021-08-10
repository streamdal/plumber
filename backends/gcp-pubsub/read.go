package gcppubsub

import (
	"context"
	"fmt"
	"os"
	"sync"

	"cloud.google.com/go/pubsub"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
)

func Read(opts *options.Options, md *desc.MessageDescriptor) error {
	if err := validateReadOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	client, err := NewClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create client")
	}

	r := &GCPPubSub{
		Options: opts,
		msgDesc: md,
		client:  client,
		log:     logrus.WithField("pkg", "gcp-pubsub/read.go"),
	}

	return r.Read()
}

func (g *GCPPubSub) Read() error {
	defer g.client.Close()

	g.log.Info("Listening for message(s) ...")

	sub := g.client.Subscription(g.Options.GCPPubSub.ReadSubscriptionId)

	// Receive launches several goroutines to exec func, need to use a mutex
	var m sync.Mutex

	count := 1

	// Standard way to cancel Receive in gcp's pubsub
	cctx, cancel := context.WithCancel(context.Background())

	if err := sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		m.Lock()
		defer m.Unlock()

		if g.Options.GCPPubSub.ReadAck {
			defer msg.Ack()
		}

		data, err := reader.Decode(g.Options, g.msgDesc, msg.Data)
		if err != nil {
			return
		}

		str := string(data)

		str = fmt.Sprintf("%d: ", count) + str
		count++

		printer.Print(str)

		if !g.Options.ReadFollow {
			cancel()
			return
		}
	}); err != nil {
		return errors.Wrap(err, "unable to complete msg receive")
	}

	g.log.Debug("reader exiting")

	return nil
}

func validateReadOptions(opts *options.Options) error {
	emulator := os.Getenv("PUBSUB_EMULATOR_HOST")
	appCreds := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if emulator == "" && appCreds == "" {
		return errors.New("GOOGLE_APPLICATION_CREDENTIALS or PUBSUB_EMULATOR_HOST must be set")
	}

	return nil
}
