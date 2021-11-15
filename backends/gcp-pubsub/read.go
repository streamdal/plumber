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

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
)

func Read(opts *cli.Options, md *desc.MessageDescriptor) error {
	if err := validateReadOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	client, err := NewClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create client")
	}

	r := &GCPPubSub{
		Options: opts,
		MsgDesc: md,
		Client:  client,
		log:     logrus.WithField("pkg", "gcp-pubsub/read.go"),
	}

	return r.Read()
}

func (g *GCPPubSub) Read() error {
	defer g.Client.Close()

	g.log.Info("Listening for message(s) ...")

	var wg sync.WaitGroup

	// Receive launches several goroutines to exec func, need to use a mutex
	var m sync.Mutex

	var count int

	for _, subID := range g.Options.GCPPubSub.ReadSubscriptionId {
		wg.Add(1)

		go func(id string) {
			defer wg.Done()
			sub := g.Client.Subscription(id)

			// Standard way to cancel Receive in gcp's pubsub
			cctx, cancel := context.WithCancel(context.Background())

			err := sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
				count++
				m.Lock()
				defer m.Unlock()

				if g.Options.GCPPubSub.ReadAck {
					defer msg.Ack()
				}

				data, err := reader.Decode(g.Options, g.MsgDesc, msg.Data)
				if err != nil {
					return
				}

				str := string(data)

				str = fmt.Sprintf("%d: ", count) + str

				printer.Print(str)

				if !g.Options.ReadFollow {
					cancel()
					return
				}
			})

			if err != nil {
				g.log.Errorf("unable to complete msg receive: %s", err)
			}

		}(subID)
	}

	wg.Wait()

	g.log.Debug("Reader exiting")

	return nil
}

func validateReadOptions(opts *cli.Options) error {
	emulator := os.Getenv("PUBSUB_EMULATOR_HOST")
	appCreds := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if emulator == "" && appCreds == "" {
		return errors.New("GOOGLE_APPLICATION_CREDENTIALS or PUBSUB_EMULATOR_HOST must be set")
	}

	return nil
}
