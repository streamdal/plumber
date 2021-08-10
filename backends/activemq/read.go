package activemq

import (
	"context"
	"fmt"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
	"github.com/batchcorp/plumber/types"
	"github.com/go-stomp/stomp"
	"github.com/pkg/errors"
)

func (a *ActiveMq) Read(ctx context.Context, opts *options.Options) ([]byte, error) {
	if a.client == nil {
		return nil, types.BackendNotConnectedErr
	}

	if err := validateReadOptions(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate read options")
	}

	return nil, nil
}

func (a *ActiveMq) read() error {
	a.log.Info("Listening for message(s) ...")

	count := 1

	sub, err := a.client.Subscribe(a.getDestination(), stomp.AckClient)
	if err != nil {
		return errors.Wrap(err, "unable to create subscription")
	}

	defer sub.Unsubscribe()

	for msg := range sub.C {
		data, err := reader.Decode(a.Options, msg.Body)
		if err != nil {
			return err
		}

		str := string(data)

		str = fmt.Sprintf("%d: ", count) + str
		count++

		printer.Print(str)

		a.client.Ack(msg)

		if !a.Options.Read.Follow {
			return nil
		}
	}

	a.log.Debug("reader exiting")
	return nil
}

func validateReadOptions(opts *options.Options) error {
	if opts.ActiveMq.Topic != "" && opts.ActiveMq.Queue != "" {
		return errors.New("you may only specify a \"topic\" or a \"queue\" not both")
	}

	return nil
}
