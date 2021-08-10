package activemq

import (
	"context"

	"github.com/batchcorp/plumber/types"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/writer"
)

func (a *ActiveMq) Write(ctx context.Context, opts *options.Options) error {
	if !a.connected {
		return types.BackendNotConnectedErr
	}

	return nil
}

func Write(opts *options.Options, md *desc.MessageDescriptor) error {
	if err := writer.ValidateWriteOptions(opts, nil); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	writeValues, err := writer.GenerateWriteValues(md, opts)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	client, err := NewClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create client")
	}

	defer client.Disconnect()

	a := &ActiveMq{
		Options: opts,
		msgDesc: md,
		client:  client,
		log:     logrus.WithField("pkg", "activemq/write.go"),
	}

	for _, value := range writeValues {
		if err := a.write(value); err != nil {
			a.log.Error(err)
		}
	}

	return nil
}

// Write writes a message to an ActiveMQ topic
func (a *ActiveMq) write(value []byte) error {
	if err := a.client.Send(a.getDestination(), "", value, nil); err != nil {
		a.log.Infof("Unable to write message to '%s': %s", a.getDestination(), err)
		return errors.Wrap(err, "unable to write message")
	}

	a.log.Infof("Successfully wrote message to '%s'", a.getDestination())

	return nil
}
