package rpubsub

import (
	"context"
	"fmt"

	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
)

func Read(opts *options.Options, md *desc.MessageDescriptor) error {
	client, err := NewClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create client")
	}

	r := &Redis{
		Options: opts,
		client:  client,
		msgDesc: md,
		log:     logrus.WithField("pkg", "redis/read.go"),
	}

	return r.Read()
}

func (r *Redis) Read() error {
	defer r.client.Close()

	ctx := context.Background()

	ps := r.client.Subscribe(ctx, r.Options.RedisPubSub.Channels...)
	defer ps.Unsubscribe(ctx)

	r.log.Info("Listening for message(s) ...")

	count := 1

	for {
		msg, err := ps.ReceiveMessage(ctx)
		if err != nil {
			r.log.Error(err)
			return err
		}

		data, err := reader.Decode(r.Options, r.msgDesc, []byte(msg.Payload))
		if err != nil {
			r.log.Error(err)
			return err
		}

		str := string(data)

		str = fmt.Sprintf("%d: ", count) + str
		count++

		printer.Print(str)

		if !r.Options.ReadFollow {
			return nil
		}
	}
}
