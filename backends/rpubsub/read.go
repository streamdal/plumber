package rpubsub

import (
	"context"
	"fmt"

	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
)

func Read(opts *cli.Options) error {
	var mdErr error
	var md *desc.MessageDescriptor

	if opts.ReadProtobufRootMessage != "" {
		md, mdErr = pb.FindMessageDescriptor(opts.ReadProtobufDirs, opts.ReadProtobufRootMessage)
		if mdErr != nil {
			return errors.Wrap(mdErr, "unable to find root message descriptor")
		}
	}

	client, err := NewClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create client")
	}

	r := &Redis{
		Options: opts,
		Client:  client,
		MsgDesc: md,
		log:     logrus.WithField("pkg", "redis/read.go"),
	}

	return r.Read()
}

func (r *Redis) Read() error {
	defer r.Client.Close()

	ctx := context.Background()

	ps := r.Client.Subscribe(ctx, r.Options.RedisPubSub.Channels...)
	defer ps.Unsubscribe(ctx)

	r.log.Info("Listening for message(s) ...")

	lineNumber := 1

	for {
		msg, err := ps.ReceiveMessage(ctx)
		if err != nil {
			r.log.Error(err)
			return err
		}

		data, err := reader.Decode(r.Options, r.MsgDesc, []byte(msg.Payload))
		if err != nil {
			r.log.Error(err)
			return err
		}

		str := string(data)

		if r.Options.ReadLineNumbers {
			str = fmt.Sprintf("%d: ", lineNumber) + str
			lineNumber++
		}

		printer.Print(str)

		if !r.Options.ReadFollow {
			return nil
		}
	}
}
