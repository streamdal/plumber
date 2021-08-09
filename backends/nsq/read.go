package nsq

import (
	"sync"

	"github.com/jhump/protoreflect/desc"
	"github.com/nsqio/go-nsq"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
)

// Read is the entry point function for performing read operations in NSQ
func Read(opts *options.Options, md *desc.MessageDescriptor) error {
	if err := validateReadOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	logger := &NSQLogger{}
	logger.Entry = logrus.WithField("pkg", "nsq")

	n := &NSQ{
		Options: opts,
		MsgDesc: md,
		log:     logger,
	}

	return n.Read()
}

// Read will attempt to consume one or more messages from a given topic,
// optionally decode it and/or convert the returned output.
func (n *NSQ) Read() error {
	config, err := getNSQConfig(n.Options)
	if err != nil {
		return errors.Wrap(err, "unable to create NSQ config")
	}

	consumer, err := nsq.NewConsumer(n.Options.NSQ.Topic, n.Options.NSQ.Channel, config)
	if err != nil {
		return errors.Wrap(err, "Could not start NSQ consumer")
	}

	logLevel := nsq.LogLevelError
	if n.Options.Debug {
		logLevel = nsq.LogLevelDebug
	}

	// Use logrus for NSQ logs
	consumer.SetLogger(n.log, logLevel)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	count := 1

	consumer.AddHandler(nsq.HandlerFunc(func(msg *nsq.Message) error {
		data, err := reader.Decode(n.Options, n.MsgDesc, msg.Body)
		if err != nil {
			return errors.Wrap(err, "unable to decode msg")
		}

		printer.PrintNSQResult(n.Options, count, msg, data)

		if !n.Options.ReadFollow {
			wg.Done()
		}
		count++
		return nil
	}))

	// Connect to correct server. Reading can be done directly from an NSQD server
	// or let lookupd find the correct one.
	if n.Options.NSQ.NSQLookupDAddress != "" {
		if err := consumer.ConnectToNSQLookupd(n.Options.NSQ.NSQLookupDAddress); err != nil {
			return errors.Wrap(err, "could not connect to nsqlookupd")
		}
	} else {
		if err := consumer.ConnectToNSQD(n.Options.NSQ.NSQDAddress); err != nil {
			return errors.Wrap(err, "could not connect to nsqd")
		}
	}
	defer consumer.Stop()

	n.log.Infof("Waiting for messages...")

	wg.Wait()

	return nil
}

// validateReadOptions ensures all necessary flags have values required for reading from NSQ
func validateReadOptions(opts *options.Options) error {
	if opts.NSQ.NSQDAddress == "" && opts.NSQ.NSQLookupDAddress == "" {
		return ErrMissingAddress
	}

	if opts.NSQ.NSQDAddress != "" && opts.NSQ.NSQLookupDAddress != "" {
		return ErrChooseAddress
	}

	if opts.NSQ.TLSCAFile != "" || opts.NSQ.TLSClientCertFile != "" || opts.NSQ.TLSClientKeyFile != "" {
		if opts.NSQ.TLSClientKeyFile == "" {
			return ErrMissingTLSKey
		}

		if opts.NSQ.TLSClientCertFile == "" {
			return ErrMissingTlsCert
		}

		if opts.NSQ.TLSCAFile == "" {
			return ErrMissingTLSCA
		}
	}

	return nil
}
