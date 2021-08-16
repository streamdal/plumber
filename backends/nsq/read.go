package nsq

import (
	"context"
	"sync"
	"time"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/types"
	"github.com/nsqio/go-nsq"
	"github.com/pkg/errors"
)

// Read will attempt to consume one or more messages from a given topic,
// optionally decode it and/or convert the returned output.
func (n *NSQ) Read(ctx context.Context, resultsChan chan *types.ReadMessage, errorChan chan *types.ErrorMessage) error {
	if err := validateReadOptions(n.Options); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	count := 1

	n.consumer.AddHandler(nsq.HandlerFunc(func(msg *nsq.Message) error {
		resultsChan <- &types.ReadMessage{
			Value: msg.Body,
			Metadata: map[string]interface{}{
				"id":          msg.ID,
				"timestamp":   msg.Timestamp,
				"attempts":    msg.Attempts,
				"nsq_address": msg.NSQDAddress,
			},
			ReceivedAt: time.Now().UTC(),
			Raw:        msg,
		}

		if !n.Options.Read.Follow {
			wg.Done()
		}

		count++

		return nil
	}))

	n.log.Info("Waiting for messages...")

	wg.Wait()

	n.log.Debug("reader exiting")

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
