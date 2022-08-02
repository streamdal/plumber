package nsq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

func (n *NSQ) Read(ctx context.Context, readOpts *opts.ReadOptions, resultsChan chan *records.ReadRecord, errorChan chan *records.ErrorRecord) error {
	if err := validateReadOptions(readOpts); err != nil {
		return errors.Wrap(err, "unable to validate read config")
	}

	consumer, err := nsq.NewConsumer(readOpts.Nsq.Args.Topic, readOpts.Nsq.Args.Channel, n.config)
	if err != nil {
		return errors.Wrap(err, "Could not start NSQ consumer")
	}

	consumer.SetLogger(n.log, nsq.LogLevelError)

	doneCh := make(chan struct{})
	defer close(doneCh)

	var count int64

	consumer.AddHandler(nsq.HandlerFunc(func(msg *nsq.Message) error {
		count++

		serializedMsg, err := json.Marshal(msg)
		if err != nil {
			return errors.Wrap(err, "unable to serialize NSQ msg to JSON")
		}

		resultsChan <- &records.ReadRecord{
			MessageId:           uuid.NewV4().String(),
			Num:                 count,
			Metadata:            nil,
			ReceivedAtUnixTsUtc: time.Now().UTC().Unix(),
			Payload:             msg.Body,
			XRaw:                serializedMsg,
			Record: &records.ReadRecord_Nsq{
				Nsq: &records.NSQ{
					Id:          fmt.Sprintf("%s", msg.ID),
					Topic:       readOpts.Nsq.Args.Topic,
					Channel:     readOpts.Nsq.Args.Channel,
					Attempts:    int32(msg.Attempts),
					NsqdAddress: msg.NSQDAddress,
					Value:       msg.Body,
					Timestamp:   msg.Timestamp,
				},
			},
		}

		if !readOpts.Continuous {
			doneCh <- struct{}{}
		}

		return nil
	}))

	// Connect to correct server. Reading can be done directly from an NSQD server
	// or let lookupd find the correct one.
	if n.connOpts.GetNsq().LookupdAddress != "" {
		if err := consumer.ConnectToNSQLookupd(n.connOpts.GetNsq().LookupdAddress); err != nil {
			return errors.Wrap(err, "could not connect to nsqlookupd")
		}
	} else {
		if err := consumer.ConnectToNSQD(n.connOpts.GetNsq().NsqdAddress); err != nil {
			return errors.Wrap(err, "could not connect to nsqd")
		}
	}

	defer consumer.Stop()

	n.log.Infof("Waiting for messages...")

	select {
	case <-doneCh:
		return nil
	case <-ctx.Done():
		return nil
	}

	return nil
}

func validateReadOptions(readOpts *opts.ReadOptions) error {
	if readOpts.Nsq == nil {
		return errors.New("NSQ read options cannot be nil")
	}

	if readOpts.Nsq.Args == nil {
		return errors.New("NSQ read option args cannot be nil")
	}

	return nil
}
