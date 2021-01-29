package relay

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/batchcorp/schemas/build/go/events/records"
	"github.com/batchcorp/schemas/build/go/services"
	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/batchcorp/plumber/backends/aws-sqs/types"
)

func (r *Relay) handleSQS(ctx context.Context, conn *grpc.ClientConn, messages []interface{}) error {
	sinkRecords, err := r.convertMessagesToSQSSinkRecords(messages)
	if err != nil {
		return fmt.Errorf("unable to convert messages to sqs sink records: %s", err)
	}

	client := services.NewGRPCCollectorClient(conn)

	if _, err := client.AddSQSRecord(ctx, &services.SQSRecordRequest{
		Records: sinkRecords,
	}); err != nil {
		return errors.Wrap(err, "unable to complete AddSQSRecord call")
	}

	// Optionally delete message from AWS SQS
	if msg.Options.AutoDelete {
		if _, err := msg.Options.Service.DeleteMessage(&sqs.DeleteMessageInput{
			QueueUrl:      aws.String(msg.Options.QueueURL),
			ReceiptHandle: msg.Value.ReceiptHandle,
		}); err != nil {
			return errors.Wrap(err, "unable to delete message upon completion")
		}
	}

	return nil
}

func (r *Relay) validateSQSRelayMessage(msg *types.RelayMessage) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	if msg.Value == nil {
		return errors.New("msg.Value cannot be nil")
	}

	if msg.Options == nil {
		return errors.New("msg.Options cannot be nil")
	}

	if msg.Options.Service == nil {
		return errors.New("msg.Options.Service cannot be nil")
	}

	if msg.Options.QueueURL == "" {
		return errors.New("msg.Options.QueueURL cannot be empty")
	}

	return nil
}

func (r *Relay) convertMessagesToSQSSinkRecords(messages []interface{}) ([]*records.SQSRecord, error) {
	sinkRecords := make([]*records.SQSRecord, 0)

	for i, v := range messages {
		relayMessage, ok := v.(*types.RelayMessage)
		if !ok {
			return nil, fmt.Errorf("unable to type assert incoming message as RelayMessage (index: %d)", i)
		}

		if err := r.validateSQSRelayMessage(relayMessage); err != nil {
			return nil, fmt.Errorf("unable to validate sqs relay message (index: %d): %s", i, err)
		}

		sqsRecord := &records.SQSRecord{
			Attributes:        make(map[string]string, 0),
			Messageattributes: make(map[string]*records.SQSRecordMessageAttribute, 0),
			Messageid:         *relayMessage.Value.MessageId,
			Receipt:           *relayMessage.Value.ReceiptHandle,
			Body:              []byte(*relayMessage.Value.Body),
			Timestamp:         time.Now().UTC().UnixNano(),
		}

		for k, v := range relayMessage.Value.Attributes {
			sqsRecord.Attributes[k] = *v
		}

		for k, v := range relayMessage.Value.MessageAttributes {
			sqsRecord.Messageattributes[k] = &records.SQSRecordMessageAttribute{
				Datatype:    *v.DataType,
				Stringvalue: *v.StringValue,
				Binaryvalue: v.BinaryValue,
			}
		}

		sinkRecords = append(sinkRecords, sqsRecord)
	}

	return sinkRecords, nil
}
