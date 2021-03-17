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
	sinkRecords, relayMessages, err := r.convertMessagesToSQSSinkRecords(messages)
	if err != nil {
		return fmt.Errorf("unable to convert messages to sqs sink records: %s", err)
	}

	client := services.NewGRPCCollectorClient(conn)

	r.CallWithRetry(ctx, "AddSQSRecord", func(ctx context.Context) error {
		_, err := client.AddSQSRecord(ctx, &services.SQSRecordRequest{
			Records: sinkRecords,
		})
		return err
	})

	if err != nil {
		return err
	}

	// Optionally delete message from AWS SQS
	for _, rm := range relayMessages {
		if !rm.Options.AutoDelete {
			continue
		}

		// Auto-delete is turned on
		if _, err := rm.Options.Service.DeleteMessage(&sqs.DeleteMessageInput{
			QueueUrl:      aws.String(rm.Options.QueueURL),
			ReceiptHandle: rm.Value.ReceiptHandle,
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

func (r *Relay) convertMessagesToSQSSinkRecords(messages []interface{}) ([]*records.SQSRecord, []*types.RelayMessage, error) {
	sinkRecords := make([]*records.SQSRecord, 0)
	relayMessages := make([]*types.RelayMessage, 0)

	for i, v := range messages {
		relayMessage, ok := v.(*types.RelayMessage)
		if !ok {
			return nil, nil, fmt.Errorf("unable to type assert incoming message as RelayMessage (index: %d)", i)
		}

		relayMessages = append(relayMessages, relayMessage)

		if err := r.validateSQSRelayMessage(relayMessage); err != nil {
			return nil, nil, fmt.Errorf("unable to validate sqs relay message (index: %d): %s", i, err)
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

	return sinkRecords, relayMessages, nil
}
