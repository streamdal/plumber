package relay

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/batchcorp/schemas/build/go/events/records"
	"github.com/batchcorp/schemas/build/go/services"
	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/batchcorp/plumber/backends/aws-sqs/types"
)

func (r *Relay) handleSQS(ctx context.Context, conn *grpc.ClientConn, msg *types.RelayMessage) error {
	if err := r.validateSQSRelayMessage(msg); err != nil {
		return errors.Wrap(err, "unable to validate SQS relay message")
	}

	sqsRecord := convertSQSMessageToProtobufRecord(msg.Value)

	client := services.NewGRPCCollectorClient(conn)

	if _, err := client.AddSQSRecord(ctx, &services.SQSRecordRequest{
		Records: []*records.SQSRecord{sqsRecord},
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

func convertSQSMessageToProtobufRecord(msg *sqs.Message) *records.SQSRecord {
	sqsRecord := &records.SQSRecord{
		Attributes:        make(map[string]string, 0),
		Messageattributes: make(map[string]*records.SQSRecordMessageAttribute, 0),
		Messageid:         *msg.MessageId,
		Receipt:           *msg.ReceiptHandle,
		Body:              []byte(*msg.Body),
		Timestamp:         time.Now().UTC().UnixNano(),
	}

	for k, v := range msg.Attributes {
		sqsRecord.Attributes[k] = *v
	}

	for k, v := range msg.MessageAttributes {
		sqsRecord.Messageattributes[k] = &records.SQSRecordMessageAttribute{
			Datatype:    *v.DataType,
			Stringvalue: *v.StringValue,
			Binaryvalue: v.BinaryValue,
		}
	}

	return sqsRecord
}
