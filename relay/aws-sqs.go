package relay

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/batchcorp/schemas/build/go/events/records"
	"github.com/batchcorp/schemas/build/go/services"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

func (r *Relay) handleSQS(ctx context.Context, conn *grpc.ClientConn, msg *sqs.Message) error {
	sqsRecord := convertSQSMessageToProtobufRecord(msg)

	client := services.NewGRPCCollectorClient(conn)

	if _, err := client.AddSQSRecord(ctx, &services.SQSRecordRequest{
		Records: []*records.SQSRecord{sqsRecord},
	}); err != nil {
		return errors.Wrap(err, "unable to complete AddSQSRecord call")
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
