package awskinesis

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

func (k *Kinesis) Write(ctx context.Context, writeOpts *opts.WriteOptions, errorCh chan *records.ErrorRecord, messages ...*records.WriteRecord) error {
	if err := validateWriteOptions(writeOpts); err != nil {
		return errors.Wrap(err, "invalid write options")
	}

	for _, msg := range messages {
		var sequenceNumber *string
		if writeOpts.AwsKinesis.Args.SequenceNumber != "" {
			sequenceNumber = aws.String(writeOpts.AwsKinesis.Args.SequenceNumber)
		}

		out, err := k.client.PutRecord(&kinesis.PutRecordInput{
			Data:                      []byte(msg.Input),
			PartitionKey:              aws.String(writeOpts.AwsKinesis.Args.PartitionKey),
			SequenceNumberForOrdering: sequenceNumber,
			StreamName:                aws.String(writeOpts.AwsKinesis.Args.Stream),
		})
		if err != nil {
			util.WriteError(k.log, errorCh, err)
			continue
		}

		k.log.Infof("Wrote message to shard '%s' with sequence number '%s'",
			util.DerefString(out.ShardId), util.DerefString(out.SequenceNumber))
	}

	return nil
}

func validateWriteOptions(writeOpts *opts.WriteOptions) error {
	if writeOpts == nil {
		return validate.ErrEmptyWriteOpts
	}

	if writeOpts.AwsKinesis == nil {
		return validate.ErrEmptyBackendGroup
	}

	args := writeOpts.AwsKinesis.Args
	if args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if args.Stream == "" {
		return ErrEmptyStream
	}

	if args.PartitionKey == "" {
		return ErrEmptyPartitionKey
	}

	return nil
}
