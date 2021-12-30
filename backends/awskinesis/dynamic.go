package awskinesis

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/validate"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber/dynamic"
)

func (k *Kinesis) Dynamic(ctx context.Context, dynamicOpts *opts.DynamicOptions, dynamicSvc dynamic.IDynamic) error {
	if err := validateDynamicOptions(dynamicOpts); err != nil {
		return errors.Wrap(err, "unable to validate dynamic options")
	}

	llog := k.log.WithField("pkg", "kinesis/dynamic")

	go dynamicSvc.Start("AWS Kinesis")

	outboundCh := dynamicSvc.Read()

	args := dynamicOpts.AwsKinesis.Args

	// Continually loop looking for messages on the channel.
	for {
		select {
		case outbound := <-outboundCh:
			putOpts := &kinesis.PutRecordInput{
				Data:         outbound.Blob,
				PartitionKey: aws.String(args.PartitionKey),
				StreamName:   aws.String(args.Stream),
			}

			if _, err := k.client.PutRecord(putOpts); err != nil {
				k.log.Errorf("Unable to replay message: %s", err)
				break
			}

			k.log.Debugf("Replayed message to Kinesis stream '%s' for replay '%s'", args.Stream, outbound.ReplayId)
		case <-ctx.Done():
			llog.Warning("context cancelled")
			return nil
		}
	}

	return nil
}

func validateDynamicOptions(dynamicOpts *opts.DynamicOptions) error {
	if dynamicOpts == nil {
		return validate.ErrEmptyDynamicOpts
	}

	if dynamicOpts.AwsKinesis == nil {
		return validate.ErrEmptyBackendGroup
	}

	args := dynamicOpts.AwsKinesis.Args
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
