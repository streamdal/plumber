package awskinesis

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/validate"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/tunnel"
)

func (k *Kinesis) Tunnel(ctx context.Context, tunnelOpts *opts.DynamicOptions, dynamicSvc tunnel.ITunnel, errorCh chan<- *records.ErrorRecord) error {
	if err := validateTunnelOptions(tunnelOpts); err != nil {
		return errors.Wrap(err, "unable to validate tunnel options")
	}

	llog := k.log.WithField("pkg", "kinesis/tunnel")

	if err := dynamicSvc.Start(ctx, "AWS Kinesis", errorCh); err != nil {
		return errors.Wrap(err, "unable to create tunnel")
	}

	outboundCh := dynamicSvc.Read()

	args := tunnelOpts.AwsKinesis.Args

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
			llog.Debug("context cancelled")
			return nil
		}
	}

	return nil
}

func validateTunnelOptions(tunnelOpts *opts.DynamicOptions) error {
	if tunnelOpts == nil {
		return validate.ErrEmptyDynamicOpts
	}

	if tunnelOpts.AwsKinesis == nil {
		return validate.ErrEmptyBackendGroup
	}

	args := tunnelOpts.AwsKinesis.Args
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
