package awssns

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/tunnel"
	"github.com/batchcorp/plumber/validate"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

func (a *AWSSNS) Tunnel(ctx context.Context, tunnelOpts *opts.TunnelOptions, tunnelSvc tunnel.ITunnel, errorCh chan<- *records.ErrorRecord) error {
	if err := validateTunnelOptions(tunnelOpts); err != nil {
		return errors.Wrap(err, "unable to validate tunnel options")
	}

	llog := a.log.WithField("pkg", "activemq/tunnel")

	if err := tunnelSvc.Start(ctx, "AWS SNS", errorCh); err != nil {
		return errors.Wrap(err, "unable to create tunnel")
	}

	topic := tunnelOpts.AwsSns.Args.Topic

	outboundCh := tunnelSvc.Read()

	// Continually loop looking for messages on the channel.
	for {
		select {
		case outbound := <-outboundCh:
			_, err := a.Service.Publish(&sns.PublishInput{
				Message:  aws.String(string(outbound.Blob)),
				TopicArn: aws.String(topic),
			})
			if err != nil {
				llog.Errorf("Unable to replay message: %s", err)
				break
			}

			llog.Debugf("Replayed message to AWSSNS topic '%s' for replay '%s'", topic, outbound.ReplayId)
		case <-ctx.Done():
			llog.Debug("context cancelled")
			return nil
		}
	}

	return nil
}

func validateTunnelOptions(tunnelOpts *opts.TunnelOptions) error {
	if tunnelOpts == nil {
		return validate.ErrEmptyTunnelOpts
	}

	if tunnelOpts.AwsSns == nil {
		return validate.ErrEmptyBackendGroup
	}

	if tunnelOpts.AwsSns.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	topic := tunnelOpts.AwsSns.Args.Topic

	if topic == "" {
		return ErrMissingTopicARN
	}

	if arn.IsARN(topic) == false {
		return fmt.Errorf("'%s' is not a valid ARN", topic)
	}
	return nil
}
