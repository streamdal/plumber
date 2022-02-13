package awssns

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/dynamic"
	"github.com/batchcorp/plumber/validate"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

func (a *AWSSNS) Dynamic(ctx context.Context, dynamicOpts *opts.DynamicOptions, dynamicSvc dynamic.IDynamic, errorCh chan<- *records.ErrorRecord) error {
	if err := validateDynamicOptions(dynamicOpts); err != nil {
		return errors.Wrap(err, "unable to validate dynamic options")
	}

	llog := a.log.WithField("pkg", "activemq/dynamic")

	if err := dynamicSvc.Start(ctx, "AWS SNS"); err != nil {
		return errors.Wrap(err, "unable to create dynamic")
	}

	topic := dynamicOpts.AwsSns.Args.Topic

	outboundCh := dynamicSvc.Read()

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

func validateDynamicOptions(dynamicOpts *opts.DynamicOptions) error {
	if dynamicOpts == nil {
		return validate.ErrEmptyDynamicOpts
	}

	if dynamicOpts.AwsSns == nil {
		return validate.ErrEmptyBackendGroup
	}

	if dynamicOpts.AwsSns.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	topic := dynamicOpts.AwsSns.Args.Topic

	if topic == "" {
		return ErrMissingTopicARN
	}

	if arn.IsARN(topic) == false {
		return fmt.Errorf("'%s' is not a valid ARN", topic)
	}
	return nil
}
