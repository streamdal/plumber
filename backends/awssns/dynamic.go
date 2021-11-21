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
)

func (a *AWSSNS) Dynamic(ctx context.Context, dynamicOpts *opts.DynamicOptions, dynamicSvc dynamic.IDynamic) error {
	if err := validateDynamicOptions(dynamicOpts); err != nil {
		return errors.Wrap(err, "unable to validate dynamic options")
	}

	go dynamicSvc.Start("AWS SNS")

	topic := dynamicOpts.Awssns.Args.Topic

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
				a.log.Errorf("Unable to replay message: %s", err)
				break
			}

			a.log.Debugf("Replayed message to AWSSNS topic '%s' for replay '%s'", topic, outbound.ReplayId)
		}
	}

	return nil
}

func validateDynamicOptions(dynamicOpts *opts.DynamicOptions) error {
	if dynamicOpts == nil {
		return validate.ErrEmptyDynamicOpts
	}

	if dynamicOpts.Awssns == nil {
		return validate.ErrEmptyBackendGroup
	}

	if dynamicOpts.Awssns.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	topic := dynamicOpts.Awssns.Args.Topic

	if topic == "" {
		return ErrMissingTopicARN
	}

	if arn.IsARN(topic) == false {
		return fmt.Errorf("'%s' is not a valid ARN", topic)
	}
	return nil
}
