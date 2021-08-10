package awssns

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/writer"
)

var (
	errMissingTopicARN = errors.New("--topic cannot be empty")
)

func Write(opts *options.Options, md *desc.MessageDescriptor) error {
	if err := writer.ValidateWriteOptions(opts, validateWriteOptions); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	writeValues, err := writer.GenerateWriteValues(nil, opts)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	svc, err := NewService(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create new service")
	}

	a := &AWSSNS{
		Options: opts,
		service: svc,
		msgDesc: md,
		log:     logrus.WithField("pkg", "aws-sns/write.go"),
	}

	for _, value := range writeValues {
		if err := a.Write(value); err != nil {
			a.log.Error(err)
		}
	}

	return nil
}

func validateWriteOptions(opts *options.Options) error {
	if opts.AWSSNS.TopicArn == "" {
		return errMissingTopicARN
	}

	if arn.IsARN(opts.AWSSNS.TopicArn) == false {
		return fmt.Errorf("'%s' is not a valid ARN", opts.AWSSNS.TopicArn)
	}
	return nil
}

func (a *AWSSNS) Write(value []byte) error {
	result, err := a.service.Publish(&sns.PublishInput{
		Message:  aws.String(string(value)),
		TopicArn: aws.String(a.Options.AWSSNS.TopicArn),
	})
	if err != nil {
		return errors.Wrap(err, "could not publish message to SNS")
	}

	a.log.Infof("Message '%s' published to topic '%s'", *result.MessageId, a.Options.AWSSNS.TopicArn)
	return nil
}
