package awssns

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/writer"
)

func Write(opts *cli.Options) error {
	if err := writer.ValidateWriteOptions(opts, validateWriteOptions); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	var mdErr error
	var md *desc.MessageDescriptor

	if opts.WriteInputType == "jsonpb" {
		md, mdErr = pb.FindMessageDescriptor(opts.WriteProtobufDirs, opts.WriteProtobufRootMessage)
		if mdErr != nil {
			return errors.Wrap(mdErr, "unable to find root message descriptor")
		}
	}

	svc, err := NewService(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create new service")
	}

	a := &AWSSNS{
		Options: opts,
		Service: svc,
		MsgDesc: md,
		log:     logrus.WithField("pkg", "aws-sns/write.go"),
	}

	msg, err := writer.GenerateWriteValue(nil, opts)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	return a.Write(msg)
}

func validateWriteOptions(opts *cli.Options) error {
	if opts.AWSSNS.TopicArn == "" {
		return errors.New("--topic cannot be empty")
	}

	if arn.IsARN(opts.AWSSNS.TopicArn) == false {
		return fmt.Errorf("'%s' is not a valid ARN", opts.AWSSNS.TopicArn)
	}
	return nil
}

func (a *AWSSNS) Write(value []byte) error {
	result, err := a.Service.Publish(&sns.PublishInput{
		Message:  aws.String(string(value)),
		TopicArn: aws.String(a.Options.AWSSNS.TopicArn),
	})
	if err != nil {
		return errors.Wrap(err, "could not publish message to SNS")
	}

	a.log.Infof("Message '%s' published to topic '%s'", *result.MessageId, a.Options.AWSSNS.TopicArn)
	return nil
}
