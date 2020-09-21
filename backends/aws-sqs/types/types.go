package types

import "github.com/aws/aws-sdk-go/service/sqs"

type RelayMessage struct {
	Value   *sqs.Message
	Options *RelayMessageOptions
}

type RelayMessageOptions struct {
	Service    *sqs.SQS
	QueueURL   string
	AutoDelete bool
}
