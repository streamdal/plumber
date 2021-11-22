package types

import (
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

type RelayMessage struct {
	Value   *sqs.Message
	Options *RelayMessageOptions
}

type RelayMessageOptions struct {
	Service    sqsiface.SQSAPI
	QueueURL   string
	AutoDelete bool
}
