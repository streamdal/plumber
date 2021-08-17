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
	Service    ISQSAPI
	QueueURL   string
	AutoDelete bool
}

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 . ISQSAPI
type ISQSAPI interface {
	sqsiface.SQSAPI
}
