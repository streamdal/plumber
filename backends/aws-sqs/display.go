package awssqs

import (
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
	"github.com/batchcorp/plumber/types"
	"github.com/pkg/errors"
)

func (a *AWSSQS) DisplayMessage(msg *types.ReadMessage) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	rawMsg, ok := msg.Raw.(*sqs.Message)
	if !ok {
		return errors.New("unable to type assert message")
	}

	decoded, err := reader.Decode(a.Options, msg.Value)
	if err != nil {
		return errors.Wrap(err, "unable to decode data")
	}

	properties := [][]string{
		{"Message ID", *rawMsg.MessageId},
		{"Receipt Handle", *rawMsg.ReceiptHandle},
	}

	for attrName, attr := range rawMsg.Attributes {
		properties = append(properties, []string{
			"Attribute " + attrName, *attr,
		})
	}

	printer.PrintTable(properties, msg.Num, msg.ReceivedAt, decoded)

	return nil
}

func (a *AWSSQS) DisplayError(msg *types.ErrorMessage) error {
	printer.DefaultDisplayError(msg)
	return nil
}
