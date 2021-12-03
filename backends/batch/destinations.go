package batch

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
)

// DestinationOutput is used for displaying destinations as a table
type DestinationOutput struct {
	Name     string `json:"name" header:"Name"`
	ID       string `json:"id" header:"Destination ID"`
	Type     string `json:"type" header:"Type"`
	Archived bool   `json:"archived" header:"Is Archived"`
}

var (
	errDestinationsFailed      = errors.New("unable to get list of destinations")
	errNoDestinations          = errors.New("you have no destinations")
	errCreateDestinationFailed = errors.New("failed to create destination")
)

// ListDestinations lists all of an account's replay destinations
func (b *Batch) ListDestinations() error {
	output, err := b.listDestinations()
	if err != nil {
		return err
	}

	b.Printer(output)

	return nil
}

func (b *Batch) listDestinations() ([]DestinationOutput, error) {
	res, _, err := b.Get("/v1/destination", nil)
	if err != nil {
		return nil, errDestinationsFailed
	}

	output := make([]DestinationOutput, 0)

	err = json.Unmarshal(res, &output)
	if err != nil {
		return nil, errDestinationsFailed
	}

	if len(output) == 0 {
		return nil, errNoDestinations
	}

	return output, nil
}

func (b *Batch) createDestination(dstType string) (*DestinationOutput, error) {
	p := map[string]interface{}{
		"type":     b.Opts.Batch.Create.Destination.XApiDestinationType,
		"name":     b.Opts.Batch.Create.Destination.Name,
		"notes":    b.Opts.Batch.Create.Destination.Notes,
		"metadata": b.getDestinationMetadata(dstType),
	}

	res, code, err := b.Post("/v1/destination", p)
	if err != nil {
		return nil, errCreateDestinationFailed
	}

	if code > 299 {
		errResponse := &BlunderErrorResponse{}
		if err := json.Unmarshal(res, errResponse); err != nil {
			return nil, errCreateDestinationFailed
		}

		for _, e := range errResponse.Errors {
			err := fmt.Errorf("%s: '%s' %s", errCreateDestinationFailed, e.Field, e.Message)
			b.Log.Error(err)
		}

		return nil, fmt.Errorf("received a non-200 response (%d) from API", code)
	}

	createdDestination := &DestinationOutput{}
	if err := json.Unmarshal(res, createdDestination); err != nil {
		return nil, errCreateCollectionFailed
	}

	return createdDestination, nil
}

func (b *Batch) CreateDestination(dstType string) error {
	apiDestinationType, err := convertDestinationType(dstType)
	if err != nil {
		return errors.Wrap(err, "unable to convert destination type")
	}

	b.Opts.Batch.Create.Destination.XApiDestinationType = apiDestinationType

	destination, err := b.createDestination(dstType)
	if err != nil {
		return err
	}

	b.Log.Infof("Created %s destination %s!\n", b.Opts.Batch.Create.Destination.XApiDestinationType, destination.ID)

	return nil
}

func convertDestinationType(dstType string) (string, error) {
	switch dstType {
	case "kafka":
		return "kafka", nil
	case "http":
		return "http", nil
	case "aws-sqs":
		return "sqs", nil
	case "rabbit":
		return "rmq", nil
	default:
		return "", fmt.Errorf("unrecognized destination type '%s'", dstType)
	}
}

func (b *Batch) getDestinationMetadata(destType string) map[string]interface{} {
	switch destType {
	case "kafka":
		return b.getDestinationMetadataKafka()
	case "http":
		return b.getDestinationMetadataHTTP()
	case "aws-sqs":
		return b.getDestinationMetadataSQS()
	case "rabbit":
		return b.getDestinationMetadataRabbitMQ()
	}

	return nil
}

func (b *Batch) getDestinationMetadataKafka() map[string]interface{} {
	return map[string]interface{}{
		"topic":        b.Opts.Batch.Create.Destination.Kafka.Args.Topics[0],
		"address":      b.Opts.Batch.Create.Destination.Kafka.XConn.Address,
		"use_tls":      b.Opts.Batch.Create.Destination.Kafka.XConn.UseTls,
		"insecure_tls": b.Opts.Batch.Create.Destination.Kafka.XConn.TlsSkipVerify,
		"sasl_type":    b.Opts.Batch.Create.Destination.Kafka.XConn.SaslType,
		"username":     b.Opts.Batch.Create.Destination.Kafka.XConn.SaslUsername,
		"password":     b.Opts.Batch.Create.Destination.Kafka.XConn.SaslPassword,
	}
}

func (b *Batch) getDestinationMetadataHTTP() map[string]interface{} {
	headers := make([]map[string]string, 0)
	for k, v := range b.Opts.Batch.Create.Destination.Http.Headers {
		headers = append(headers, map[string]string{k: v})
	}

	return map[string]interface{}{
		"url":     b.Opts.Batch.Create.Destination.Http.Url,
		"headers": headers,
	}
}

func (b *Batch) getDestinationMetadataSQS() map[string]interface{} {
	return map[string]interface{}{
		"aws_account_id": b.Opts.Batch.Create.Destination.AwsSqs.Args.RemoteAccountId,
		"queue_name":     b.Opts.Batch.Create.Destination.AwsSqs.Args.QueueName,
	}
}

func (b *Batch) getDestinationMetadataRabbitMQ() map[string]interface{} {
	return map[string]interface{}{
		"dsn":                  b.Opts.Batch.Create.Destination.Rabbit.XConn.Address,
		"exchange":             b.Opts.Batch.Create.Destination.Rabbit.Args.ExchangeName,
		"routing_key":          b.Opts.Batch.Create.Destination.Rabbit.Args.RoutingKey,
		"exchange_type":        b.Opts.Batch.Create.Destination.Rabbit.Args.ExchangeType,
		"exchange_declare":     b.Opts.Batch.Create.Destination.Rabbit.Args.ExchangeDeclare,
		"exchange_durable":     b.Opts.Batch.Create.Destination.Rabbit.Args.ExchangeDurable,
		"exchange_auto_delete": b.Opts.Batch.Create.Destination.Rabbit.Args.ExchangeAutoDelete,
	}
}
