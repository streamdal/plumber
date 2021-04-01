package batch

import (
	"encoding/json"
	"errors"
	"fmt"
)

// DestinationOutput is used for displaying destinations as a table
type DestinationOutput struct {
	Name     string `json:"name" header:"Name"`
	ID       string `json:"id" header:"Destination ID"`
	Type     string `json:"type" header:"Type""`
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
		"type":     b.Opts.Batch.DestinationType,
		"name":     b.Opts.Batch.DestinationName,
		"notes":    b.Opts.Batch.Notes,
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

		return nil, fmt.Errorf("received a non-200 response (%d) from API: %s", code, err)
	}

	createdDestination := &DestinationOutput{}
	if err := json.Unmarshal(res, createdDestination); err != nil {
		return nil, errCreateCollectionFailed
	}

	return createdDestination, nil
}

func (b *Batch) CreateDestination(dstType string) error {
	destination, err := b.createDestination(dstType)
	if err != nil {
		return err
	}

	b.Log.Infof("Created %s destination %s!\n", b.Opts.Batch.DestinationType, destination.ID)

	return nil
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
		"topic":        b.Opts.Batch.DestinationMetadata.KafkaTopic,
		"address":      b.Opts.Batch.DestinationMetadata.KafkaAddress,
		"use_tls":      b.Opts.Batch.DestinationMetadata.KafkaUseTLS,
		"insecure_tls": b.Opts.Batch.DestinationMetadata.KafkaInsecureTLS,
		"sasl_type":    b.Opts.Batch.DestinationMetadata.KafkaSASLType,
		"username":     b.Opts.Batch.DestinationMetadata.KafkaUsername,
		"password":     b.Opts.Batch.DestinationMetadata.KafkaPassword,
	}
}

func (b *Batch) getDestinationMetadataHTTP() map[string]interface{} {
	headers := make([]map[string]string, 0)
	for k, v := range b.Opts.Batch.DestinationMetadata.HTTPHeaders {
		headers = append(headers, map[string]string{k: v})
	}

	return map[string]interface{}{
		"url":     b.Opts.Batch.DestinationMetadata.HTTPURL,
		"headers": headers,
	}
}

func (b *Batch) getDestinationMetadataSQS() map[string]interface{} {
	return map[string]interface{}{
		"aws_account_id": b.Opts.Batch.DestinationMetadata.SQSAccountID,
		"queue_name":     b.Opts.Batch.DestinationMetadata.SQSQueue,
	}
}

func (b *Batch) getDestinationMetadataRabbitMQ() map[string]interface{} {
	return map[string]interface{}{
		"dsn":                  b.Opts.Batch.DestinationMetadata.RabbitDSN,
		"exchange":             b.Opts.Batch.DestinationMetadata.RabbitExchangeName,
		"routing_key":          b.Opts.Batch.DestinationMetadata.RabbitRoutingKey,
		"exchange_type":        b.Opts.Batch.DestinationMetadata.RabbitExchangeType,
		"exchange_declare":     b.Opts.Batch.DestinationMetadata.RabbitExchangeDeclare,
		"exchange_durable":     b.Opts.Batch.DestinationMetadata.RabbitExchangeDurable,
		"exchange_auto_delete": b.Opts.Batch.DestinationMetadata.RabbitExchangeAutoDelete,
	}
}
