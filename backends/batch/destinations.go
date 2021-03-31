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
		b.Log.Info()
		return nil, errNoDestinations
	}

	return output, nil
}

func (b *Batch) CreateDestination(dstType string) error {
	p := map[string]interface{}{
		"type":     b.Opts.Batch.DestinationType,
		"name":     b.Opts.Batch.DestinationName,
		"notes":    b.Opts.Batch.Notes,
		"metadata": b.getDestinationMetadata(dstType),
	}

	j, _ := json.MarshalIndent(p, "", "  ")

	fmt.Println(string(j))

	return nil
	res, code, err := b.Post("/v1/destination", p)
	if err != nil {
		return errCreateDestinationFailed
	}

	if code > 299 {
		errResponse := &BlunderErrorResponse{}
		if err := json.Unmarshal(res, errResponse); err != nil {
			return errCreateDestinationFailed
		}

		for _, e := range errResponse.Errors {
			b.Log.Errorf("%s: '%s' %s", errCreateDestinationFailed, e.Field, e.Message)
		}

		return nil
	}

	createdDestination := &DestinationOutput{}
	if err := json.Unmarshal(res, createdDestination); err != nil {
		return errCreateCollectionFailed
	}

	b.Log.Infof("Created %s destination %s!\n", b.Opts.Batch.DestinationType, createdDestination.ID)

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
		"topic":        b.Opts.Batch.Metadata.KafkaTopic,
		"address":      b.Opts.Batch.Metadata.KafkaAddress,
		"use_tls":      b.Opts.Batch.Metadata.KafkaUseTLS,
		"insecure_tls": b.Opts.Batch.Metadata.KafkaInsecureTLS,
		"sasl_type":    b.Opts.Batch.Metadata.KafkaSASLType,
		"username":     b.Opts.Batch.Metadata.KafkaUsername,
		"password":     b.Opts.Batch.Metadata.KafkaPassword,
	}
}

func (b *Batch) getDestinationMetadataHTTP() map[string]interface{} {
	headers := make([]map[string]string, 0)
	for k, v := range b.Opts.Batch.Metadata.HTTPHeaders {
		headers = append(headers, map[string]string{k: v})
	}

	return map[string]interface{}{
		"url":     b.Opts.Batch.Metadata.HTTPURL,
		"headers": headers,
	}
}

func (b *Batch) getDestinationMetadataSQS() map[string]interface{} {
	return map[string]interface{}{
		"aws_account_id": b.Opts.Batch.Metadata.SQSAccountID,
		"queue_name":     b.Opts.Batch.Metadata.SQSQueue,
	}
}

func (b *Batch) getDestinationMetadataRabbitMQ() map[string]interface{} {
	return map[string]interface{}{
		"dsn":                  b.Opts.Batch.Metadata.RabbitDSN,
		"exchange":             b.Opts.Batch.Metadata.RabbitExchangeName,
		"routing_key":          b.Opts.Batch.Metadata.RabbitRoutingKey,
		"exchange_type":        b.Opts.Batch.Metadata.RabbitExchangeType,
		"exchange_declare":     b.Opts.Batch.Metadata.RabbitExchangeDeclare,
		"exchange_durable":     b.Opts.Batch.Metadata.RabbitExchangeDurable,
		"exchange_auto_delete": b.Opts.Batch.Metadata.RabbitExchangeAutoDelete,
	}
}
