package server

import (
	"errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/conns"
	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
)

var (
	// Connections

	ErrMissingConnection     = errors.New("connection cannot be nil")
	ErrMissingAddress        = errors.New("at least one kafka server address must be specified")
	ErrMissingUsername       = errors.New("you must provide a username when specifying a SASL type")
	ErrMissingPassword       = errors.New("you must provide a password when specifying a SASL type")
	ErrMissingConnName       = errors.New("you must provide a connection name")
	ErrMissingConnectionType = errors.New("you must provide at least one connection of: kafka")

	// Reads

	ErrMissingConnectionID      = errors.New("missing connection ID")
	ErrMissingRead              = errors.New("missing Read message")
	ErrMissingReadType          = errors.New("you must provide at least one read argument message")
	ErrMissingTopic             = errors.New("you must provide at least one topic to read from")
	ErrMissingConsumerGroupName = errors.New("group name must be specified when using a consumer group")
	ErrMissingRootType          = errors.New("root message cannot be empty")
	ErrMissingZipArchive        = errors.New("zip archive is empty")
	ErrMissingAVROSchema        = errors.New("AVRO schema cannot be empty")
)

// validateConnection ensures all required parameters are passed when creating/testing/updating a connection
func validateConnection(conn *protos.Connection) error {
	if conn == nil {
		return ErrMissingConnection
	}

	if conn.Name == "" {
		return ErrMissingConnName
	}

	switch {
	case conn.GetKafka() != nil:
		return validateConnectionKafka(conn.GetKafka())
	}

	return ErrMissingConnectionType
}

// validateConnectionKafka ensures all required parameters are passed when creating/testing/updating a kafka connection
func validateConnectionKafka(conn *conns.Kafka) error {
	if len(conn.Address) == 0 {
		return ErrMissingAddress
	}

	if conn.SaslType != conns.SASLType_NONE && conn.SaslUsername == "" {
		return ErrMissingUsername
	}

	if conn.SaslType != conns.SASLType_NONE && conn.SaslPassword == "" {
		return ErrMissingPassword
	}

	return nil
}

func validateRead(req *protos.Read) error {
	if req == nil {
		return ErrMissingRead
	}

	if req.ConnectionId == "" {
		return ErrMissingConnectionID
	}

	if err := validateDecodeOptions(req.GetDecodeOptions()); err != nil {
		return err
	}

	switch {
	case req.GetKafka() != nil:
		return validateArgsKafka(req.GetKafka())
	}

	return ErrMissingReadType
}

// validateArgsKafka ensures all mandatory arguments are present for a kafka read
func validateArgsKafka(cfg *args.Kafka) error {
	if len(cfg.Topics) == 0 {
		return ErrMissingTopic
	}

	if cfg.UseConsumerGroup && cfg.ConsumerGroupName == "" {
		return ErrMissingConsumerGroupName
	}

	return nil
}

func validateDecodeOptions(opts *encoding.Options) error {
	if opts == nil {
		// These aren't mandatory. Only check if they are specified
		return nil
	}

	if err := validateDecodeOptionsProtobuf(opts.GetProtobuf()); err != nil {
		return err
	}

	// TODO
	return nil
}

func validateDecodeOptionsProtobuf(opts *encoding.Protobuf) error {
	if opts == nil {
		// These aren't mandatory. Only check if they are specified
		return nil
	}

	if opts.RootType == "" {
		return ErrMissingRootType
	}

	if opts.ZipArchive == nil {
		return ErrMissingZipArchive
	}

	return nil
}

func validateDecodeOptionsAvro(opts *encoding.Avro) error {
	if opts == nil {
		// These aren't mandatory. Only check if they are specified
		return nil
	}

	if opts.Schema == nil {
		return ErrMissingAVROSchema
	}

	return nil
}
