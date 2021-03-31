package cli

import "gopkg.in/alecthomas/kingpin.v2"

type BatchOptions struct {
	CollectionID   string
	CollectionName string

	// Destination specific
	DestinationID   string
	DestinationName string
	DestinationType string
	Metadata        *DestinationMetadata

	Notes    string
	Query    string
	Page     int
	ReplayID string
	SchemaID string
}

type DestinationMetadata struct {
	KafkaTopic       string
	KafkaAddress     string
	KafkaUseTLS      bool
	KafkaInsecureTLS bool
	KafkaSASLType    string
	KafkaUsername    string
	KafkaPassword    string

	// HTTP specific
	HTTPURL     string
	HTTPHeaders map[string]string

	// AWS-SQS specific
	SQSQueue     string
	SQSAccountID string

	// RabbitMQ
	RabbitDSN                string
	RabbitExchangeName       string
	RabbitRoutingKey         string
	RabbitExchangeType       string
	RabbitExchangeDeclare    bool
	RabbitExchangeAutoDelete bool
	RabbitExchangeDurable    bool
}

func HandleBatchFlags(batchCmd *kingpin.CmdClause, opts *Options) {
	batchCmd.Command("login", "Login to your Batch.sh account")
	batchCmd.Command("logout", "Logout and clear saved credentials")

	listCmd := batchCmd.Command("list", "List batch resources")
	listCmd.Command("destination", "Replay destinations")
	listCmd.Command("schema", "Schemas")
	listCmd.Command("replay", "Replays")
	listCmd.Command("collection", "Collections")

	createCmd := batchCmd.Command("create", "Create a batch resource")

	createCollectionCmd := createCmd.Command("collection", "Create a new collection")
	handleCreateCollectionFlags(createCollectionCmd, opts)

	createDestinationCmd := createCmd.Command("destination", "Create a new destination")
	handleCreateDestinationFlags(createDestinationCmd, opts)

	// Search Collections
	searchCmd := batchCmd.Command("search", "Search A Collection")
	searchCmd.Flag("query", "Search a specified collection").
		Default("*").
		StringVar(&opts.Batch.Query)

	searchCmd.Flag("collection-id", "Collection ID").
		StringVar(&opts.Batch.CollectionID)

	searchCmd.Flag("page", "Page of search results to display. A page is 25 results").
		IntVar(&opts.Batch.Page)
}

func handleCreateDestinationFlags(cmd *kingpin.CmdClause, opts *Options) {
	kafkaCmd := cmd.Command("kafka", "Kafka Destination")
	httpCmd := cmd.Command("http", "HTTP Post Destination")
	sqsCmd := cmd.Command("aws-sqs", "AWS SQS Destination")
	rabbitCmd := cmd.Command("rabbit", "RabbitMQ Destination")

	handleCreateDestinationFlagsKafka(kafkaCmd, opts)
	handleCreateDestinationFlagsHTTP(httpCmd, opts)
	handleCreateDestinationFlagsSQS(sqsCmd, opts)
	handleCreateDestinationFlagsRabbit(rabbitCmd, opts)
}

func handleCreateDestinationFlagsShared(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("name", "Destination Name").
		Required().
		StringVar(&opts.Batch.DestinationName)

	cmd.Flag("notes", "Destination Notes").
		StringVar(&opts.Batch.Notes)
}

func handleCreateDestinationFlagsKafka(cmd *kingpin.CmdClause, opts *Options) {
	handleCreateDestinationFlagsShared(cmd, opts)

	cmd.Flag("topic", "Kafka Topic").
		Required().
		StringVar(&opts.Batch.Metadata.KafkaTopic)

	cmd.Flag("address", "Kafka Server Address and Port. (Ex: msg.domain.com:9092)").
		Required().
		StringVar(&opts.Batch.Metadata.KafkaAddress)

	cmd.Flag("use-tls", "Connect to kafka using TLS").
		BoolVar(&opts.Batch.Metadata.KafkaUseTLS)

	cmd.Flag("insecure-tls", "Connect to kafka but do not verify TLS").
		BoolVar(&opts.Batch.Metadata.KafkaInsecureTLS)

	cmd.Flag("sasl-type", "Kafka Authentication Type").
		EnumVar(&opts.Batch.Metadata.KafkaSASLType, "", "basic", "scram")

	cmd.Flag("username", "Kafka Username (required if --sasl-type is specified)").
		StringVar(&opts.Batch.Metadata.KafkaUsername)

	cmd.Flag("password", "Kafka Password  (required if --sasl-type is specified)").
		StringVar(&opts.Batch.Metadata.KafkaPassword)
}

func handleCreateDestinationFlagsHTTP(cmd *kingpin.CmdClause, opts *Options) {
	handleCreateDestinationFlagsShared(cmd, opts)

	cmd.Flag("url", "Full URL to HTTP server. (Ex: https://myserver.com)").
		StringVar(&opts.Batch.Metadata.HTTPURL)

	cmd.Flag("with-header", "HTTP Header to include with POST request of replayed event. You can specify "+
		"this flag multiple times for each header").
		StringMapVar(&opts.Batch.Metadata.HTTPHeaders)
}

func handleCreateDestinationFlagsSQS(cmd *kingpin.CmdClause, opts *Options) {
	handleCreateDestinationFlagsShared(cmd, opts)

	cmd.Flag("aws-account-id", "AWS Account ID containing the SQS queue to publish to").
		Required().
		StringVar(&opts.Batch.Metadata.SQSAccountID)

	cmd.Flag("queue-name", "SQS Queue name to publish to").
		Required().
		StringVar(&opts.Batch.Metadata.SQSQueue)
}

func handleCreateDestinationFlagsRabbit(cmd *kingpin.CmdClause, opts *Options) {
	handleCreateDestinationFlagsShared(cmd, opts)

	cmd.Flag("dsn", "RabbitMQ DSN (Ex: amqps://server.domain.com:5672)").
		Required().
		StringVar(&opts.Batch.Metadata.RabbitDSN)

	cmd.Flag("exchange-name", "RabbitMQ Exchange to publish to").
		Required().
		StringVar(&opts.Batch.Metadata.RabbitExchangeName)

	cmd.Flag("exchange-type", "RabbitMQ Exchange Type").
		Default("topic").
		EnumVar(&opts.Batch.Metadata.RabbitExchangeType, "topic", "direct", "fanout", "headers")

	cmd.Flag("routing-key", "RabbitMQ Routing Key to publish to").
		Required().
		StringVar(&opts.Batch.Metadata.RabbitRoutingKey)

	cmd.Flag("exchange-declare", "Whether to declare the RabbitMQ exchange or not").
		BoolVar(&opts.Batch.Metadata.RabbitExchangeDeclare)

	cmd.Flag("exchange-auto-delete", "Whether to auto-delete the exchange at disconnect").
		BoolVar(&opts.Batch.Metadata.RabbitExchangeDeclare)

	cmd.Flag("exchange-durable", "Whether the RabbitMQ exchange is durable").
		BoolVar(&opts.Batch.Metadata.RabbitExchangeDeclare)
}

func handleCreateCollectionFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("name", "Collection Name").
		Required().
		StringVar(&opts.Batch.CollectionName)

	cmd.Flag("schema-id", "Schema ID. Can be obtained by running `plumber batch schemas`").
		Required().
		StringVar(&opts.Batch.SchemaID)

	cmd.Flag("notes", "Any notes for this collection").
		StringVar(&opts.Batch.SchemaID)
}
