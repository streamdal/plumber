package options

import (
	"gopkg.in/alecthomas/kingpin.v2"
)

type BatchOptions struct {
	// Shared
	DestinationID string
	CollectionID  string
	ReplayID      string
	SchemaID      string
	Notes         string
	Query         string
	Page          int
	OutputType    string

	// Collection specific
	CollectionName string

	// Destination specific
	DestinationName     string
	DestinationType     string
	DestinationMetadata *DestinationMetadata

	// Replay Specific
	ReplayName string
	ReplayType string
	ReplayFrom string
	ReplayTo   string
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

	// List
	listCmd := batchCmd.Command("list", "List batch resources")

	listCmd.Command("destination", "Replay destinations")
	listCmd.Command("schema", "Schemas")
	listCmd.Command("collection", "Collections")
	listCmd.Command("replay", "Replays")

	handleListFlags(listCmd, opts)

	// Create
	createCmd := batchCmd.Command("create", "Create a batch resource")

	createCollectionCmd := createCmd.Command("collection", "Create a new collection")
	handleCreateCollectionFlags(createCollectionCmd, opts)

	createDestinationCmd := createCmd.Command("destination", "Create a new destination")
	handleCreateDestinationFlags(createDestinationCmd, opts)

	createReplayCmd := createCmd.Command("replay", "Create and start a new replay")
	handleCreateReplayFlags(createReplayCmd, opts)

	archiveCmd := batchCmd.Command("archive", "Archive a batch resource")
	archiveCmd.Command("replay", "Archive a Replay")
	handleArchiveReplayFlags(archiveCmd, opts)

	// Search
	searchCmd := batchCmd.Command("search", "Search A Collection")
	searchCmd.Flag("query", "Search a specified collection").
		Default("*").
		StringVar(&opts.Batch.Query)

	searchCmd.Flag("collection-id", "Collection ID").
		StringVar(&opts.Batch.CollectionID)

	searchCmd.Flag("page", "Page of search results to display. A page is 25 results").
		IntVar(&opts.Batch.Page)
}

func handleListFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("output", "Output type").
		Default("table").
		EnumVar(&opts.Batch.OutputType, "table", "json")
}

func handleArchiveReplayFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("replay-id", "Replay ID").
		Required().
		StringVar(&opts.Batch.ReplayID)
}

func handleCreateReplayFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("name", "Name of replay").
		Required().
		StringVar(&opts.Batch.ReplayName)

	cmd.Flag("type", "Type of replay").
		Default("single").
		EnumVar(&opts.Batch.ReplayType, "single", "continuous")

	cmd.Flag("notes", "Notes to store on this replay").
		StringVar(&opts.Batch.Notes)

	cmd.Flag("collection-id", "Collection ID to replay from").
		Required().
		StringVar(&opts.Batch.CollectionID)

	cmd.Flag("destination-id", "ID of destination to replay to").
		Required().
		StringVar(&opts.Batch.DestinationID)

	cmd.Flag("query", "Search query").
		Required().
		StringVar(&opts.Batch.Query)

	cmd.Flag("from-timestamp", "RFC3339 timestamp for where to begin event search query").
		Required().
		StringVar(&opts.Batch.ReplayFrom)

	cmd.Flag("to-timestamp", "RFC3339 timestamp for where to end event search query").
		Required().
		StringVar(&opts.Batch.ReplayTo)
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

	cmd.Flag("topic", "Kafka topic").
		Required().
		StringVar(&opts.Batch.DestinationMetadata.KafkaTopic)

	cmd.Flag("address", "Kafka server Address and Port. (Ex: msg.domain.com:9092)").
		Required().
		StringVar(&opts.Batch.DestinationMetadata.KafkaAddress)

	cmd.Flag("use-tls", "Connect to kafka using TLS").
		BoolVar(&opts.Batch.DestinationMetadata.KafkaUseTLS)

	cmd.Flag("insecure-tls", "Connect to kafka but do not verify TLS").
		BoolVar(&opts.Batch.DestinationMetadata.KafkaInsecureTLS)

	cmd.Flag("sasl-type", "Kafka Authentication Type").
		EnumVar(&opts.Batch.DestinationMetadata.KafkaSASLType, "", "basic", "scram")

	cmd.Flag("username", "Kafka Username (required if --sasl-type is specified)").
		StringVar(&opts.Batch.DestinationMetadata.KafkaUsername)

	cmd.Flag("password", "Kafka Password  (required if --sasl-type is specified)").
		StringVar(&opts.Batch.DestinationMetadata.KafkaPassword)
}

func handleCreateDestinationFlagsHTTP(cmd *kingpin.CmdClause, opts *Options) {
	handleCreateDestinationFlagsShared(cmd, opts)

	cmd.Flag("url", "Full URL to HTTP server. (Ex: https://myserver.com)").
		StringVar(&opts.Batch.DestinationMetadata.HTTPURL)

	cmd.Flag("with-header", "HTTP Header to include with POST request of replayed event. You can specify "+
		"this flag multiple times for each header").
		StringMapVar(&opts.Batch.DestinationMetadata.HTTPHeaders)
}

func handleCreateDestinationFlagsSQS(cmd *kingpin.CmdClause, opts *Options) {
	handleCreateDestinationFlagsShared(cmd, opts)

	cmd.Flag("aws-account-id", "AWS Account ID containing the SQS queue to publish to").
		Required().
		StringVar(&opts.Batch.DestinationMetadata.SQSAccountID)

	cmd.Flag("queue-name", "SQS queue name to publish to").
		Required().
		StringVar(&opts.Batch.DestinationMetadata.SQSQueue)
}

func handleCreateDestinationFlagsRabbit(cmd *kingpin.CmdClause, opts *Options) {
	handleCreateDestinationFlagsShared(cmd, opts)

	cmd.Flag("dsn", "RabbitMQ DSN (Ex: amqps://server.domain.com:5672)").
		Required().
		StringVar(&opts.Batch.DestinationMetadata.RabbitDSN)

	cmd.Flag("exchange-name", "RabbitMQ Exchange to publish to").
		Required().
		StringVar(&opts.Batch.DestinationMetadata.RabbitExchangeName)

	cmd.Flag("exchange-type", "RabbitMQ Exchange Type").
		Default("topic").
		EnumVar(&opts.Batch.DestinationMetadata.RabbitExchangeType, "topic", "direct", "fanout", "headers")

	cmd.Flag("routing-key", "RabbitMQ Routing Key to publish to").
		Required().
		StringVar(&opts.Batch.DestinationMetadata.RabbitRoutingKey)

	cmd.Flag("exchange-declare", "Whether to declare the RabbitMQ exchange or not").
		BoolVar(&opts.Batch.DestinationMetadata.RabbitExchangeDeclare)

	cmd.Flag("exchange-auto-delete", "Whether to auto-delete the exchange at disconnect").
		BoolVar(&opts.Batch.DestinationMetadata.RabbitExchangeDeclare)

	cmd.Flag("exchange-durable", "Whether the RabbitMQ exchange is durable").
		BoolVar(&opts.Batch.DestinationMetadata.RabbitExchangeDeclare)
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
