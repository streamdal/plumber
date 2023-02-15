package printer

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/hokaccha/go-prettyjson"
	"github.com/logrusorgru/aurora"
	"github.com/olekukonko/tablewriter"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

func Error(str string) {
	fmt.Printf("%s: %s\n", aurora.Red(">> ERROR"), str)
}

func Errorf(str string, args ...interface{}) {
	args = append([]interface{}{aurora.Red(">> ERROR")}, args...)
	fmt.Printf("%s: "+str+"\n", args...)
}

func Print(str string) {
	fmt.Printf("%s\n", str)
}

func PrintLogo() {
	logo := `
█▀█ █   █ █ █▀▄▀█ █▄▄ █▀▀ █▀█
█▀▀ █▄▄ █▄█ █ ▀ █ █▄█ ██▄ █▀▄
`

	logrus.Info(logo)
}

func PrintTable(cliOpts *opts.CLIOptions, count int64, receivedAt time.Time, data []byte, properties [][]string) {
	pretty := displayPrettyOutput(cliOpts)

	if displayJSONOutput(cliOpts) {
		displayJSON(count, receivedAt, data, properties, pretty)
	} else {
		displayTabular(count, receivedAt, data, properties, pretty)
	}
}

func displayPrettyOutput(cliOpts *opts.CLIOptions) bool {
	if cliOpts == nil {
		return false
	}

	if cliOpts.Read == nil {
		return false
	}

	if cliOpts.Read.XCliOptions == nil {
		return false
	}

	return cliOpts.Read.XCliOptions.Pretty
}

type JSONOutput struct {
	Count      int64      `json:"count"`
	ReceivedAt time.Time  `json:"received_at"`
	Data       string     `json:"data"`
	Properties [][]string `json:"properties"`
}

func displayJSON(count int64, receivedAt time.Time, data []byte, properties [][]string, pretty bool) {
	output := &JSONOutput{
		Count:      count,
		ReceivedAt: receivedAt,
		Data:       string(data),
		Properties: properties,
	}

	data, err := json.Marshal(output)
	if err != nil {
		logrus.Errorf("unable to marshal output as JSON: %s", err)
	}

	if pretty {
		colorized, err := prettyjson.Format(data)
		if err != nil {
			Error(fmt.Sprintf("unable to colorize JSON output: %s", err))
		} else {
			data = colorized
		}
	}

	fmt.Println(string(data))
}

func displayTabular(count int64, receivedAt time.Time, data []byte, properties [][]string, pretty bool) {
	fullHeader := fmt.Sprintf("\n------------- [Count: %d Received at: %s] -------------------\n\n",
		aurora.Cyan(count), aurora.Yellow(receivedAt.Format(time.RFC3339)).String())

	minimalHeader := fmt.Sprintf("\n------------- [Received at: %s] -------------------\n\n",
		aurora.Yellow(receivedAt.Format(time.RFC3339)).String())

	if count == 0 && data == nil {
		fmt.Print(minimalHeader)
	} else {
		fmt.Print(fullHeader)
	}

	tableString := &strings.Builder{}

	table := tablewriter.NewWriter(tableString)

	if len(properties) > 0 {
		for _, row := range properties {
			if len(row) == 2 {
				if row[1] == "" {
					table.Append([]string{row[0], aurora.Gray(12, "NONE").String()})
					continue
				}
			}

			table.Append(row)
		}
	}

	table.SetColMinWidth(0, 22)
	table.SetColMinWidth(1, 60)
	// First column align left, second column align right
	table.SetColumnAlignment([]int{tablewriter.ALIGN_LEFT, tablewriter.ALIGN_RIGHT})
	table.Render()

	if len(properties) > 0 {
		fmt.Println(tableString.String())
	}

	if pretty {
		colorized, err := prettyjson.Format(data)
		if err != nil {
			Error(fmt.Sprintf("unable to colorize JSON output: %s", err))
		} else {
			data = colorized
		}
	}

	// Display value
	if len(data) != 0 {
		Print(string(data))
	}

}

// Helper for determining if plumber is set to display output as JSON
func displayJSONOutput(cliOpts *opts.CLIOptions) bool {
	if cliOpts == nil {
		return false
	}

	if cliOpts.Read == nil {
		return false
	}

	if cliOpts.Read.XCliOptions == nil {
		return false
	}

	return cliOpts.Read.XCliOptions.Json
}

// PrintTableProperties prints only properties (no data or count)
func PrintTableProperties(properties [][]string, timestamp time.Time) {
	PrintTable(nil, 0, timestamp, nil, properties)
}

func DefaultDisplayError(msg *records.ErrorRecord) {
	humanReadable := time.Unix(msg.OccurredAtUnixTsUtc, 0)
	Errorf("[%s] %s", humanReadable, msg.Error)
}

func PrintRelayOptions(cliOpts *opts.CLIOptions) {
	if cliOpts == nil {
		return
	}

	logrus.Info("----------------------------------------------------------------")
	logrus.Info("> Relay Settings")
	logrus.Info("----------------------------------------------------------------")
	logrus.Info("")
	logrus.Infof("- %-30s%-6s", "RelayType", cliOpts.Global.XBackend)
	logrus.Infof("- %-30s%-6s", "Collection token", cliOpts.Relay.CollectionToken)
	logrus.Infof("- %-30s%-6s", "Streamdal collector address", cliOpts.Relay.XStreamdalGrpcAddress)
	logrus.Infof("- %-30s%-6d", "Num workers", cliOpts.Relay.NumWorkers)
	logrus.Infof("- %-30s%-6d", "Batch size", cliOpts.Relay.BatchSize)
	logrus.Infof("- %-30s%-6v", "Stats enabled", cliOpts.Relay.StatsEnable)
	logrus.Infof("- %-30s%-6s", "Stats report interval", fmt.Sprintf("%d seconds", cliOpts.Relay.StatsReportIntervalSec))
	logrus.Info("")

	switch cliOpts.Global.XBackend {
	case "kafka":
		printKafkaOptions(cliOpts)
	case "rabbit":
		printRabbitOptions(cliOpts)
	case "aws-sqs":
		printSQSOptions(cliOpts)
	case "azure":
		printAzureOptions(cliOpts)
	case "gcp-pubsub":
		printGCPOptions(cliOpts)
	case "redis-pubsub":
		printRedisPubSubOptions(cliOpts)
	case "redis-streams":
		printRedisStreamsOptions(cliOpts)
	case "nsq":
		printNSQOptions(cliOpts)
	}
}

func printKafkaOptions(cliOpts *opts.CLIOptions) {
	logrus.Info("----------------------------------------------------------------")
	logrus.Info("> Kafka Settings")
	logrus.Info("----------------------------------------------------------------")
	logrus.Info("")
	logrus.Infof("- %-25s%-6v", "Brokers", strings.Join(cliOpts.Relay.Kafka.XConn.Address, ", "))
	logrus.Infof("- %-25s%-6v", "Topics", strings.Join(cliOpts.Relay.Kafka.Args.Topics, ", "))
	logrus.Infof("- %-25s%-6v", "ConsumerGroup", cliOpts.Relay.Kafka.Args.ConsumerGroupName)
	logrus.Infof("- %-25s%-6v", "CommitInterval (seconds)", cliOpts.Relay.Kafka.Args.CommitIntervalSeconds)
	logrus.Infof("- %-25s%-6v", "MaxWait (seconds)", cliOpts.Relay.Kafka.Args.MaxWaitSeconds)
	logrus.Infof("- %-25s%-6v", "MinBytes", cliOpts.Relay.Kafka.Args.MinBytes)
	logrus.Infof("- %-25s%-6v", "MaxBytes", cliOpts.Relay.Kafka.Args.MaxBytes)
	logrus.Info("")
}

func printRabbitOptions(cliOpts *opts.CLIOptions) {
	logrus.Info("----------------------------------------------------------------")
	logrus.Info("> Rabbit Settings")
	logrus.Info("----------------------------------------------------------------")
	logrus.Info("")
	logrus.Infof("- %-24s%-6v", "Address", cliOpts.Relay.Rabbit.XConn.Address)
	logrus.Infof("- %-24s%-6v", "Exchange", cliOpts.Relay.Rabbit.Args.ExchangeName)
	logrus.Infof("- %-24s%-6v", "BindingKey", cliOpts.Relay.Rabbit.Args.BindingKey)
	logrus.Infof("- %-24s%-6v", "ReadQueue", cliOpts.Relay.Rabbit.Args.QueueName)
	logrus.Infof("- %-24s%-6v", "ReadQueueDurable", cliOpts.Relay.Rabbit.Args.QueueDurable)
	logrus.Infof("- %-24s%-6v", "ReadQueueAutoDelete", cliOpts.Relay.Rabbit.Args.QueueDelete)
	logrus.Infof("- %-24s%-6v", "ReadQueueDeclare", cliOpts.Relay.Rabbit.Args.QueueDeclare)
	logrus.Infof("- %-24s%-6v", "ReadQueueExclusive", cliOpts.Relay.Rabbit.Args.QueueExclusive)
	logrus.Infof("- %-24s%-6v", "ReadAutoAck", cliOpts.Relay.Rabbit.Args.AutoAck)
	logrus.Infof("- %-24s%-6v", "ReadConsumerTag", cliOpts.Relay.Rabbit.Args.ConsumerTag)
	logrus.Infof("- %-24s%-6v", "UseTLS", cliOpts.Relay.Rabbit.XConn.UseTls)
	logrus.Infof("- %-24s%-6v", "SkipVerifyTLS", cliOpts.Relay.Rabbit.XConn.TlsSkipVerify)
	logrus.Info("")
}

func printAzureOptions(cliOpts *opts.CLIOptions) {
	logrus.Info("----------------------------------------------------------------")
	logrus.Info("> Azure Settings")
	logrus.Info("----------------------------------------------------------------")
	logrus.Info("")
	logrus.Infof("- %-24s%-6v", "Subscription", cliOpts.Relay.AzureServiceBus.Args.SubscriptionName)
	logrus.Infof("- %-24s%-6v", "queue", cliOpts.Relay.AzureServiceBus.Args.Queue)
	logrus.Infof("- %-24s%-6v", "topic", cliOpts.Relay.AzureServiceBus.Args.Topic)
	logrus.Infof("- %-24s%-6v", "ConnectionString", cliOpts.Relay.AzureServiceBus.XConn.ConnectionString)
	logrus.Info("")
}

func printSQSOptions(cliOpts *opts.CLIOptions) {
	logrus.Info("----------------------------------------------------------------")
	logrus.Info("> SQS Settings")
	logrus.Info("----------------------------------------------------------------")
	logrus.Info("")
	logrus.Infof("- %-24s%-6v", "QueueName", cliOpts.Relay.AwsSqs.Args.QueueName)
	logrus.Infof("- %-24s%-6v", "RemoteAccountID", cliOpts.Relay.AwsSqs.Args.RemoteAccountId)
	logrus.Infof("- %-24s%-6v", "RelayMaxNumMessages", cliOpts.Relay.AwsSqs.Args.MaxNumMessages)
	logrus.Infof("- %-24s%-6v", "ReceiveRequestAttemptID", cliOpts.Relay.AwsSqs.Args.ReceiveRequestAttemptId)
	logrus.Infof("- %-24s%-6v", "AutoDelete", cliOpts.Relay.AwsSqs.Args.AutoDelete)
	logrus.Infof("- %-24s%-6v", "WaitTimeSeconds", cliOpts.Relay.AwsSqs.Args.WaitTimeSeconds)
	logrus.Info("")
}

func printGCPOptions(cliOpts *opts.CLIOptions) {
	logrus.Info("----------------------------------------------------------------")
	logrus.Info("> GCP Settings")
	logrus.Info("----------------------------------------------------------------")
	logrus.Info("")
	logrus.Infof("- %-24s%-6v", "ProjectID", cliOpts.Relay.GcpPubsub.XConn.ProjectId)
	logrus.Infof("- %-24s%-6v", "SubscriptionID", cliOpts.Relay.GcpPubsub.Args.SubscriptionId)
	logrus.Infof("- %-24s%-6v", "ReadAck", cliOpts.Relay.GcpPubsub.Args.AckMessages)
	logrus.Info("")
}

func printRedisPubSubOptions(cliOpts *opts.CLIOptions) {
	logrus.Info("----------------------------------------------------------------")
	logrus.Info("> Redis PubSub Settings")
	logrus.Info("----------------------------------------------------------------")
	logrus.Info("")
	logrus.Infof("- %-24s%-6v", "Address", cliOpts.Relay.RedisPubsub.XConn.Address)
	logrus.Infof("- %-24s%-6v", "Channels", cliOpts.Relay.RedisPubsub.Args.Channels)
	logrus.Infof("- %-24s%-6v", "Username", cliOpts.Relay.RedisPubsub.XConn.Username)
	logrus.Infof("- %-24s%-6v", "Database", cliOpts.Relay.RedisPubsub.XConn.Database)
	logrus.Info("")
}

func printRedisStreamsOptions(cliOpts *opts.CLIOptions) {
	logrus.Info("----------------------------------------------------------------")
	logrus.Info("> Redis Streams Settings")
	logrus.Info("----------------------------------------------------------------")
	logrus.Info("")
	logrus.Infof("- %-28s%-6v", "Address", cliOpts.Relay.RedisStreams.XConn.Address)
	logrus.Infof("- %-28s%-6v", "Streams", cliOpts.Relay.RedisStreams.Args.Streams)
	logrus.Infof("- %-28s%-6v", "Username", cliOpts.Relay.RedisStreams.XConn.Username)
	logrus.Infof("- %-28s%-6v", "Database", cliOpts.Relay.RedisStreams.XConn.Database)
	logrus.Infof("- %-28s%-6v", "Create Streams", cliOpts.Relay.RedisStreams.Args.CreateConsumerConfig.CreateStreams)
	logrus.Infof("- %-28s%-6v", "consumer Name", cliOpts.Relay.RedisStreams.Args.ConsumerName)
	logrus.Infof("- %-28s%-6v", "consumer Group", cliOpts.Relay.RedisStreams.Args.ConsumerGroup)
	logrus.Infof("- %-28s%-6v", "Recreate consumer Group", cliOpts.Relay.RedisStreams.Args.CreateConsumerConfig.RecreateConsumerGroup)
	logrus.Info("")
}

func printNSQOptions(cliOpts *opts.CLIOptions) {
	logrus.Info("----------------------------------------------------------------")
	logrus.Info("> NSQ Settings")
	logrus.Info("----------------------------------------------------------------")
	logrus.Info("")
	if cliOpts.Relay.Nsq.XConn.LookupdAddress != "" {
		logrus.Infof("- %-24s%-6v", "Address", cliOpts.Relay.Nsq.XConn.LookupdAddress)
	} else {
		logrus.Infof("- %-24s%-6v", "Address", cliOpts.Relay.Nsq.XConn.NsqdAddress)
	}
	logrus.Infof("- %-24s%-6v", "topic", cliOpts.Relay.Nsq.Args.Topic)
	logrus.Infof("- %-24s%-6v", "OutputChannel", cliOpts.Relay.Nsq.Args.Channel)
	logrus.Info("")
}
