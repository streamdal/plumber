package printer

import (
	"fmt"
	"strings"
	"time"

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

func Printf(str string, args ...interface{}) {
	fmt.Printf(str+"\n", args...)
}

func PrintLogo() {
	logo := `
█▀█ █   █ █ █▀▄▀█ █▄▄ █▀▀ █▀█
█▀▀ █▄▄ █▄█ █ ▀ █ █▄█ ██▄ █▀▄
`

	logrus.Info(logo)
}

func PrintTable(properties [][]string, count int64, receivedAt time.Time, data []byte) {
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

	table.SetColMinWidth(0, 20)
	table.SetColMinWidth(1, 40)
	// First column align left, second column align right
	table.SetColumnAlignment([]int{tablewriter.ALIGN_LEFT, tablewriter.ALIGN_RIGHT})
	table.Render()

	fmt.Println(tableString.String())

	// Display value
	if len(data) != 0 {
		Print(string(data))
	}
}

// PrintTableProperties prints only properties (no data or count)
func PrintTableProperties(properties [][]string, timestamp time.Time) {
	PrintTable(properties, 0, timestamp, nil)
}

func DefaultDisplayError(msg *records.ErrorRecord) {
	humanReadable := time.Unix(msg.OccurredAtUnixTsUtc, 0)
	Errorf("[%s] %s", humanReadable, msg.Error)
}

func PrintRelayOptions(cliOpts *opts.CLIOptions) {
	if cliOpts == nil {
		return
	}

	// TODO: Don't think this is needed since moving to kong ~ds 09.11.21
	//// Because of some funky business with env var handling - we have to do some
	//// silly things like this to get the RelayType
	//relayType := cliOpts.Relay.Type
	//
	//if relayType == "" {
	//	splitCmd := strings.Split(cmd, " ")
	//
	//	if len(splitCmd) >= 2 {
	//		relayType = splitCmd[1]
	//	} else {
	//		relayType = "N/A"
	//	}
	//}

	logrus.Info("----------------------------------------------------------------")
	logrus.Info("> Relay Settings")
	logrus.Info("----------------------------------------------------------------")
	logrus.Info("")
	logrus.Infof("- %-24s%-6s", "RelayType", cliOpts.Global.XBackend)
	logrus.Infof("- %-24s%-6s", "Collection token", cliOpts.Relay.CollectionToken)
	logrus.Infof("- %-24s%-6s", "Batch collector address", cliOpts.Relay.XBatchshGrpcAddress)
	logrus.Infof("- %-24s%-6d", "Num workers", cliOpts.Relay.NumWorkers)
	logrus.Infof("- %-24s%-6d", "Batch size", cliOpts.Relay.BatchSize)
	logrus.Infof("- %-24s%-6v", "Stats enabled", cliOpts.Read.XCliOptions.StatsEnable)
	logrus.Infof("- %-24s%-6d", "Stats report interval (seconds)", cliOpts.Read.XCliOptions.StatsReportIntervalSec)
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
	logrus.Infof("- %-24s%-6v", "Brokers", strings.Join(cliOpts.Relay.Kafka.XConn.Address, ", "))
	logrus.Infof("- %-24s%-6v", "Topics", strings.Join(cliOpts.Relay.Kafka.Args.Topics, ", "))
	logrus.Infof("- %-24s%-6v", "ConsumerGroup", cliOpts.Relay.Kafka.Args.ConsumerGroupName)
	logrus.Infof("- %-24s%-6v", "CommitInterval (seconds)", cliOpts.Relay.Kafka.Args.CommitIntervalSeconds)
	logrus.Infof("- %-24s%-6v", "MaxWait (seconds)", cliOpts.Relay.Kafka.Args.MaxWaitSeconds)
	logrus.Infof("- %-24s%-6v", "MinBytes", cliOpts.Relay.Kafka.Args.MinBytes)
	logrus.Infof("- %-24s%-6v", "MaxBytes", cliOpts.Relay.Kafka.Args.MaxBytes)
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
	logrus.Infof("- %-24s%-6v", "SkipVerifyTLS", cliOpts.Relay.Rabbit.XConn.InsecureTls)
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
	logrus.Infof("- %-24s%-6v", "QueueName", cliOpts.Relay.Awssqs.Args.QueueName)
	logrus.Infof("- %-24s%-6v", "RemoteAccountID", cliOpts.Relay.Awssqs.Args.RemoteAccountId)
	logrus.Infof("- %-24s%-6v", "RelayMaxNumMessages", cliOpts.Relay.Awssqs.Args.MaxNumMessages)
	logrus.Infof("- %-24s%-6v", "ReceiveRequestAttemptID", cliOpts.Relay.Awssqs.Args.ReceiveRequestAttemptId)
	logrus.Infof("- %-24s%-6v", "AutoDelete", cliOpts.Relay.Awssqs.Args.AutoDelete)
	logrus.Infof("- %-24s%-6v", "WaitTimeSeconds", cliOpts.Relay.Awssqs.Args.WaitTimeSeconds)
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
	logrus.Infof("- %-24s%-6v", "Channels", cliOpts.Relay.RedisPubsub.Args.Channel)
	logrus.Infof("- %-24s%-6v", "Username", cliOpts.Relay.RedisPubsub.XConn.Username)
	logrus.Infof("- %-24s%-6v", "Database", cliOpts.Relay.RedisPubsub.Args.Database)
	logrus.Info("")
}

func printRedisStreamsOptions(cliOpts *opts.CLIOptions) {
	logrus.Info("----------------------------------------------------------------")
	logrus.Info("> Redis Streams Settings")
	logrus.Info("----------------------------------------------------------------")
	logrus.Info("")
	logrus.Infof("- %-28s%-6v", "Address", cliOpts.Relay.RedisStreams.XConn.Address)
	logrus.Infof("- %-28s%-6v", "Streams", cliOpts.Relay.RedisStreams.Args.Stream)
	logrus.Infof("- %-28s%-6v", "Username", cliOpts.Relay.RedisStreams.XConn.Username)
	logrus.Infof("- %-28s%-6v", "Database", cliOpts.Relay.RedisStreams.Args.Database)
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
