package printer

import (
	"fmt"
	"strings"

	"github.com/logrusorgru/aurora"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 . IPrinter
type IPrinter interface {
	Error(str string)
	Print(str string)
}

type Printer struct {
	PrintFunc func(format string, a ...interface{}) (n int, err error)
}

func New() *Printer {
	return &Printer{
		PrintFunc: fmt.Printf,
	}
}

// Error is a convenience function for printing errors.
func (p *Printer) Error(str string) {
	p.PrintFunc("%s: %s\n", aurora.Red(">> ERROR"), str)
}

// Print is a convenience function for printing regular output.
func (p *Printer) Print(str string) {
	p.PrintFunc("%s\n", str)
}

// TODO: convert backends to use IPrinter methods
// Error is a convenience function for printing errors.
func Error(str string) {
	fmt.Printf("%s: %s\n", aurora.Red(">> ERROR"), str)
}

// TODO: convert backends to use IPrinter methods
// Print is a convenience function for printing regular output.
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

func PrintRelayOptions(cmd string, opts *cli.Options) {
	if opts == nil {
		return
	}

	// Because of some funky business with env var handling - we have to do some
	// silly things like this to get the RelayType
	relayType := opts.RelayType

	if relayType == "" {
		splitCmd := strings.Split(cmd, " ")

		if len(splitCmd) >= 2 {
			relayType = splitCmd[1]
		} else {
			relayType = "N/A"
		}
	}

	logrus.Info("----------------------------------------------------------------")
	logrus.Info("> Relay Settings")
	logrus.Info("----------------------------------------------------------------")
	logrus.Info("")
	logrus.Infof("- %-24s%-6s", "RelayType", relayType)
	logrus.Infof("- %-24s%-6s", "RelayToken", opts.RelayToken)
	logrus.Infof("- %-24s%-6s", "RelayGRPCAddress", opts.RelayGRPCAddress)
	logrus.Infof("- %-24s%-6d", "RelayNumWorkers", opts.RelayNumWorkers)
	logrus.Infof("- %-24s%-6d", "RelayBatchSize", opts.RelayBatchSize)
	logrus.Infof("- %-24s%-6v", "Stats", opts.Stats)
	logrus.Infof("- %-24s%-6s", "StatsReportInterval", opts.StatsReportInterval)
	logrus.Info("")

	switch relayType {
	case "kafka":
		printKafkaOptions(opts)
	case "rabbit":
		printRabbitOptions(opts)
	case "sqs":
		printSQSOptions(opts)
	case "azure":
		printAzureOptions(opts)
	case "gcp":
		printGCPOptions(opts)
	}
}

func printKafkaOptions(opts *cli.Options) {
	logrus.Info("----------------------------------------------------------------")
	logrus.Info("> Kafka Settings")
	logrus.Info("----------------------------------------------------------------")
	logrus.Info("")
	logrus.Infof("- %-24s%-6v", "Address", opts.Kafka.Address)
	logrus.Infof("- %-24s%-6v", "Topic", opts.Kafka.Topic)
	logrus.Infof("- %-24s%-6v", "CommitInterval", opts.Kafka.CommitInterval)
	logrus.Infof("- %-24s%-6v", "MaxWait", opts.Kafka.MaxWait)
	logrus.Infof("- %-24s%-6v", "MinBytes", opts.Kafka.MinBytes)
	logrus.Infof("- %-24s%-6v", "MaxBytes", opts.Kafka.MaxBytes)
	logrus.Info("")
}

func printRabbitOptions(opts *cli.Options) {
	logrus.Info("----------------------------------------------------------------")
	logrus.Info("> Rabbit Settings")
	logrus.Info("----------------------------------------------------------------")
	logrus.Info("")
	logrus.Infof("- %-24s%-6v", "Address", opts.Rabbit.Address)
	logrus.Infof("- %-24s%-6v", "Exchange", opts.Rabbit.Exchange)
	logrus.Infof("- %-24s%-6v", "RoutingKey", opts.Rabbit.RoutingKey)
	logrus.Infof("- %-24s%-6v", "RoutingKey", opts.Rabbit.ReadQueue)
	logrus.Infof("- %-24s%-6v", "ReadQueueDurable", opts.Rabbit.ReadQueueDurable)
	logrus.Infof("- %-24s%-6v", "ReadQueueAutoDelete", opts.Rabbit.ReadQueueAutoDelete)
	logrus.Infof("- %-24s%-6v", "ReadQueueDeclare", opts.Rabbit.ReadQueueDeclare)
	logrus.Infof("- %-24s%-6v", "ReadQueueExclusive", opts.Rabbit.ReadQueueExclusive)
	logrus.Infof("- %-24s%-6v", "ReadAutoAck", opts.Rabbit.ReadAutoAck)
	logrus.Infof("- %-24s%-6v", "ReadConsumerTag", opts.Rabbit.ReadConsumerTag)
	logrus.Infof("- %-24s%-6v", "UseTLS", opts.Rabbit.UseTLS)
	logrus.Infof("- %-24s%-6v", "SkipVerifyTLS", opts.Rabbit.SkipVerifyTLS)
	logrus.Info("")
}

func printAzureOptions(opts *cli.Options) {
	logrus.Info("----------------------------------------------------------------")
	logrus.Info("> Azure Settings")
	logrus.Info("----------------------------------------------------------------")
	logrus.Info("")
	logrus.Infof("- %-24s%-6v", "Subscription", opts.Azure.Subscription)
	logrus.Infof("- %-24s%-6v", "Queue", opts.Azure.Queue)
	logrus.Infof("- %-24s%-6v", "Topic", opts.Azure.Topic)
	logrus.Infof("- %-24s%-6v", "ConnectionString", opts.Azure.ConnectionString)
	logrus.Info("")
}

func printSQSOptions(opts *cli.Options) {
	logrus.Info("----------------------------------------------------------------")
	logrus.Info("> SQS Settings")
	logrus.Info("----------------------------------------------------------------")
	logrus.Info("")
	logrus.Infof("- %-24s%-6v", "QueueName", opts.AWSSQS.QueueName)
	logrus.Infof("- %-24s%-6v", "RemoteAccountID", opts.AWSSQS.RemoteAccountID)
	logrus.Infof("- %-24s%-6v", "RelayMaxNumMessages", opts.AWSSQS.RelayMaxNumMessages)
	logrus.Infof("- %-24s%-6v", "ReceiveRequestAttemptID", opts.AWSSQS.RelayReceiveRequestAttemptId)
	logrus.Infof("- %-24s%-6v", "AutoDelete", opts.AWSSQS.RelayAutoDelete)
	logrus.Infof("- %-24s%-6v", "WaitTimeSeconds", opts.AWSSQS.RelayWaitTimeSeconds)
	logrus.Info("")
}

func printGCPOptions(opts *cli.Options) {
	logrus.Info("----------------------------------------------------------------")
	logrus.Info("> GCP Settings")
	logrus.Info("----------------------------------------------------------------")
	logrus.Info("")
	logrus.Infof("- %-24s%-6v", "ProjectID", opts.GCPPubSub.ProjectId)
	logrus.Infof("- %-24s%-6v", "SubscriptionID", opts.GCPPubSub.ReadSubscriptionId)
	logrus.Infof("- %-24s%-6v", "ReadAck", opts.GCPPubSub.ReadAck)
	logrus.Info("")
}
