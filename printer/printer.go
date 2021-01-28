package printer

import (
	"fmt"
	"strings"

	"github.com/logrusorgru/aurora"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
)

// Error is a convenience function for printing errors.
func Error(str string) {
	fmt.Printf("%s: %s\n", aurora.Red(">> ERROR"), str)
}

// Print is a convenience function for printing regular output.
func Print(str string) {
	fmt.Printf("%s\n", str)
}

func PrintLogo() {
	logo := `
█▀█ █   █ █ █▀▄▀█ █▄▄ █▀▀ █▀█
█▀▀ █▄▄ █▄█ █ ▀ █ █▄█ ██▄ █▀▄
`

	fmt.Println(logo)
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
