package cli

import "gopkg.in/alecthomas/kingpin.v2"

type AWSSNSOptions struct {
	TopicArn string
}

func HandleAWSSNSFlags(_, writeCmd, _ *kingpin.CmdClause, opts *Options) {
	// AWS-SNS write cmd
	wc := writeCmd.Command("aws-sns", "AWS Simple Notification System")
	addWriteAWSSNSFlags(wc, opts)
}

func addWriteAWSSNSFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("topic", "Topic ARN").
		StringVar(&opts.AWSSNS.TopicArn)
}
