package rabbitmq

import (
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/rabbit"
	"github.com/jhump/protoreflect/desc"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

// RabbitMQ holds all attributes required for performing a read/write operations
// in RabbitMQ. This struct should be instantiated via the rabbitmq.Read(..) or
// rabbitmq.Write(..) functions.
type RabbitMQ struct {
	Options  *cli.Options
	Channel  *amqp.Channel
	Consumer *rabbit.Rabbit
	MsgDesc  *desc.MessageDescriptor
	log      *logrus.Entry
}
