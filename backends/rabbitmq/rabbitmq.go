package rabbitmq

import (
	"context"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/rabbit"

	"github.com/streamdal/plumber/types"
	"github.com/streamdal/plumber/validate"
)

const (
	BackendName = "rabbitmq"
)

var (
	ErrEmptyRoutingKey   = errors.New("routing key cannot be empty")
	ErrEmptyExchangeName = errors.New("exchange name cannot be empty")
	ErrEmptyQueueName    = errors.New("queue name cannot be empty")
	ErrEmptyBindingKey   = errors.New("binding key cannot be empty")
)

// RabbitMQ holds all attributes required for performing a read/write operations
// in RabbitMQ. This struct should be instantiated via the rabbitmq.Read(..) or
// rabbitmq.Write(..) functions.
type RabbitMQ struct {
	connOpts *opts.ConnectionOptions
	connArgs *args.RabbitConn
	client   rabbit.IRabbit
	log      *logrus.Entry
}

func New(opts *opts.ConnectionOptions) (*RabbitMQ, error) {
	if err := validateBaseConnOpts(opts); err != nil {
		return nil, errors.Wrap(err, "invalid connection options")
	}

	connArgs := opts.GetRabbit()

	r := &RabbitMQ{
		connOpts: opts,
		connArgs: connArgs,
		log:      logrus.WithField("backend", BackendName),
	}

	return r, nil
}

func (r *RabbitMQ) newRabbitForRead(readArgs *args.RabbitReadArgs) (rabbit.IRabbit, error) {
	rmq, err := rabbit.New(&rabbit.Options{
		URLs:      []string{r.connArgs.Address},
		QueueName: readArgs.QueueName,
		Bindings: []rabbit.Binding{
			{
				BindingKeys:  []string{readArgs.BindingKey},
				ExchangeName: readArgs.ExchangeName,
			},
		},
		QueueExclusive: readArgs.QueueExclusive,
		QueueDurable:   readArgs.QueueDurable,
		QueueDeclare:   readArgs.QueueDeclare,
		AutoAck:        readArgs.AutoAck,
		ConsumerTag:    readArgs.ConsumerTag,
		UseTLS:         r.connArgs.UseTls,
		SkipVerifyTLS:  r.connArgs.TlsSkipVerify,
		Mode:           rabbit.Consumer,
		QueueArgs:      mapToTable(readArgs.QueueArg),
	})

	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize rabbitmq consumer")
	}

	return rmq, nil
}

func (r *RabbitMQ) newRabbitForWrite(writeArgs *args.RabbitWriteArgs) (rabbit.IRabbit, error) {
	rmq, err := rabbit.New(&rabbit.Options{
		URLs: []string{r.connArgs.Address},
		Bindings: []rabbit.Binding{
			{
				ExchangeName:       writeArgs.ExchangeName,
				ExchangeDeclare:    writeArgs.ExchangeDeclare,
				ExchangeDurable:    writeArgs.ExchangeDurable,
				ExchangeAutoDelete: writeArgs.ExchangeAutoDelete,
				ExchangeType:       writeArgs.ExchangeType,
				BindingKeys:        []string{writeArgs.RoutingKey},
			},
		},
		AppID:         writeArgs.AppId,
		UseTLS:        r.connArgs.UseTls,
		SkipVerifyTLS: r.connArgs.TlsSkipVerify,
		Mode:          rabbit.Producer,
	})

	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize rabbitmq consumer")
	}

	return rmq, nil
}

func (r *RabbitMQ) Name() string {
	return BackendName
}

func (r *RabbitMQ) Close(_ context.Context) error {
	// Since these are instantiated inside the Write/Read/Relay methods,
	// there is chance that r.client might be nil
	if r.client != nil {
		return r.client.Close()
	}
	return nil
}

func (r *RabbitMQ) Test(_ context.Context) error {
	return types.NotImplementedErr
}

func validateBaseConnOpts(connOpts *opts.ConnectionOptions) error {
	if connOpts == nil {
		return validate.ErrMissingConnOpts
	}

	if connOpts.Conn == nil {
		return validate.ErrMissingConnCfg
	}

	args := connOpts.GetRabbit()
	if args == nil {
		return validate.ErrMissingConnArgs
	}

	if args.Address == "" {
		return validate.ErrMissingAddress
	}

	return nil
}

// mapToTable converts kong map[string]string to map[string]interface{}, which is the expected
// type of args for rabbit lib
func mapToTable(m map[string]string) map[string]interface{} {
	out := make(map[string]interface{})

	if m == nil {
		return out
	}

	for k, v := range m {
		out[k] = v
	}

	return out
}
