package nats_jetstream

import (
	"context"
	"crypto/tls"
	"net/url"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
)

const BackendName = "nats-jetstream"

var (
	ErrMissingStream  = errors.New("--stream cannot be empty")
	ErrMissingSubject = errors.New("--subject cannot be empty")
)

type NatsJetstream struct {
	connOpts *opts.ConnectionOptions
	client   *nats.Conn
	log      *logrus.Entry
}

func New(connOpts *opts.ConnectionOptions) (*NatsJetstream, error) {
	if err := validateBaseConnOpts(connOpts); err != nil {
		return nil, errors.Wrap(err, "invalid connection options")
	}

	args := connOpts.GetNatsJetstream()

	uri, err := url.Parse(args.Dsn)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse address")
	}

	// Credentials can be specified by a .creds file if users do not wish to pass in with the DSN
	var creds nats.Option
	if len(args.UserCredentials) > 0 {
		if util.FileExists(args.UserCredentials) {
			creds = nats.UserCredentials(string(args.UserCredentials))
		} else {
			creds = func(o *nats.Options) error {
				o.UserJWT = func() (string, error) {
					return string(args.UserCredentials), nil
				}
				o.SignatureCB = nil
				return nil
			}
		}
	}

	var client *nats.Conn
	if uri.Scheme == "tls" || args.TlsOptions.UseTls {
		// TLS Secured connection
		tlsConfig, err := util.GenerateTLSConfig(
			args.TlsOptions.TlsCaCert,
			args.TlsOptions.TlsClientCert,
			args.TlsOptions.TlsClientKey,
			args.TlsOptions.TlsSkipVerify,
			tls.NoClientCert,
		)
		if err != nil {
			return nil, errors.Wrap(err, "Unable to generate TLS Config")
		}

		client, err = nats.Connect(args.Dsn, nats.Secure(tlsConfig), creds)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create new nats client")
		}
	} else {
		// Plaintext connection
		client, err = nats.Connect(args.Dsn, creds)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create new nats client")
		}
	}

	return &NatsJetstream{
		connOpts: connOpts,
		client:   client,
		log:      logrus.WithField("backend", BackendName),
	}, nil
}

func (n *NatsJetstream) Name() string {
	return BackendName
}

func (n *NatsJetstream) Close(_ context.Context) error {
	n.client.Close()
	return nil
}

func (n *NatsJetstream) Test(_ context.Context) error {
	return types.NotImplementedErr
}

func (n *NatsJetstream) validateExistingConsumerConfig(readArgs *args.NatsJetstreamReadArgs, config *nats.ConsumerConfig) error {
	if readArgs == nil || config == nil {
		return errors.New("readArgs and config cannot be nil")
	}

	if !readArgs.ExistingDurableConsumer {
		return errors.New("expected existing durable consumer to be enabled - bug?")
	}

	// Consumer should be configured to use deliverByStartSeq
	if readArgs.ConsumerStartSequence != 0 {
		if config.DeliverPolicy != nats.DeliverByStartSequencePolicy {
			return errors.New("existing consumer's deliver policy is not set to DeliverByStartSequence (tip: do not use existing consumer)")
		}

		return nil
	}

	if readArgs.ConsumerStartTime != "" {
		if config.DeliverPolicy != nats.DeliverByStartTimePolicy {
			return errors.New("existing consumer's deliver policy is not set to DeliverByStartTime (tip: do not use existing consumer)")
		}
	}

	return nil
}

func (n *NatsJetstream) createConsumer(ctx nats.JetStreamContext, args *args.NatsJetstreamReadArgs) (*nats.ConsumerInfo, error) {
	if args == nil || ctx == nil {
		return nil, errors.New("both ctx and args cannot be nil")
	}

	if !args.CreateDurableConsumer && !args.ExistingDurableConsumer {
		return nil, errors.New("durable consumer usage not enabled - nothing to do")
	}

	if args.ExistingDurableConsumer {
		if args.ConsumerName == "" {
			return nil, errors.New("consumer name must be specified when existing consumer is enabled")
		}

		consumerInfo, err := ctx.ConsumerInfo(args.Stream, args.ConsumerName)
		if err != nil {
			return nil, errors.Wrap(err, "unable to fetch existing consumer")
		}

		if err := n.validateExistingConsumerConfig(args, &consumerInfo.Config); err != nil {
			return nil, errors.Wrap(err, "unable to validate existing consumer config")
		}

		return consumerInfo, nil
	}

	filterSubject := args.ConsumerFilterSubject

	if filterSubject == "" {
		filterSubject = args.Stream
	}

	consumerCfg := &nats.ConsumerConfig{
		Durable:       getConsumerName(args.ConsumerName),
		Description:   "plumber consumer",
		OptStartSeq:   uint64(args.ConsumerStartSequence),
		FilterSubject: filterSubject,
		AckPolicy:     nats.AckExplicitPolicy,
		DeliverPolicy: nats.DeliverLastPolicy,
	}

	// Which delivery policy should we use?
	if args.ConsumerStartSequence != 0 {
		consumerCfg.DeliverPolicy = nats.DeliverByStartSequencePolicy
	} else if args.ConsumerStartTime != "" {
		t, err := time.Parse(time.RFC3339, args.ConsumerStartTime)
		if err != nil {
			return nil, errors.Wrap(err, "unable to parse start time")
		}

		consumerCfg.DeliverPolicy = nats.DeliverByStartTimePolicy
		consumerCfg.OptStartTime = &t
	}

	consumerInfo, err := ctx.AddConsumer(args.Stream, consumerCfg)
	if err != nil {
		return nil, errors.Wrap(err, "unable to add consumer")
	}

	n.log.Debugf("Created durable consumer '%s'", consumerInfo.Name)

	return consumerInfo, nil
}

// If name is empty, generate a random'ish name; otherwise use provided
func getConsumerName(name string) string {
	if name == "" {
		return "plumber-" + util.RandomString(8)
	}

	return name
}

func validateBaseConnOpts(connOpts *opts.ConnectionOptions) error {
	if connOpts == nil {
		return validate.ErrMissingConnOpts
	}

	if connOpts.Conn == nil {
		return validate.ErrMissingConnCfg
	}

	args := connOpts.GetNatsJetstream()
	if args == nil {
		return validate.ErrMissingConnArgs
	}

	return nil
}
