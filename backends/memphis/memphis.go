package memphis

import (
	"context"
	"strconv"
	"strings"

	"github.com/memphisdev/memphis.go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/validate"
)

var (
	ErrEmptyStation = errors.New("station cannot be empty")
)

const BackendName = "memphis"

type Memphis struct {
	connOpts *opts.ConnectionOptions
	client   *memphis.Conn
	log      *logrus.Entry
}

func New(connOpts *opts.ConnectionOptions) (*Memphis, error) {
	if err := validateBaseConnOpts(connOpts); err != nil {
		return nil, errors.Wrap(err, "invalid connection options")
	}

	opts := connOpts.GetMemphis()

	host, port, err := getHostAndPort(opts.Address)
	if err != nil {
		return nil, errors.Wrap(err, "unable to setup memphis connection")
	}

	c, err := memphis.Connect(host, opts.Username, opts.BrokerToken, memphis.Port(port))
	if err != nil {
		return nil, errors.Wrap(err, "unable to connect to memphis server")
	}

	m := &Memphis{
		connOpts: connOpts,
		client:   c,
		log:      logrus.WithField("backend", BackendName),
	}

	return m, nil
}

func (m *Memphis) Name() string {
	return BackendName
}

func (m *Memphis) Close(_ context.Context) error {
	if m.client != nil {
		m.client.Close()
	}

	return nil
}

func (m *Memphis) Test(_ context.Context) error {
	return types.NotImplementedErr
}

func validateBaseConnOpts(connOpts *opts.ConnectionOptions) error {
	if connOpts == nil {
		return validate.ErrMissingConnOpts
	}

	if connOpts.Conn == nil {
		return validate.ErrMissingConnCfg
	}

	args := connOpts.GetMemphis()
	if args == nil {
		return validate.ErrMissingConnArgs
	}

	if args.Address == "" {
		return validate.ErrMissingAddress
	}

	return nil
}

func getHostAndPort(addr string) (string, int, error) {
	port := 6666
	addrParts := strings.Split(addr, ":")
	if len(addrParts) < 2 {
		return addr, port, nil
	}

	host := addrParts[0]
	pPort, err := strconv.Atoi(addrParts[1])
	if err != nil {
		return "", 0, errors.Wrapf(err, "unable to parse port from '%s'", addr)
	}
	return host, pPort, nil
}

func genHeaders(in map[string]string) memphis.Headers {
	headers := memphis.Headers{}
	headers.New()

	for k, v := range in {
		headers.Add(k, v)
	}

	return headers
}
