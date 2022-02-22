// etcd/clientv3 has an interface: https://godoc.org/go.etcd.io/etcd/clientv3#KV
// We choose to do our own as we are only using a few of the available methods
// AND in case we have to do something special as part of Get/Put/Delete.
package etcd

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
)

const (
	AvailabilityTimeout  = 5 * time.Second
	CollectMessagePrefix = "/collector/collect"
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 . IEtcd
type IEtcd interface {
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
	Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error)
	Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error)
}

type Etcd struct {
	Client *clientv3.Client
	log    *logrus.Entry
}

type Options struct {
	Endpoints   []string
	DialTimeout time.Duration
	TLS         *tls.Config
	Username    string
	Password    string
}

func New(opts *Options) (*Etcd, error) {
	if err := validateOptions(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	clientConfig := clientv3.Config{
		Endpoints:   opts.Endpoints,
		DialTimeout: opts.DialTimeout,
		Username:    opts.Username,
		Password:    opts.Password,
	}

	if opts.TLS != nil {
		logrus.Debug("TLS enabled for etcd")

		clientConfig.TLS = opts.TLS
	}

	cli, err := clientv3.New(clientConfig)
	if err != nil {
		return nil, err
	}

	if err := testAvailability(cli); err != nil {
		return nil, errors.Wrap(err, "etcd is not available")
	}

	return &Etcd{
		Client: cli,
		log:    logrus.WithField("pkg", "etcd"),
	}, nil
}

func validateOptions(opts *Options) error {
	if len(opts.Endpoints) == 0 {
		return errors.New("endpoints cannot be empty")
	}

	if opts.DialTimeout < 1 {
		return errors.New("DialTimeout must be > 0")
	}

	return nil
}

func (e *Etcd) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return e.Client.Get(ctx, key, opts...)
}

func (e *Etcd) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	return e.Client.Put(ctx, key, val, opts...)
}

func (e *Etcd) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return e.Client.Delete(ctx, key, opts...)
}

func testAvailability(cli *clientv3.Client) error {
	key := fmt.Sprintf("test-%d", rand.Int())

	ctx, cancel := context.WithTimeout(context.Background(), AvailabilityTimeout)

	lease, err := cli.Grant(ctx, 5)
	if err != nil {
		return errors.Wrap(err, "unable to create lease for test file")
	}

	_, err = cli.Put(ctx, key, "Are we alive?", clientv3.WithLease(lease.ID))
	cancel()
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("unable to PUT key '%s'", key))
	}

	ctx, cancel = context.WithTimeout(context.Background(), AvailabilityTimeout)

	_, err = cli.Get(ctx, key)
	cancel()
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("unable to GET key '%s'", key))
	}

	return nil
}
