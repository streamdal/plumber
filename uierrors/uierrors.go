package uierrors

import (
	"context"
	"sync"
	"time"

	"github.com/batchcorp/plumber/kv"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
)

const (
	Bucket = "uierrors"

	// DefaultErrorTTL determines how long errors remain in etcd before the key expires
	DefaultErrorTTL = time.Minute * 24 * 30

	// DefaultMaxErrorHistory determines how many latest errors to return when calling GetErrorHistory()
	DefaultMaxErrorHistory = 1000
)

var (
	ErrMissingError      = errors.New("error cannot be empty")
	ErrMissingResource   = errors.New("resource cannot be empty")
	ErrMissingResourceID = errors.New("resource ID cannot be empty")
	ErrMissingKV         = errors.New("KV cannot be nil")
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 . IUIErrors
type IUIErrors interface {
	AddError(msg *protos.ErrorMessage) error
	ConnectClient(id string) *AttachedStream
	DisconnectClient(id string)
	GetHistory(ctx context.Context) ([]*protos.ErrorMessage, error)
}

type UIErrors struct {
	*Config
	AttachedClientsMtx *sync.RWMutex
	AttachedClients    map[string]*AttachedStream
	log                *logrus.Entry
}

// Config is used to pass required options to New()
type Config struct {
	KV kv.IKV
}

// AttachedStream is used to hold a channel which sends protos.ErrorMessage for a gRPC stream to receive
type AttachedStream struct {
	MessageCh chan *protos.ErrorMessage
}

// New instantiates the uierrors service
func New(cfg *Config) (*UIErrors, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, errors.Wrap(err, "unable to validate config")
	}

	return &UIErrors{
		Config:             cfg,
		AttachedClients:    make(map[string]*AttachedStream, 0),
		AttachedClientsMtx: &sync.RWMutex{},
		log:                logrus.WithField("pkg", "uierrors"),
	}, nil
}

// validateConfig ensures all required configuration options are passed when instantiating the uierrors service
func validateConfig(cfg *Config) error {
	if cfg.KV == nil {
		return ErrMissingKV
	}

	return nil
}

// AddError is called by plumber code where we need to send an error to the client
func (u *UIErrors) AddError(msg *protos.ErrorMessage) error {
	return nil

	//if err := validateError(msg); err != nil {
	//	return errors.Wrap(err, "could not send error message")
	//}
	//
	//// Send to consumers
	//u.AttachedClientsMtx.RLock()
	//for _, c := range u.AttachedClients {
	//	c.MessageCh <- msg
	//}
	//u.AttachedClientsMtx.RUnlock()
	//
	//// Save to etcd
	//data, err := proto.Marshal(msg)
	//if err != nil {
	//	return errors.Wrap(err, "unable to marshal protobuf ErrorMessage to JSON")
	//}
	//
	//// We are storing keys as nanoseconds so they won't conflict, and can be sorted when retrieved from etcd
	//cachePath := fmt.Sprintf("%s/%d", bus.CacheErrorsPrefix, time.Now().UTC().UnixNano())
	//
	//_, err = u.KV.PutWithTTL(context.Background(), cachePath, string(data), DefaultErrorTTL)
	//if err != nil {
	//	return errors.Wrap(err, "unable to save error to etcd")
	//}
	//
	//return nil
}

// ConnectClient is called whenever the streaming gRPC endpoint GetErrors() is called. A new AttachedClient
// will be added to the AttachedClients. Any stored messages in etcd will be read, sent to the client, and then deleted
// from etcd.
func (u *UIErrors) ConnectClient(id string) *AttachedStream {
	s := &AttachedStream{MessageCh: make(chan *protos.ErrorMessage, 1000)}

	u.AttachedClientsMtx.Lock()
	u.AttachedClients[id] = s
	u.AttachedClientsMtx.Unlock()

	return s
}

// GetHistory reads all error messages stored in etcd
func (u *UIErrors) GetHistory(_ context.Context) ([]*protos.ErrorMessage, error) {
	// NATS KV does not have a sort + limit in their key/val implementation.
	// Will need to figure something out.
	return nil, nil

	//resp, err := u.EtcdService.Get(
	//	ctx,
	//	bus.CacheErrorsPrefix,
	//	clientv3.WithPrefix(),
	//	clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend),
	//	clientv3.WithLimit(DefaultMaxErrorHistory),
	//)
	//if err != nil {
	//	return nil, errors.Wrap(err, "unable to read cached error messages")
	//}
	//
	//history := make([]*protos.ErrorMessage, 0)
	//
	//for _, v := range resp.Kvs {
	//	errorMsg := &protos.ErrorMessage{}
	//	if err := proto.Unmarshal(v.Value, errorMsg); err != nil {
	//		u.log.Errorf("unable to unmarshal saved error message: %s", err)
	//		continue
	//	}
	//	history = append(history, errorMsg)
	//}
	//
	//return history, nil
}

// DisconnectClient removes an attached client from the map, and cleans up the channel
func (u *UIErrors) DisconnectClient(id string) {
	u.AttachedClientsMtx.Lock()
	defer u.AttachedClientsMtx.Unlock()
	client, ok := u.AttachedClients[id]
	if !ok {
		return
	}

	delete(u.AttachedClients, id)
	close(client.MessageCh)
}

// validateError ensures an error message has all required fields before shipping
func validateError(err *protos.ErrorMessage) error {
	if err == nil {
		return ErrMissingError
	}

	if err.Error == "" {
		return ErrMissingError
	}

	if err.Resource == "" {
		return ErrMissingResource
	}

	if err.ResourceId == "" {
		return ErrMissingResourceID
	}

	if err.Timestamp == 0 {
		err.Timestamp = time.Now().UTC().UnixNano()
	}

	return nil
}
