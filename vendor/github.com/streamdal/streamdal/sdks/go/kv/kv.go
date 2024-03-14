package kv

import (
	"sync"

	"github.com/pkg/errors"

	"github.com/streamdal/streamdal/sdks/go/logger"
)

var (
	ErrNilConfig = errors.New("config cannot be nil")
)

type KV struct {
	cfg    *Config
	kvs    map[string]string
	kvsMtx *sync.RWMutex
	log    logger.Logger
}

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 . IKV
type IKV interface {
	// Get gets a value from the KV store; bool indicates if value exists
	Get(key string) (string, bool)

	// Set sets a key/value pair in the KV store; return bool indicates if key
	// was overwritten
	Set(key string, value string) bool

	// Delete will delete a key from the KV store; return bool indicates if
	// key existed before deletion
	Delete(key string) bool

	// Exists checks if a key exists in the KV store
	Exists(key string) bool

	// Purge removes all keys from the KV store; return int indicates how many
	// keys were removed.
	Purge() int64

	// Keys returns a slice of all keys in the KV store
	Keys() []string

	// Items returns the number of keys in the KV store
	Items() int64
}

type Config struct {
	Logger logger.Logger
}

func New(cfg *Config) (*KV, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, errors.Wrap(err, "unable to validate config")
	}

	return &KV{
		cfg:    cfg,
		kvs:    make(map[string]string),
		kvsMtx: &sync.RWMutex{},
		log:    cfg.Logger,
	}, nil
}

func (k *KV) Get(key string) (string, bool) {
	k.kvsMtx.RLock()
	defer k.kvsMtx.RUnlock()

	if val, ok := k.kvs[key]; ok {
		return val, true
	}

	return "", false
}

func (k *KV) Set(key string, value string) bool {
	k.kvsMtx.Lock()
	defer k.kvsMtx.Unlock()

	if _, ok := k.kvs[key]; ok {
		k.kvs[key] = value
		return true
	}

	k.kvs[key] = value
	return false
}

func (k *KV) Delete(key string) bool {
	k.kvsMtx.Lock()
	defer k.kvsMtx.Unlock()

	exists := false

	if _, ok := k.kvs[key]; ok {
		exists = true
	}

	delete(k.kvs, key)

	return exists
}

func (k *KV) Exists(key string) bool {
	k.kvsMtx.RLock()
	defer k.kvsMtx.RUnlock()

	if _, ok := k.kvs[key]; ok {
		return true
	}

	return false
}

func (k *KV) Purge() int64 {
	k.kvsMtx.Lock()
	defer k.kvsMtx.Unlock()

	purged := int64(len(k.kvs))
	k.kvs = make(map[string]string)

	return purged
}

func (k *KV) Keys() []string {
	k.kvsMtx.RLock()
	defer k.kvsMtx.RUnlock()

	keys := make([]string, len(k.kvs))

	i := 0

	for key := range k.kvs {
		keys[i] = key
		i++
	}

	return keys
}

func (k *KV) Items() int64 {
	k.kvsMtx.RLock()
	defer k.kvsMtx.RUnlock()

	return int64(len(k.kvs))
}

func validateConfig(cfg *Config) error {
	if cfg == nil {
		return ErrNilConfig
	}

	if cfg.Logger == nil {
		cfg.Logger = &logger.TinyLogger{}
	}

	return nil
}
