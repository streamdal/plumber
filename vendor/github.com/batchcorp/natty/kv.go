package natty

import (
	"context"
	"regexp"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
)

type KeyValueMap struct {
	rwMutex *sync.RWMutex
	// Key = bucket name, value = KeyValue
	kvMap map[string]nats.KeyValue
}

func (n *Natty) Get(ctx context.Context, bucket string, key string) ([]byte, error) {
	// NOTE: Context usage for K/V operations is not available in NATS (yet)
	kv, err := n.getBucket(ctx, bucket, false, 0)
	if err != nil {
		if err == nats.ErrBucketNotFound {
			return nil, nats.ErrKeyNotFound
		}

		return nil, errors.Wrap(err, "failed to get bucket")
	}

	kve, err := kv.Get(key)
	if err != nil {
		if err == nats.ErrKeyNotFound {
			return nil, nats.ErrKeyNotFound
		}

		return nil, errors.Wrap(err, "unable to fetch key")
	}

	return kve.Value(), nil
}

// Put puts a key/val into a bucket and will create bucket if it doesn't already
// exit. TTL is optional - it will only be used if the bucket does not exist &
// only the first TTL will be used.
func (n *Natty) Put(ctx context.Context, bucket string, key string, data []byte, keyTTL ...time.Duration) error {
	// NOTE: Context usage for K/V operations is not available in NATS (yet)
	var ttl time.Duration

	if len(keyTTL) > 0 {
		ttl = keyTTL[0]
	}

	kv, err := n.getBucket(ctx, bucket, true, ttl)
	if err != nil {
		return errors.Wrap(err, "unable to fetch bucket")
	}

	if _, err := kv.Put(key, data); err != nil {
		return errors.Wrap(err, "unable to put key")
	}

	return nil
}

// Create will add the key/value pair iff it does not exist; it will create
// the bucket if it does not already exist. TTL is optional - it will only be
// used if the bucket does not exist & only the first TTL will be used.
func (n *Natty) Create(ctx context.Context, bucket string, key string, data []byte, keyTTL ...time.Duration) error {
	// NOTE: Context usage for K/V operations is not available in NATS (yet)
	var ttl time.Duration

	if len(keyTTL) > 0 {
		ttl = keyTTL[0]
	}

	kv, err := n.getBucket(ctx, bucket, true, ttl)
	if err != nil {
		return errors.Wrap(err, "unable to fetch bucket")
	}

	if _, err := kv.Create(key, data); err != nil {
		return errors.Wrap(err, "unable to put key")
	}

	return nil
}

func (n *Natty) Keys(ctx context.Context, bucket string) ([]string, error) {
	kv, err := n.getBucket(ctx, bucket, false, 0)
	if err != nil {
		return nil, err
	}

	keys, err := kv.Keys(nats.Context(ctx))
	if err != nil {
		if err == nats.ErrNoKeysFound {
			return make([]string, 0), nil
		}

		return nil, err
	}

	return keys, nil
}

func (n *Natty) Delete(ctx context.Context, bucket string, key string) error {
	// NOTE: Context usage for K/V operations is not available in NATS (yet)
	kv, err := n.getBucket(ctx, bucket, false, 0)
	if err != nil {
		if err == nats.ErrBucketNotFound {
			return nil
		}

		return errors.Wrap(err, "unable to fetch bucket")
	}

	return kv.Purge(key)
}

func (n *Natty) DeleteBucket(_ context.Context, bucket string) error {
	// Get rid of it locally (noop if doesn't exist)
	n.kvMap.Delete(bucket)

	if err := n.js.DeleteKeyValue(bucket); err != nil {
		if err == nats.ErrStreamNotFound {
			return nil
		}

		return errors.Wrap(err, "unable to delete bucket")
	}

	return nil
}

// CreateBucket creates a bucket; returns an error if it already exists.
// Context usage not supported by NATS kv (yet).
func (n *Natty) CreateBucket(_ context.Context, name string, ttl time.Duration, replicaCount int, description ...string) error {
	if err := validateCreateBucket(name, ttl, replicaCount); err != nil {
		return errors.Wrap(err, "unable to validate args")
	}

	cfg := &nats.KeyValueConfig{
		Bucket:   name,
		TTL:      ttl,
		Replicas: replicaCount,
	}

	if len(description) > 0 {
		cfg.Description = description[0]
	}

	kv, err := n.js.CreateKeyValue(cfg)
	if err != nil {
		return err
	}

	n.kvMap.Put(name, kv)

	return nil
}

func validateCreateBucket(name string, _ time.Duration, replicaCount int) error {
	if name == "" {
		return errors.New("bucket name cannot be empty")
	}

	regex := regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
	if !regex.MatchString(name) {
		return errors.New("bucket name can only contain alphanumeric, dash or underscore characters")
	}

	if replicaCount < 1 {
		return errors.New("replicaCount must be greater than 0")
	}

	return nil
}

// WatchBucket returns an instance of nats.KeyWatcher for the given bucket
func (n *Natty) WatchBucket(ctx context.Context, bucket string) (nats.KeyWatcher, error) {
	b, err := n.getBucket(ctx, bucket, false, 0)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to watch bucket '%s'", bucket)
	}

	watcher, err := b.WatchAll()
	if err != nil {
		return nil, errors.Wrapf(err, "unable to watch bucket '%s'", bucket)
	}

	return watcher, nil
}

// getBucket will either fetch a known bucket or create it if it doesn't exist
func (n *Natty) getBucket(_ context.Context, bucket string, create bool, ttl time.Duration) (nats.KeyValue, error) {
	// NOTE: Context usage for K/V operations is not available in NATS (yet)

	// Do we have this bucket locally?
	kv, ok := n.kvMap.Get(bucket)
	if ok {
		return kv, nil
	}

	// Nope - try to get it from NATS
	kv, err := n.js.KeyValue(bucket)
	if err != nil {
		// Is this a fatal error?
		if err != nats.ErrBucketNotFound {
			return nil, errors.Wrap(err, "key value fetch error in getBucket()")
		}
	}

	// We either found the bucket or got a ErrBucketNotFound
	if kv != nil {
		n.kvMap.Put(bucket, kv)
		return kv, nil
	}

	// Bucket was not found and we want to create
	if kv == nil && create {
		kv, err = n.js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket:      bucket,
			Description: "auto-created bucket via natty",
			History:     5,
			TTL:         ttl,
		})

		if err != nil {
			return nil, errors.Wrap(err, "bucket create error in getBucket()")
		}

		n.kvMap.Put(bucket, kv)

		return kv, nil
	}

	// If we got here, we ran into a ErrBucketNotFound and we don't want to
	// create a new bucket.
	return nil, nats.ErrBucketNotFound
}

func (k *KeyValueMap) Get(key string) (nats.KeyValue, bool) {
	k.rwMutex.RLock()
	v, ok := k.kvMap[key]
	k.rwMutex.RUnlock()

	return v, ok
}

func (k *KeyValueMap) Put(key string, value nats.KeyValue) {
	k.rwMutex.Lock()
	k.kvMap[key] = value
	k.rwMutex.Unlock()
}

// Delete functionality is not used because there is no way to list buckets in NATS
func (k *KeyValueMap) Delete(key string) {
	k.rwMutex.Lock()
	delete(k.kvMap, key)
	k.rwMutex.Unlock()
}
