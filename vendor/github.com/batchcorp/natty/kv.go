package natty

import (
	"context"
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
)

type KeyValueMap struct {
	rwMutex *sync.RWMutex
	kvMap   map[string]nats.KeyValue
}

func (n *Natty) Get(ctx context.Context, bucket string, key string) ([]byte, error) {
	// NOTE: Context usage for K/V operations is not available in NATS (yet)
	kv, err := n.getBucket(ctx, bucket, false)
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

func (n *Natty) Put(ctx context.Context, bucket string, key string, data []byte) error {
	// NOTE: Context usage for K/V operations is not available in NATS (yet)
	kv, err := n.getBucket(ctx, bucket, true)
	if err != nil {
		return errors.Wrap(err, "unable to fetch bucket")
	}

	if _, err := kv.Put(key, data); err != nil {
		return errors.Wrap(err, "unable to put key")
	}

	return nil
}

func (n *Natty) Delete(ctx context.Context, bucket string, key string) error {
	// NOTE: Context usage for K/V operations is not available in NATS (yet)
	kv, err := n.getBucket(ctx, bucket, false)
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

// getBucket will either fetch a known bucket or create it if it doesn't exist
func (n *Natty) getBucket(_ context.Context, bucket string, create bool) (nats.KeyValue, error) {
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
