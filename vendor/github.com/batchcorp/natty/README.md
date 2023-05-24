natty
=====
[![Go Reference](https://pkg.go.dev/badge/github.com/batchcorp/natty.svg)](https://pkg.go.dev/github.com/batchcorp/natty)
[![Go Report Card](https://goreportcard.com/badge/github.com/batchcorp/natty)](https://goreportcard.com/report/github.com/batchcorp/natty)

An opinionated, [NATS](https://nats.io) Jetstream client wrapper lib for Go.

Used by [plumber](https://github.com/batchcorp/plumber) and other Batch applications.

## Why

NATS allows you tweak a lot of things - create push or pull streams, durable or
ephemeral consumers and all kinds of other settings.

The library exposes several, opinionated, quality-of-life functionality such as:

* Simplified publish/consume API *specifically* to be used with NATS-JS
    * `Consume()` uses ONLY durable consumers (as we want kafka-like behavior)
* Methods for interacting with key/value store in NATS
* Concurrency / leader election functionality

See the full interface [here](https://pkg.go.dev/github.com/batchcorp/natty#INatty).

## Consume & Publish

This library uses ONLY durable consumers and provides a two method API to interact
with your NATS deployment:

* `Consume(ctx context.Context, subject string, errorCh chan error, cb func(msg *nats.Msg)) error`
* `Publish(ctx context.Context, subject string, data []byte) error`

The `Consume()` will block and has to be cancelled via context. You can also
pass an optional error channel that the lib will write to when the callback func
runs into an error.

## HasLeader

`natty` provides an easy way to execute a function only if the instance is the
leader for a given bucket and key.

Example:
```go

bucketName := "election-bucket"
keyName := "election-key"

n.AsLeader(context.Background(), natty.AsLeaderConfig{
	Looper:   director.NewFreeLooper(director.Forever, make(error chan, 1)),
	Bucket:   bucketName,
	Key:      keyName,
	NodeName: "node1"
}, func() error {
	fmt.Println("executed by node 1")
})

n.AsLeader(context.Background(), natty.AsLeaderConfig{
	Looper:   director.NewFreeLooper(director.Forever, make(error chan, 1)),
    Bucket:   bucketName, 
    Key:      keyName,
    NodeName: "node2"
}, func() error {
    fmt.Println("executed by node 2")
})

// Only one will be executed
```

`AsLeader` uses NATS k/v store to facilitate leader election.

### Election Logic

During first execution, all instances running `AsLeader()` on the same bucket 
and key will attempt to `Create()` the leader key - only one will succeed as 
`Create()` will error if a key already exists.

On subsequent iterations, each `AsLeader()` will first check if it is the leader
by reading the key in the bucket. If it is the leader, it will `Put()` the 
cfg.Key with contents set to cfg.NodeName - the `Put()` will NOT error if the 
key already exists.

If the current leader is unable to `Put()` - it will try again next time until
it either succeeds or the key is TTL'd by the bucket policy.

When the bucket TTL is reached, the key will be deleted by NATS at which point,
one of the `AsLeader()` instances `Create()` call will succeed and they will
become the current leader.

## TLS NATS

The NATS server started via `docker-compose` is configured to use TLS (with keys
and certs located in `./assets/*`).

We are doing NATS w/ TLS purely to ensure that the library will work with it.
