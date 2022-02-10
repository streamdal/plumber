natty
=====
An opinionated, [NATS](https://nats.io) Jetstream client wrapper lib for Go.

Used by [plumber](https://github.com/batchcorp/plumber) and other Batch applications.

## Why

NATS allows you tweak a lot of things - create push or pull streams, durable or
ephemeral consumers and all kinds of other settings.

This library uses ONLY durable consumers and provides a two method API to interact
with your NATS deployment:

* `Consume(ctx context.Context, subject string, errorCh chan error, cb func(msg *nats.Msg)) error`
* `Publish(ctx context.Context, subject string, data []byte) error`

The `Consume()` will block and has to be cancelled via context. You can also
pass an optional error channel that the lib will write to when the callback func
runs into an error.

`Publish()` is nothing fancy.

`New()` will perform the connect, create the stream and consumer.

## TLS NATS

The NATS server started via `docker-compose` is configured to use TLS (with keys
and certs located in `./assets/*`).

We are doing NATS w/ TLS purely to ensure that the library will work with it.
