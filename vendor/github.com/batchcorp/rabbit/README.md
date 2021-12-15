rabbit
======
[![](https://godoc.org/github.com/batchcorp/rabbit?status.svg)](http://godoc.org/github.com/batchcorp/rabbit) [![Master build status](https://github.com/batchcorp/rabbit/workflows/main/badge.svg)](https://github.com/batchcorp/rabbit/actions) [![Go Report Card](https://goreportcard.com/badge/github.com/batchcorp/rabbit)](https://goreportcard.com/report/github.com/batchcorp/rabbit)

A RabbitMQ wrapper lib around [streadway/amqp](https://github.com/streadway/amqp) 
with some bells and whistles.

* Support for auto-reconnect
* Support for context (ie. cancel/timeout)
* Support for using multiple binding keys

# Motivation

We (Batch), make heavy use of RabbitMQ - we use it as the primary method for
facilitating inter-service communication. Due to this, all services make use of
RabbitMQ and are both publishers and consumers.

We wrote this lib to ensure that all of our services make use of Rabbit in a
consistent, predictable way AND are able to survive network blips.

**NOTE**: This library works only with non-default exchanges. If you need support
for default exchange - open a PR!

# Usage
```go
package main

import (
    "fmt"
    "log"  

    "github.com/batchcorp/rabbit"
)

func main() { 
    r, err := rabbit.New(&rabbit.Options{
        URL:          "amqp://localhost",
        QueueName:    "my-queue",
        ExchangeName: "messages",
        BindingKeys:   []string{"messages"},
    })
    if err != nil {
        log.Fatalf("unable to instantiate rabbit: %s", err)
    }
    
    routingKey := "messages"
    data := []byte("pumpkins")

    // Publish something
    if err := r.Publish(context.Background(), routingKey, data); err != nil {
        log.Fatalf("unable to publish message: ")
    }

    // Consume once
    if err := r.ConsumeOnce(nil, func(amqp.Delivery) error {
        fmt.Printf("Received new message: %+v\n", msg)
    }); err != nil {
        log.Fatalf("unable to consume once: %s", err),
    }

    var numReceived int

    // Consume forever (blocks)
    ctx, cancel := context.WithCancel(context.Background())

    r.Consume(ctx, nil, func(msg amqp.Delivery) error {
        fmt.Printf("Received new message: %+v\n", msg)
        
        numReceived++
        
        if numReceived > 1 {
            r.Stop()
        }
    })

    // Or stop via ctx 
    r.Consume(..)
    cancel()
}
```
