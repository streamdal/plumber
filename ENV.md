Environment Variables
=====================
`plumber` reads the following env vars in relay mode:

## Shared

| Environment Variable  | Description |
| --------------------- | ------------|
| `PLUMBER_RELAY_TYPE` | Type of collector to use. Ex: rabbit, kafka, aws-sqs | **REQUIRED** | 
| `PLUMBER_RELAY_TOKEN` | Collection token to use when sending data to Batch | **REQUIRED** |
| `PLUMBER_RELAY_GRPC_ADDRESS` | Alternative gRPC collector address | `grpc-collector.batch.sh:9000` |
| `PLUMBER_RELAY_GRPC_DISABLE_TLS` | Disable TLS when talking to gRPC collector | `false` |
| `PLUMBER_RELAY_GRPC_TIMEOUT` | gRPC collector timeout | `10s` |
| `PLUMBER_RELAY_NUM_WORKERS` | Number of relay workers | `10` |
| `PLUMBER_RELAY_HTTP_LISTEN_ADDRESS` | Alternative listen address for local HTTP server | `:8080` |


## SQS

| Environment Variable  | Description | Default |
| --------------------- | ------------| ------- |
| `PLUMBER_RELAY_SQS_QUEUE_NAME`                 | Queue name | **REQUIRED** |
| `PLUMBER_RELAY_SQS_REMOTE_ACCOUNT_ID`          | Remote AWS account ID | **REQUIRED** |
| `PLUMBER_RELAY_SQS_MAX_NUM_MESSAGES`           | Max number of messages to read | `1` |
| `PLUMBER_RELAY_SQS_RECEIVE_REQUEST_ATTEMPT_ID` | An id to identify this read request by | `plumber` | 
| `PLUMBER_RELAY_SQS_AUTO_DELETE`                | Delete read/received messages | `false` |
| `PLUMBER_RELAY_SQS_WAIT_TIME_SECONDS`          | Number of seconds to wait for messages (not used when using 'follow') |
| 

## RabbitMQ

| Environment Variable  | Description | Default |
| --------------------- | ------------| ------- |
| `PLUMBER_RELAY_RABBIT_ADDRESS`           | Destination host address | `amqp://localhost` |
| `PLUMBER_RELAY_RABBIT_EXCHANGE`          | Name of the exchange | **REQUIRED** | 
| `PLUMBER_RELAY_RABBIT_USE_TLS`           | Force TLS usage (regardless of DSN) | `false` |
| `PLUMBER_RELAY_RABBIT_SKIP_VERIFY_TLS`   | Skip server cert verification | `false` |
| `PLUMBER_RELAY_RABBIT_ROUTING_KEY`       | Used as binding-key | **REQUIRED** |
| `PLUMBER_RELAY_RABBIT_QUEUE`             | Name of the queue where messages will be routed to | **REQUIRED** |
| `PLUMBER_RELAY_RABBIT_QUEUE_DURABLE`     | Whether the queue we declare should survive server restarts | `false` |
| `PLUMBER_RELAY_RABBIT_QUEUE_AUTO_DELETE` | Whether to auto-delete the queue after plumber has disconnected | `true` |
| `PLUMBER_RELAY_RABBIT_QUEUE_EXCLUSIVE`   | Whether plumber should be the only one using the newly defined queue | `true` |
| `PLUMBER_RELAY_RABBIT_AUTOACK`           | Automatically acknowledge receipt of read/received messages | `true` |
| `PLUMBER_RELAY_RABBIT_QUEUE_DECLARE`     | Whether to declare the specified queue to create it | `true` |
| `PLUMBER_RELAY_CONSUMER_TAG`             | How to identify the consumer to RabbitMQ | `plumber` |

## Kafka

| Environment Variable  | Description | Default |
| --------------------- | ------------| ------- |
| `PLUMBER_RELAY_KAFKA_ADDRESS`      | Destination host address | `localhost:9092` |
| `PLUMBER_RELAY_KAFKA_TOPIC`        | Topic to read message(s) from | **REQUIRED** |
| `PLUMBER_RELAY_KAFKA_TIMEOUT`      | Connect timeout | `10s` |
| `PLUMBER_RELAY_KAFKA_INSECURE_TLS` | Use insecure TLS (ie. do not verify cert) | `false` |
| `PLUMBER_RELAY_KAFKA_USERNAME`     | SASL Username | |
| `PLUMBER_RELAY_KAFKA_PASSWORD`     | SASL Password. If omitted, you will be prompted for the password | |
| `PLUMBER_RELAY_KAFKA_SASL_TYPE`    | SASL Authentication type (plain or scram) | `scram` |
| `PLUMBER_RELAY_GROUP_ID`           | Specify a specific group-id to use when reading from kafka | `plumber` | 
| `PLUMBER_RELAY_MAX_WAIT`           | How long to wait for new data when reading batches of messages | `1s` |
| `PLUMBER_RELAY_MIN_BYTES`          | Minimum number of bytes to fetch in a single kafka request (throughput optimization) | `1` |
| `PLUMBER_RELAY_MAX_BYTES`          | Maximum number of bytes to fetch in a single kafka request (throughput optimization) | `1` |
| `PLUMBER_RELAY_QUEUE_CAPACITY`     | Internal queue capacity (throughput optimization) | `1` |
| `PLUMBER_RELAY_REBALANCE_TIMEOUT`  | How long a coordinator will wait for member joins as part of a rebalance | `0` |

**NOTE**: For _Confluent-hosted_ Kafka, you MUST set:

* `PLUMBER_RELAY_KAFKA_INSECURE_TLS` to `true`
* `PLUMBER_RELAY_KAFKA_SASL_TYPE` to `plain`
* Use API key for `PLUMBER_RELAY_KAFKA_USERNAME`
* Use API secret for `PLUMBER_RELAY_KAFKA_PASSWORD`
