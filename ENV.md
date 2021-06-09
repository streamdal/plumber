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
| `PLUMBER_RELAY_BATCH_SIZE` | How many messages to batch before sending them to grpc-collector | `10` |
| `PLUMBER_STATS` | Display periodic consumer/producer stats | `false` |
| `PLUMBER_STATS_REPORT_INTERVAL` | Interval at which periodic stats are displayed | `5s` |

## SQS

| Environment Variable  | Description | Default |
| --------------------- | ------------| ------- |
| `PLUMBER_RELAY_SQS_QUEUE_NAME`                 | Queue name | **REQUIRED** |
| `PLUMBER_RELAY_SQS_REMOTE_ACCOUNT_ID`          | Remote AWS account ID | **REQUIRED** |
| `PLUMBER_RELAY_SQS_MAX_NUM_MESSAGES`           | Max number of messages to read | `1` |
| `PLUMBER_RELAY_SQS_RECEIVE_REQUEST_ATTEMPT_ID` | An id to identify this read request by | `plumber` | 
| `PLUMBER_RELAY_SQS_AUTO_DELETE`                | Delete read/received messages | `false` |
| `PLUMBER_RELAY_SQS_WAIT_TIME_SECONDS`          | Number of seconds to wait for messages (not used when using 'follow') |

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
| `PLUMBER_RELAY_RABBIT_QUEUE_EXCLUSIVE`   | Whether plumber should be the only one using the queue | `false` |
| `PLUMBER_RELAY_RABBIT_AUTOACK`           | Automatically acknowledge receipt of read/received messages | `true` |
| `PLUMBER_RELAY_RABBIT_QUEUE_DECLARE`     | Whether to declare the specified queue to create it | `true` |
| `PLUMBER_RELAY_CONSUMER_TAG`             | How to identify the consumer to RabbitMQ | `plumber` |

## Kafka

| Environment Variable  | Description | Default |
| --------------------- | ------------| ------- |
| `PLUMBER_RELAY_KAFKA_ADDRESS`            | Destination host address. Separate multiple address with a comma | `broker1.domain.com:9092,broker2.domain.com` |
| `PLUMBER_RELAY_KAFKA_TOPICS`              | Topic(s) to read message(s) from. Separate multiple topics with a comma | **REQUIRED** |
| `PLUMBER_RELAY_KAFKA_TIMEOUT`            | Connect timeout | `10s` |
| `PLUMBER_RELAY_KAFKA_USE_TLS`            | Use TLS or not | `false` |
| `PLUMBER_RELAY_KAFKA_INSECURE_TLS`       | Use insecure TLS (ie. do not verify cert) | `false` |
| `PLUMBER_RELAY_KAFKA_USERNAME`           | SASL Username | |
| `PLUMBER_RELAY_KAFKA_PASSWORD`           | SASL Password. If omitted, you will be prompted for the password | |
| `PLUMBER_RELAY_KAFKA_SASL_TYPE`          | SASL Authentication type (plain or scram) | `scram` |
| `PLUMBER_RELAY_KAFKA_USE_CONSUMER_GROUP` | Use consumer  | `true` |
| `PLUMBER_RELAY_KAFKA_GROUP_ID`           | Specify a specific group-id to use when reading from kafka | `plumber` | 
| `PLUMBER_RELAY_KAFKA_READ_OFFSET`        | At what offset should consumer start reading (NOTE: offset is ignored if plumber is using consumer group) | `0` |
| `PLUMBER_RELAY_KAFKA_MAX_WAIT`           | How long to wait for new data when reading batches of messages | `1s` |
| `PLUMBER_RELAY_KAFKA_MIN_BYTES`          | Minimum number of bytes to fetch in a single kafka request (throughput optimization) | `1` |
| `PLUMBER_RELAY_KAFKA_MAX_BYTES`          | Maximum number of bytes to fetch in a single kafka request (throughput optimization) | `1` |
| `PLUMBER_RELAY_KAFKA_QUEUE_CAPACITY`     | Internal queue capacity (throughput optimization) | `1` |
| `PLUMBER_RELAY_KAFKA_REBALANCE_TIMEOUT`  | How long a coordinator will wait for member joins as part of a rebalance | `0` |
| `PLUMBER_RELAY_KAFKA_COMMIT_INTERVAL`    | How often to commit offsets to broker (0 = synchronous) | `5s` | 

**NOTE**: For _Confluent-hosted_ Kafka, you MUST set:

* `PLUMBER_RELAY_KAFKA_INSECURE_TLS` to `true`
* `PLUMBER_RELAY_KAFKA_SASL_TYPE` to `plain`
* Use API key for `PLUMBER_RELAY_KAFKA_USERNAME`
* Use API secret for `PLUMBER_RELAY_KAFKA_PASSWORD`

## Azure Message Bus

| Environment Variable  | Description | Default |
| --------------------- | ------------| ------- |
| `SERVICEBUS_CONNECTION_STRING`      | Full connection string used to access azure message bus queue or topic | **REQUIRED** |
| `PLUMBER_RELAY_AZURE_QUEUE_NAME`    | Queue name to read from. (*Must specify this or topic name*)|  |
| `PLUMBER_RELAY_AZURE_TOPIC_NAME`    | Topic name to read from. (*Must specify this or queue name*) |  |
| `PLUMBER_RELAY_AZURE_SUBSCRIPTION`  | Topic's Subscription name to read from | **REQUIRED if topic is specified** |

## GCP Pub/Sub

| Environment Variable  | Description | Default |
| --------------------- | ------------| ------- |
| `GOOGLE_APPLICATION_CREDENTIALS` | Credentials file for service account | **REQUIRED** |
| `PLUMBER_RELAY_GCP_PROJECT_ID` | Project ID | **REQUIRED** |
| `PLUMBER_RELAY_GCP_SUBSCRIPTION_ID` | Subscription ID | **REQUIRED** |
| `PLUMBER_RELAY_GCP_ACK_MESSAGE` | Whether to acknowledge message receive | `true` |

## Redis-PubSub

| Environment Variable  | Description | Default |
| --------------------- | ------------| ------- |
| `PLUMBER_RELAY_REDIS_PUBSUB_ADDRESS` | Redis server address | `localhost:6379` |
| `PLUMBER_RELAY_REDIS_PUBSUB_CHANNELS` | Channels that plumber should subscribe to | **REQUIRED** |
| `PLUMBER_RELAY_REDIS_PUBSUB_USERNAME` | Username (redis >= v6.0.0) | |
| `PLUMBER_RELAY_REDIS_PUBSUB_PASSWORD` | Password (redis >= v1.0.0) | |
| `PLUMBER_RELAY_REDIS_PUBSUB_DATABASE` | Database (0-15) | `0` |

## Redis-Streams
| Environment Variable  | Description | Default |
| --------------------- | ------------| ------- |
| `PLUMBER_RELAY_REDIS_STREAMS_ADDRESS` | Redis server address | `localhost:6379` |
| `PLUMBER_RELAY_REDIS_STREAMS_CHANNELS` | Channels that plumber should subscribe to | **REQUIRED** |
| `PLUMBER_RELAY_REDIS_STREAMS_USERNAME` | Username (redis >= v6.0.0) | |
| `PLUMBER_RELAY_REDIS_STREAMS_PASSWORD` | Password (redis >= v1.0.0) | |
| `PLUMBER_RELAY_REDIS_STREAMS_DATABASE` | Database (0-15) | `0` |
| `PLUMBER_RELAY_REDIS_STREAMS_STREAMS` | Comma-separated list of streams | **REQUIRED** |
| `PLUMBER_RELAY_REDIS_STREAMS_CONSUMER_GROUP` | Consumer group name to use | `plumber`
| `PLUMBER_RELAY_REDIS_STREAMS_CONSUMER_NAME` | Name of this consumer | `plumber-consumer-1` |
| `PLUMBER_RELAY_REDIS_STREAMS_COUNT` | How many messages to read per batch | `10` |
| `PLUMBER_RELAY_REDIS_STREAMS_START_ID` | What ID should consumer start reading at (only applies to a NEW consumer group) | `0` |
| `PLUMBER_RELAY_REDIS_STREAMS_RECREATE_CONSUMER_GROUP` | Recreate consumer group (will purge old consumer offsets) | `false` |
| `PLUMBER_RELAY_REDIS_STREAMS_CREATE_STREAMS` | Create streams when declaring consumer group | `false` |

## Postgres CDC
| Environment Variable  | Description | Default |
| --------------------- | ------------| ------- |
| `PLUMBER_RELAY_CDCPOSTGRES_USE_TLS` | Force TLS usage  | `false` |
| `PLUMBER_RELAY_CDCPOSTGRES_SKIP_VERIFY_TLS` | Skip server cert verification if using TLS | `false` |
| `PLUMBER_RELAY_CDCPOSTGRES_HOSTNAME` | Postgres server hostname | **REQUIRED** |
| `PLUMBER_RELAY_CDCPOSTGRES_PORT` | Port Postgres is running on | `5432` |
| `PLUMBER_RELAY_CDCPOSTGRES_USERNAME` | DB Username | **REQUIRED** |
| `PLUMBER_RELAY_CDCPOSTGRES_PASSWORD` | DB Passsword |  |
| `PLUMBER_RELAY_CDCPOSTGRES_DATABASE` | Database | **REQUIRED** |
| `PLUMBER_RELAY_CDCPOSTGRES_SLOT` | Replication slot name | **REQUIRED**  |
| `PLUMBER_RELAY_CDCPOSTGRES_PUBLISHER`| Publication Name | **REQUIRED** |

## Mongo CDC
| Environment Variable  | Description | Default |
| --------------------- | ------------| ------- |
| `PLUMBER_RELAY_CDCMONGO_DSN` | Data Source Name string to connect to mongo | |
| `PLUMBER_RELAY_CDCMONGO_DATABASE` | Database name | *Optional* |
| `PLUMBER_RELAY_CDCMONGO_COLLECTION` | Collection Name | *Optional* |
| `PLUMBER_RELAY_CDCMONGO_INCLUDE_FULL_DOC` | Include fullDocument in return payload | `false` |
