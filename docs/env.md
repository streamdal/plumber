# Available environment variables

## Server

| **Environment Variable** | **Description** | **Default** | **Required** |
| ------------------------ | --------------- | ----------- | ------------ |
| PLUMBER_SERVER_NODE_ID | Unique ID that identifies this plumber node | plumber1 | **true** |
| PLUMBER_SERVER_CLUSTER_ID | ID of the plumber cluster (has to be the same across all nodes) | aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa | **true** |
| PLUMBER_SERVER_GRPC_LISTEN_ADDRESS | Host:port that the gRPC server will bind to | 127.0.0.1:9090 | false |
| PLUMBER_SERVER_AUTH_TOKEN | All gRPC requests require this auth token to be set | batchcorp | **true** |
| PLUMBER_SERVER_INITIAL_CLUSTER | InitialCluster should contain comma separated list of key=value pairs of host:port entries for ALL peers in the cluster. (Example: server1=http://192.168.1.10:2380 | plumber1=http://127.0.0.1:2380 | **true** |
| PLUMBER_SERVER_ADVERTISE_PEER_URL | Address of _this_ plumber instance etcd server interface. Example: http://local-ip:2380 | http://127.0.0.1:2380 | false |
| PLUMBER_SERVER_ADVERTISE_CLIENT_URL | Address of _this_ plumber instance etcd client interface. Example: http://local-ip:2379 | http://127.0.0.1:2379 | **true** |
| PLUMBER_SERVER_LISTENER_PEER_URL | Address that _this_ plumber instance etcd server should listen on. Example: http://local-ip:2380 | http://127.0.0.1:2380 | false |
| PLUMBER_SERVER_LISTENER_CLIENT_URL | Address that _this_ plumber instance etcd client should listen on. Example: http://local-ip:2379 | http://127.0.0.1:2379 | false |
| PLUMBER_SERVER_PEER_TOKEN | Secret token that ALL cluster members should use/share. This node will NOT be able to join the cluster IF this token does not match on one of the plumber instances. | secret | false |

## Relay

| **Environment Variable** | **Description** | **Default** | **Required** |
| ------------------------ | --------------- | ----------- | ------------ |
| PLUMBER_RELAY_HTTP_LISTEN_ADDRESS | What address to bind the built-in HTTP server to | localhost:9191 | false |
| PLUMBER_RELAY_TOKEN | Secret collection token |  | **true** |
| PLUMBER_RELAY_BATCH_SIZE | How many messages to send in a single batch | 1000 | false |
| PLUMBER_RELAY_BATCH_MAX_RETRY | How many times plumber will try re-sending a batch | 3 | false |
| PLUMBER_RELAY_NUM_WORKERS |  | 10 | false |
| PLUMBER_RELAY_GRPC_ADDRESS | Alternative collector to relay events to | grpc-collector.batch.sh:9000 | false |
| PLUMBER_RELAY_GRPC_DISABLE_TLS | Whether to use TLS with collector | false | false |
| PLUMBER_RELAY_GRPC_TIMEOUT | How long to wait before giving up talking to the gRPC collector | 5 | false |

## Backends

### AQS SNS

| **Environment Variable** | **Description** | **Default** | **Required** |
| ------------------------ | --------------- | ----------- | ------------ |
| AWS_DEFAULT_REGION |  |  | **true** |
| AWS_ACCESS_KEY_ID |  |  | **true** |
| AWS_SECRET_ACCESS_KEY |  |  | **true** |

### AWS SQS

| **Environment Variable** | **Description** | **Default** | **Required** |
| ------------------------ | --------------- | ----------- | ------------ |
| AWS_DEFAULT_REGION |  |  | **true** |
| AWS_ACCESS_KEY_ID |  |  | **true** |
| AWS_SECRET_ACCESS_KEY |  |  | **true** |
| PLUMBER_RELAY_SQS_QUEUE_NAME | Queue name |  | false |
| PLUMBER_RELAY_SQS_REMOTE_ACCOUNT_ID | Remote AWS account ID |  | false |
| PLUMBER_RELAY_SQS_MAX_NUM_MESSAGES | Max number of messages to read | 1 | false |
| PLUMBER_RELAY_SQS_RECEIVE_REQUEST_ATTEMPT_ID | An id to identify this read request by | plumber/relay | false |
| PLUMBER_RELAY_SQS_AUTO_DELETE | Auto-delete read/received message(s) |  | false |
| PLUMBER_RELAY_SQS_WAIT_TIME_SECONDS | Number of seconds to wait for messages (not used when using --continuous) | 5 | false |

### Azure Event Hub

| **Environment Variable** | **Description** | **Default** | **Required** |
| ------------------------ | --------------- | ----------- | ------------ |
| EVENTHUB_CONNECTION_STRING | Connection string |  | **true** |

### Azure Service Bus

| **Environment Variable** | **Description** | **Default** | **Required** |
| ------------------------ | --------------- | ----------- | ------------ |
| SERVICEBUS_CONNECTION_STRING | Connection string |  | **true** |
| PLUMBER_RELAY_AZURE_QUEUE_NAME | Queue name |  | **true** |
| PLUMBER_RELAY_AZURE_TOPIC_NAME | Topic name |  | **true** |
| PLUMBER_RELAY_AZURE_SUBSCRIPTION | Subscription name |  | **true** |

### Google Cloud PubSub

| **Environment Variable** | **Description** | **Default** | **Required** |
| ------------------------ | --------------- | ----------- | ------------ |
| PLUMBER_RELAY_GCP_PROJECT_ID | Project ID |  | **true** |
| PLUMBER_RELAY_GCP_CREDENTIALS | GCP Credentials in JSON format |  | false |
| GOOGLE_APPLICATION_CREDENTIALS | Path to GCP credentials JSON file |  | false |
| PLUMBER_RELAY_GCP_SUBSCRIPTION_ID | Subscription ID |  | **true** |
| PLUMBER_RELAY_GCP_ACK_MESSAGE | Whether to acknowledge message receive | true | false |

### Kafka

| **Environment Variable** | **Description** | **Default** | **Required** |
| ------------------------ | --------------- | ----------- | ------------ |
| PLUMBER_RELAY_KAFKA_ADDRESS | Kafka broker address (you may specify this flag multiple times) | localhost:9092 | **true** |
| PLUMBER_RELAY_TIMEOUT_SECONDS | Connect timeout | 10 | false |
| PLUMBER_RELAY_USE_TLS | Enable TLS usage |  | false |
| PLUMBER_RELAY_KAFKA_INSECURE_TLS | Allow insecure TLS usage |  | false |
| PLUMBER_RELAY_TLS_CA_CERT | TLS ca cert |  | false |
| PLUMBER_RELAY_TLS_CLIENT_CERT | TLS client cert|  | false |
| PLUMBER_RELAY_TLS_CLIENT_KEY | TLS client key |  | false |
| PLUMBER_RELAY_KAFKA_SASL_TYPE | SASL authentication type (options: none plain scram) | none | false |
| PLUMBER_RELAY_KAFKA_USERNAME | SASL Username |  | false |
| PLUMBER_RELAY_KAFKA_PASSWORD | SASL Password. You will be prompted for the password if omitted |  | false |
| PLUMBER_RELAY_KAFKA_TOPIC | Topic(s) to read |  | **true** |
| PLUMBER_RELAY_KAFKA_READ_OFFSET | Specify what offset the consumer should read from (only works if --use-consumer-group is false) | 0 | false |
| PLUMBER_RELAY_KAFKA_USE_CONSUMER_GROUP | Whether plumber should use a consumer group | true | false |
| PLUMBER_RELAY_KAFKA_GROUP_ID | Specify a specific group-id to use when reading from kafka | plumber | false |
| PLUMBER_RELAY_KAFKA_MAX_WAIT | How long to wait for new data when reading batches of messages | 5 | false |
| PLUMBER_RELAY_KAFKA_MIN_BYTES | Minimum number of bytes to fetch in a single kafka request (throughput optimization) | 1048576 | false |
| PLUMBER_RELAY_KAFKA_MAX_BYTES | Maximum number of bytes to fetch in a single kafka request (throughput optimization) | 1048576 | false |
| PLUMBER_RELAY_KAFKA_COMMIT_INTERVAL | How often to commit offsets to broker (0 = synchronous) | 5 | false |
| PLUMBER_RELAY_KAFKA_REBALANCE_TIMEOUT | How long a coordinator will wait for member joins as part of a rebalance | 5 | false |
| PLUMBER_RELAY_KAFKA_QUEUE_CAPACITY | Internal library queue capacity (throughput optimization) | 1000 | false |

### KubeMQ Queue

| **Environment Variable** | **Description** | **Default** | **Required** |
| ------------------------ | --------------- | ----------- | ------------ |
| PLUMBER_RELAY_KUBEMQ_QUEUE_ADDRESS | Dial string for KubeMQ server | localhost:50000 | **true** |
| PLUMBER_RELAY_KUBEMQ_QUEUE_AUTH_TOKEN | Client JWT authentication token |  | false |
| PLUMBER_RELAY_KUBEMQ_QUEUE_TLS_CLIENT_CERT | KubeMQ client cert file |  | false |
| PLUMBER_RELAY_KUBEMQ_QUEUE_CLIENT_ID | KubeMQ client ID | plumber | false |
| PLUMBER_RELAY_KUBEMQ_QUEUE_QUEUE | KubeMQ queue name |  | false |

### Mongo

| **Environment Variable** | **Description** | **Default** | **Required** |
| ------------------------ | --------------- | ----------- | ------------ |
| PLUMBER_RELAY_CDCMONGO_DSN | Dial string for mongo server (Ex: mongodb://localhost:27017) | mongodb://localhost:27017 | false |
| PLUMBER_RELAY_CDCMONGO_DATABASE | Database name |  | false |
| PLUMBER_RELAY_CDCMONGO_COLLECTION | Collection name |  | false |
| PLUMBER_RELAY_CDCMONGO_INCLUDE_FULL_DOC | Include full document in update in update changes (default - return deltas only) |  | false |

### Mqtt

| **Environment Variable** | **Description** | **Default** | **Required** |
| ------------------------ | --------------- | ----------- | ------------ |
| PLUMBER_RELAY_MQTT_TLS_CA_CERT | CA cert (only needed if addr is ssl://) |  | false |
| PLUMBER_RELAY_MQTT_TLS_CLIENT_CERT | Client cert file (only needed if addr is ssl://) |  | false |
| PLUMBER_RELAY_MQTT_TLS_CLIENT_KEY | Client key file (only needed if addr is ssl://) |  | false |
| PLUMBER_RELAY_MQTT_SKIP_VERIFY_TLS | Whether to verify server certificate |  | false |
| PLUMBER_RELAY_MQTT_ADDRESS | MQTT address | tcp://localhost:1883 | **true** |
| PLUMBER_RELAY_MQTT_CONNECT_TIMEOUT | How long to attempt to connect for | 5 | false |
| PLUMBER_RELAY_MQTT_CLIENT_ID | Client id presented to MQTT broker | plumber | false |
| PLUMBER_RELAY_MQTT_QOS | QoS level to use for pub/sub (options: at_most_once | at_most_once | false |
| PLUMBER_RELAY_MQTT_TOPIC | Topic to read message(s) from |  | **true** |
| PLUMBER_RELAY_MQTT_READ_TIMEOUT_SECONDS | How long to attempt to read message(s) | 0 | false |

### NATS

| **Environment Variable** | **Description** | **Default** | **Required** |
| ------------------------ | --------------- | ----------- | ------------ |
| PLUMBER_RELAY_NATS_DSN | Dial string for NATS server (Ex: nats://localhost:4222) | nats://localhost:4222 | **true** |
| PLUMBER_RELAY_NATS_CREDENTIALS | Contents of NATS .creds file to authenticate with |  | false |
| PLUMBER_RELAY_NATS_SUBJECT | Subject to read from. Ex: foo.bar.* |  | false |
| PLUMBER_RELAY_NATS_USE_TLS | Force TLS connection. (Ignored if DSN begins with "tls://") | false | false |
| PLUMBER_RELAY_NATS_TLS_CA_CERT | CA file (only used for TLS connections) |  | false |
| PLUMBER_RELAY_NATS_TLS_CLIENT_CERT | Client cert file (only used for TLS connections) |  | false |
| PLUMBER_RELAY_NATS_TLS_CLIENT_KEY | Client key file (only used for TLS connections) |  | false |

### NATS Streaming

| **Environment Variable** | **Description** | **Default** | **Required** |
| ------------------------ | --------------- | ----------- | ------------ |
| PLUMBER_RELAY_NATS_STREAMING_DSN | Dial string for NATS server (Ex: nats://localhost:4222) | nats://localhost:4222 | **true** |
| PLUMBER_RELAY_NATS_STREAMING_CLUSTER_ID | Cluster ID | Cluster ID of the Nats server  | **true** |
| PLUMBER_RELAY_NATS_STREAMING_CHANNEL | Channel name | Channel name to subscribe to | **true** |
| PLUMBER_RELAY_NATS_STREAMING_DURABLE_SUBSCRIPTION_NAME | Create a durable subscription with this name for the given channel |  | false |
| PLUMBER_RELAY_NATS_STREAMING_USE_TLS | Force TLS connection. (Ignored if DSN begins with "tls://") | false | false |
| PLUMBER_RELAY_NATS_STREAMING_TLS_CA_CERT | CA file (only used for TLS connections) |  | false |
| PLUMBER_RELAY_NATS_STREAMING_TLS_CLIENT_CERT | Client cert file (only used for TLS connections) |  | false |
| PLUMBER_RELAY_NATS_STREAMING_TLS_CLIENT_KEY | Client key file (only used for TLS connections) |  | false |
| PLUMBER_RELAY_NATS_STREAMING_SKIP_VERIFY_TLS | Whether to verify server certificate |  | false |
| PLUMBER_RELAY_NATS_STREAMING_READ_LAST | Deliver starting with last published message | false | false |
| PLUMBER_RELAY_NATS_STREAMING_READ_SEQUENCE | Deliver messages starting at this sequence number | 0 | false |
| PLUMBER_RELAY_NATS_STREAMING_READ_SINCE | Deliver messages in last interval (e.g. 1s, 1h) | | false |
| PLUMBER_RELAY_NATS_STREAMING_READ_ALL | Deliver all available messages | false | false |

### NATS JetStream

| **Environment Variable**                     | **Description**                                                                                                                        | **Default** | **Required** |
|----------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------| ----------- |------------|
| PLUMBER_RELAY_NATS_JETSTREAM_DSN             | Dial string for NATS server (Ex: nats://localhost:4222)                                                                                | nats://localhost:4222 | **true**   |
| PLUMBER_RELAY_NATS_JETSTREAM_CLIENT_ID       | User specified client ID to connect with                                                                                               | plumber | false      |
| PLUMBER_RELAY_NATS_JETSTREAM_STREAM          | Stream name to read from                                                                                                               |  | **true**   |
| PLUMBER_RELAY_NATS_JETSTREAM_USE_TLS         | Force TLS connection. (Ignored if DSN begins with "tls://")                                                                            | false | false      |
| PLUMBER_RELAY_NATS_JETSTREAM_TLS_CA_CERT     | CA file (only used for TLS connections)                                                                                                |  | false      |
| PLUMBER_RELAY_NATS_JETSTREAM_TLS_CLIENT_CERT | Client cert file (only used for TLS connections)                                                                                       |  | false      |
| PLUMBER_RELAY_NATS_JETSTREAM_TLS_CLIENT_KEY  | Client key file (only used for TLS connections)                                                                                        |  | false      |
| PLUMBER_RELAY_NATS_JETSTREAM_SKIP_VERIFY_TLS | Whether to verify server certificate (only needed using TLS)                                                                           |  | false      |
| PLUMBER_RELAY_NATS_JETSTREAM_CONSUMER_NAME   | Consumer name (has no effect if create_durable_consumer or existing_durable_consumer is not set)                                       |  | false      |
| PLUMBER_RELAY_NATS_JETSTREAM_CREATE_DURABLE_CONSUMER  | Whether to create a durable consumer (setting this will cause plumber to create a pull consumer)                                       |  | false      |
| PLUMBER_RELAY_NATS_JETSTREAM_KEEP_CONSUMER  | Whether to delete the consumer after use                                                                                               |  | false      |
| PLUMBER_RELAY_NATS_JETSTREAM_CONSUMER_START_SEQUENCE | Where in the stream the consumer should start reading (NOTE: If set, consumer deliver policy will be set to DeliverByStartSequence)    | | false |
| PLUMBER_RELAY_NATS_JETSTREAM_CONSUMER_START_TIME | At what time in the stream should the consumer begin reading (NOTE: If set, consumer deliver policy will be set to DeliverByStartTime) | | false |
| PLUMBER_RELAY_NATS_JETSTREAM_CONSUMER_FILTER_SUBJECT | Only receive a subset of messages from the Stream based on the subject | | false |

### Nsq

| **Environment Variable** | **Description** | **Default** | **Required** |
| ------------------------ | --------------- | ----------- | ------------ |
| PLUMBER_RELAY_NSQ_NSQD_ADDRESS | Address of NSQ server (Ex: localhost:4150) | localhost:4150 | false |
| PLUMBER_RELAY_NSQ_LOOKUPD_ADDRESS | Address of LookupD server (Ex: localhost:4161) |  | false |
| PLUMBER_RELAY_NSQ_USE_TLS | Enable TLS usage |  | false |
| PLUMBER_RELAY_NSQ_SKIP_VERIFY_TLS | Whether to verify server certificate |  | false |
| PLUMBER_RELAY_NSQ_TLS_CA_CERT | CA file |  | false |
| PLUMBER_RELAY_NSQ_TLS_CLIENT_CERT | Client cert file |  | false |
| PLUMBER_RELAY_NSQ_TLS_CLIENT_KEY | Client key file |  | false |
| PLUMBER_RELAY_NSQ_AUTH_SECRET | Authentication secret |  | false |
| PLUMBER_RELAY_NSQ_CLIENT_ID | Client ID to identify as | plumber | false |
| PLUMBER_RELAY_NSQ_TOPIC | NSQ topic to read from |  | **true** |
| PLUMBER_RELAY_NSQ_CHANNEL | Output channel |  | **true** |

### Postgres

| **Environment Variable** | **Description** | **Default** | **Required** |
| ------------------------ | --------------- | ----------- | ------------ |
| PLUMBER_RELAY_CDCPOSTGRES_HOSTNAME | Postgres server hostname |  | **true** |
| PLUMBER_RELAY_CDCPOSTGRES_USERNAME | Postgres port | 5432 | **true** |
| PLUMBER_RELAY_CDCPOSTGRES_USERNAME | Postgres username |  | **true** |
| PLUMBER_RELAY_CDCPOSTGRES_PASSWORD | Postgres server password |  | false |
| PLUMBER_RELAY_CDCPOSTGRES_DATABASE | Postgres server database name |  | **true** |
| PLUMBER_RELAY_CDCPOSTGRES_USE_TLS | Enable TLS usage |  | false |
| PLUMBER_RELAY_CDCPOSTGRES_SKIP_VERIFY_TLS | Whether to verify server certificate |  | false |
| PLUMBER_RELAY_CDCPOSTGRES_SLOT | CDC replication slot name |  | **true** |
| PLUMBER_RELAY_CDCPOSTGRES_PUBLISHER | CDC publisher name |  | **true** |

### Rabbit

| **Environment Variable** | **Description** | **Default** | **Required** |
| ------------------------ | --------------- | ----------- | ------------ |
| PLUMBER_RELAY_RABBIT_ADDRESS | Destination host address (full DSN) | amqp://localhost | **true** |
| PLUMBER_RELAY_RABBIT_USE_TLS | Force TLS usage (regardless of DSN) |  | false |
| PLUMBER_RELAY_RABBIT_SKIP_VERIFY_TLS | Whether to verify server TLS certificate |  | false |
| PLUMBER_RELAY_RABBIT_EXCHANGE | Name of the exchange |  | **true** |
| PLUMBER_RELAY_RABBIT_QUEUE | Name of the queue where messages will be routed to |  | **true** |
| PLUMBER_RELAY_RABBIT_ROUTING_KEY | Binding key for topic based exchanges |  | **true** |
| PLUMBER_RELAY_RABBIT_QUEUE_EXCLUSIVE | Whether plumber should be the only one using the queue |  | false |
| PLUMBER_RELAY_RABBIT_QUEUE_DECLARE | Whether to create/declare the queue (if it does not exist) | true | false |
| PLUMBER_RELAY_RABBIT_QUEUE_DURABLE | Whether the queue should survive after disconnect |  | false |
| PLUMBER_RELAY_RABBIT_AUTOACK | Automatically acknowledge receipt of read/received messages | true | false |
| PLUMBER_RELAY_CONSUMER_TAG | How to identify the consumer to RabbitMQ | plumber | false |
| PLUMBER_RELAY_RABBIT_QUEUE_AUTO_DELETE | Whether to auto-delete the queue after plumber has disconnected | true | false |

### Redis PubSub

| **Environment Variable** | **Description** | **Default** | **Required** |
| ------------------------ | --------------- | ----------- | ------------ |
| PLUMBER_RELAY_REDIS_PUBSUB_ADDRESS | Address of redis server | localhost:6379 | false |
| PLUMBER_RELAY_REDIS_PUBSUB_USERNAME | Optional username to auth with (redis >= v6.0.0) |  | false |
| PLUMBER_RELAY_REDIS_PUBSUB_PASSWORD | Optional password to auth with (redis >= v6.0.0) |  | false |
| PLUMBER_RELAY_REDIS_PUBSUB_DATABASE | Database (0-16) |  | false |
| PLUMBER_RELAY_REDIS_PUBSUB_CHANNELS | Comma separated list of channels to read from |  | **true** |

### Redis Streams

| **Environment Variable** | **Description** | **Default** | **Required** |
| ------------------------ | --------------- | ----------- | ------------ |
| PLUMBER_RELAY_REDIS_STREAMS_ADDRESS | Address of redis server | localhost:6379 | **true** |
| PLUMBER_RELAY_REDIS_STREAMS_USERNAME | Username (redis >= v6.0.0) |  | false |
| PLUMBER_RELAY_REDIS_STREAMS_PASSWORD | Password (redis >= v6.0.0) |  | false |
| PLUMBER_RELAY_REDIS_PUBSUB_DATABASE | Database (0-16) |  | false |
| PLUMBER_RELAY_REDIS_STREAMS_CREATE_STREAMS | Create the streams if creating a new consumer group |  | false |
| PLUMBER_RELAY_REDIS_STREAMS_RECREATE_CONSUMER_GROUP | Recreate this consumer group if it does not exist |  | false |
| PLUMBER_RELAY_REDIS_STREAMS_START_ID | What offset to start reading at (options: latest oldest) | latest | **true** |
| PLUMBER_RELAY_REDIS_STREAMS_STREAMS | Streams to read from |  | **true** |
| PLUMBER_RELAY_REDIS_STREAMS_CONSUMER_GROUP | Consumer group name | plumber | false |
| PLUMBER_RELAY_REDIS_STREAMS_CONSUMER_NAME | Consumer name | plumber-consumer-1 | false |
| PLUMBER_RELAY_REDIS_STREAMS_COUNT | Number of records to read from stream(s) per read | 10 | false |

