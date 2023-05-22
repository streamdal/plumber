module github.com/batchcorp/plumber

go 1.18

replace github.com/v2pro/plz => github.com/batchcorp/plz v0.9.2

require (
	cloud.google.com/go/pubsub v1.6.1
	github.com/Azure/azure-event-hubs-go/v3 v3.3.16
	github.com/Azure/azure-service-bus-go v0.11.5
	github.com/Masterminds/semver v1.5.0
	github.com/apache/pulsar-client-go v0.10.0
	github.com/aws/aws-sdk-go v1.34.28
	github.com/batchcorp/collector-schemas v0.0.22
	github.com/batchcorp/kong v0.2.17-batch-fix
	github.com/batchcorp/natty v0.0.16
	github.com/batchcorp/plumber-schemas v0.0.182-0.20230522140832-73b38dbcaa3c
	github.com/batchcorp/rabbit v0.1.17
	github.com/batchcorp/thrifty v0.0.10
	github.com/eclipse/paho.mqtt.golang v1.2.0
	github.com/go-redis/redis/v8 v8.11.4
	github.com/go-stomp/stomp v2.1.4+incompatible
	github.com/golang/protobuf v1.5.3
	github.com/google/uuid v1.3.0
	github.com/hokaccha/go-prettyjson v0.0.0-20210113012101-fb4e108d2519
	github.com/imdario/mergo v0.3.13
	github.com/jackc/pgx v3.6.2+incompatible
	github.com/jhump/protoreflect v1.10.1
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/julienschmidt/httprouter v1.3.0
	github.com/kataras/tablewriter v0.0.0-20180708051242-e063d29b7c23
	github.com/kubemq-io/kubemq-go v1.7.2
	github.com/lensesio/tableprinter v0.0.0-20201125135848-89e81fc956e7
	github.com/linkedin/goavro/v2 v2.9.8
	github.com/logrusorgru/aurora v2.0.3+incompatible
	github.com/maxbrunsfeld/counterfeiter/v6 v6.3.0
	github.com/mcuadros/go-lookup v0.0.0-20200831155250-80f87a4fa5ee
	github.com/memphisdev/memphis.go v0.2.0
	github.com/nats-io/nats.go v1.19.0
	github.com/nats-io/nkeys v0.3.0
	github.com/nats-io/stan.go v0.10.2
	github.com/nsqio/go-nsq v1.0.8
	github.com/olekukonko/tablewriter v0.0.5
	github.com/onsi/ginkgo v1.16.5
	github.com/onsi/gomega v1.20.2
	github.com/pkg/errors v0.9.1
	github.com/posthog/posthog-go v0.0.0-20220817142604-0b0bbf0f9c0f
	github.com/prometheus/client_golang v1.11.1
	github.com/rabbitmq/rabbitmq-stream-go-client v1.0.1-rc.2
	github.com/relistan/go-director v0.0.0-20200406104025-dbbf5d95248d
	github.com/satori/go.uuid v1.2.0
	github.com/segmentio/kafka-go v0.4.16
	github.com/sirupsen/logrus v1.8.1
	github.com/streadway/amqp v1.0.0
	github.com/streamdal/pgoutput v0.3.3
	github.com/tidwall/gjson v1.9.3
	go.mongodb.org/mongo-driver v1.7.3
	golang.org/x/crypto v0.8.0
	google.golang.org/api v0.30.0
	google.golang.org/grpc v1.40.0
	google.golang.org/protobuf v1.30.0
)

require (
	github.com/Shopify/sarama v1.38.1
	github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2 v2.13.0
	github.com/cloudevents/sdk-go/protocol/nats/v2 v2.13.0
	github.com/cloudevents/sdk-go/v2 v2.13.0
)

require (
	cloud.google.com/go v0.65.0 // indirect
	github.com/99designs/go-keychain v0.0.0-20191008050251-8e49817e8af4 // indirect
	github.com/99designs/keyring v1.2.1 // indirect
	github.com/AthenZ/athenz v1.10.39 // indirect
	github.com/Azure/azure-amqp-common-go/v3 v3.2.1 // indirect
	github.com/Azure/azure-sdk-for-go v51.1.0+incompatible // indirect
	github.com/Azure/go-amqp v0.16.4 // indirect
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest v0.11.18 // indirect
	github.com/Azure/go-autorest/autorest/adal v0.9.13 // indirect
	github.com/Azure/go-autorest/autorest/date v0.3.0 // indirect
	github.com/Azure/go-autorest/autorest/to v0.4.0 // indirect
	github.com/Azure/go-autorest/autorest/validation v0.3.1 // indirect
	github.com/Azure/go-autorest/logger v0.2.1 // indirect
	github.com/Azure/go-autorest/tracing v0.6.0 // indirect
	github.com/DataDog/zstd v1.5.0 // indirect
	github.com/ardielle/ardielle-go v1.5.2 // indirect
	github.com/batchcorp/thrift-iterator v0.0.0-20220918180557-4c4a158fc6e9 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.4.0 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/danieljoos/wincred v1.1.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/devigned/tab v0.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/dvsekhvalnov/jose2go v1.5.0 // indirect
	github.com/eapache/go-resiliency v1.3.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20230111030713-bf00bc1b83b6 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/fatih/color v1.12.0 // indirect
	github.com/form3tech-oss/jwt-go v3.2.3+incompatible // indirect
	github.com/fsnotify/fsnotify v1.4.9 // indirect
	github.com/go-stack/stack v1.8.0 // indirect
	github.com/godbus/dbus v0.0.0-20190726142602-4481cbc300e2 // indirect
	github.com/gofrs/uuid v4.4.0+incompatible // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt v3.2.1+incompatible // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/go-cmp v0.5.8 // indirect
	github.com/googleapis/gax-go/v2 v2.0.5 // indirect
	github.com/graph-gophers/graphql-go v1.4.0 // indirect
	github.com/gsterjov/go-libsecret v0.0.0-20161001094733-a6f4afe4910c // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.3 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/jstemmer/go-junit-report v0.9.1 // indirect
	github.com/klauspost/compress v1.15.14 // indirect
	github.com/kubemq-io/protobuf v1.3.1 // indirect
	github.com/lib/pq v1.10.4 // indirect
	github.com/mattn/go-colorable v0.1.8 // indirect
	github.com/mattn/go-isatty v0.0.12 // indirect
	github.com/mattn/go-runewidth v0.0.10 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/mitchellh/mapstructure v1.3.3 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mtibben/percent v0.2.1 // indirect
	github.com/nats-io/nats-streaming-server v0.24.1 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/nxadm/tail v1.4.8 // indirect
	github.com/pierrec/lz4 v2.6.1+incompatible // indirect
	github.com/pierrec/lz4/v4 v4.1.17 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.26.0 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/rivo/uniseg v0.2.0 // indirect
	github.com/rogpeppe/go-internal v1.9.0 // indirect
	github.com/santhosh-tekuri/jsonschema/v5 v5.1.0 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/stretchr/objx v0.5.0 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/v2pro/plz v0.0.0-20200805122259-422184e41b6e // indirect
	github.com/v2pro/quokka v0.0.0-20171201153428-382cb39c6ee6 // indirect
	github.com/v2pro/wombat v0.0.0-20180402055224-a56dbdcddef2 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c // indirect
	github.com/xdg/stringprep v1.0.0 // indirect
	github.com/xtgo/uuid v0.0.0-20140804021211-a0b114877d4c // indirect
	github.com/youmark/pkcs8 v0.0.0-20181117223130-1be2e3e5546d // indirect
	go.opencensus.io v0.22.4 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.1.0 // indirect
	go.uber.org/thriftrw v1.29.2 // indirect
	go.uber.org/zap v1.10.0 // indirect
	golang.org/x/lint v0.0.0-20210508222113-6edffad5e616 // indirect
	golang.org/x/mod v0.8.0 // indirect
	golang.org/x/net v0.9.0 // indirect
	golang.org/x/oauth2 v0.0.0-20210402161424-2e8d93401602 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/sys v0.7.0 // indirect
	golang.org/x/term v0.7.0 // indirect
	golang.org/x/text v0.9.0 // indirect
	golang.org/x/time v0.0.0-20211116232009-f0f3c7e86c11 // indirect
	golang.org/x/tools v0.6.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20210602131652-f16073e35f0c // indirect
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	nhooyr.io/websocket v1.8.7 // indirect
)
