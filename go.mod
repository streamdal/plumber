module github.com/batchcorp/plumber

go 1.18

replace github.com/v2pro/plz => github.com/batchcorp/plz v0.9.2

require (
	cloud.google.com/go/pubsub v1.6.1
	github.com/Azure/azure-event-hubs-go/v3 v3.3.16
	github.com/Azure/azure-service-bus-go v0.11.5
	github.com/Masterminds/semver v1.5.0
	github.com/apache/pulsar-client-go v0.7.0
	github.com/aws/aws-sdk-go v1.34.28
	github.com/batchcorp/collector-schemas v0.0.22
	github.com/batchcorp/kong v0.2.17-batch-fix
	github.com/batchcorp/natty v0.0.16
	github.com/batchcorp/pgoutput v0.3.2
	github.com/batchcorp/plumber-schemas v0.0.179
	github.com/batchcorp/rabbit v0.1.17
	github.com/batchcorp/thrifty v0.0.10
	github.com/eclipse/paho.mqtt.golang v1.2.0
	github.com/go-redis/redis/v8 v8.11.4
	github.com/go-stomp/stomp v2.1.4+incompatible
	github.com/golang/protobuf v1.5.2
	github.com/google/uuid v1.3.0
	github.com/hokaccha/go-prettyjson v0.0.0-20210113012101-fb4e108d2519
	github.com/imdario/mergo v0.3.13
	github.com/jackc/pgx v3.2.0+incompatible
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
	github.com/prometheus/client_golang v1.11.0
	github.com/rabbitmq/rabbitmq-stream-go-client v1.0.1-rc.2
	github.com/relistan/go-director v0.0.0-20200406104025-dbbf5d95248d
	github.com/satori/go.uuid v1.2.0
	github.com/segmentio/kafka-go v0.4.16
	github.com/sirupsen/logrus v1.8.1
	github.com/streadway/amqp v1.0.0
	github.com/tidwall/gjson v1.9.3
	go.mongodb.org/mongo-driver v1.7.3
	golang.org/x/crypto v0.0.0-20220926161630-eccd6366d1be
	google.golang.org/api v0.29.0
	google.golang.org/grpc v1.40.0
	google.golang.org/protobuf v1.28.1
)

require (
	cloud.google.com/go v0.61.0 // indirect
	github.com/99designs/keyring v1.1.5 // indirect
	github.com/AthenZ/athenz v1.10.15 // indirect
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
	github.com/DataDog/zstd v1.4.6-0.20210211175136-c6db21d202f4 // indirect
	github.com/apache/pulsar-client-go/oauth2 v0.0.0-20201120111947-b8bd55bc02bd // indirect
	github.com/ardielle/ardielle-go v1.5.2 // indirect
	github.com/batchcorp/thrift-iterator v0.0.0-20220918180557-4c4a158fc6e9 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/danieljoos/wincred v1.0.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/devigned/tab v0.1.1 // indirect
	github.com/dgrijalva/jwt-go v3.2.0+incompatible // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/dvsekhvalnov/jose2go v0.0.0-20180829124132-7f401d37b68a // indirect
	github.com/fatih/color v1.12.0 // indirect
	github.com/form3tech-oss/jwt-go v3.2.3+incompatible // indirect
	github.com/fsnotify/fsnotify v1.4.9 // indirect
	github.com/go-stack/stack v1.8.0 // indirect
	github.com/godbus/dbus v0.0.0-20190726142602-4481cbc300e2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/go-cmp v0.5.8 // indirect
	github.com/googleapis/gax-go/v2 v2.0.5 // indirect
	github.com/graph-gophers/graphql-go v1.4.0 // indirect
	github.com/gsterjov/go-libsecret v0.0.0-20161001094733-a6f4afe4910c // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/jstemmer/go-junit-report v0.9.1 // indirect
	github.com/keybase/go-keychain v0.0.0-20190712205309-48d3d31d256d // indirect
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/kubemq-io/protobuf v1.3.1 // indirect
	github.com/lib/pq v1.10.4 // indirect
	github.com/mattn/go-colorable v0.1.8 // indirect
	github.com/mattn/go-isatty v0.0.12 // indirect
	github.com/mattn/go-runewidth v0.0.10 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/mapstructure v1.3.3 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mtibben/percent v0.2.1 // indirect
	github.com/nats-io/nats-streaming-server v0.24.1 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/nxadm/tail v1.4.8 // indirect
	github.com/pierrec/lz4 v2.6.1+incompatible // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.26.0 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/rivo/uniseg v0.2.0 // indirect
	github.com/santhosh-tekuri/jsonschema/v5 v5.1.0 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/stretchr/testify v1.8.0 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/v2pro/plz v0.0.0-20200805122259-422184e41b6e // indirect
	github.com/v2pro/quokka v0.0.0-20171201153428-382cb39c6ee6 // indirect
	github.com/v2pro/wombat v0.0.0-20180402055224-a56dbdcddef2 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.0.2 // indirect
	github.com/xdg-go/stringprep v1.0.2 // indirect
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c // indirect
	github.com/xdg/stringprep v1.0.0 // indirect
	github.com/xtgo/uuid v0.0.0-20140804021211-a0b114877d4c // indirect
	github.com/youmark/pkcs8 v0.0.0-20181117223130-1be2e3e5546d // indirect
	go.opencensus.io v0.22.4 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/thriftrw v1.29.2 // indirect
	golang.org/x/lint v0.0.0-20210508222113-6edffad5e616 // indirect
	golang.org/x/mod v0.6.0-dev.0.20220419223038-86c51ed26bb4 // indirect
	golang.org/x/net v0.0.0-20220722155237-a158d28d115b // indirect
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d // indirect
	golang.org/x/sync v0.0.0-20220722155255-886fb9371eb4 // indirect
	golang.org/x/sys v0.0.0-20220722155257-8c9f86f7a55f // indirect
	golang.org/x/term v0.0.0-20210927222741-03fcf44c2211 // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/tools v0.1.12 // indirect
	google.golang.org/appengine v1.6.6 // indirect
	google.golang.org/genproto v0.0.0-20210602131652-f16073e35f0c // indirect
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	nhooyr.io/websocket v1.8.7 // indirect
)
