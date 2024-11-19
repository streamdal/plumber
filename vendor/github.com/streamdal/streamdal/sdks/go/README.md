Streamdal Go SDK
================
[![Release](https://github.com/streamdal/streamdal/actions/workflows/sdks-go-release.yml/badge.svg)](https://github.com/streamdal/streamdal/actions/workflows/sdks-go-release.yml)
[![Pull Request](https://github.com/streamdal/streamdal/actions/workflows/sdks-go-pr.yml/badge.svg)](https://github.com/streamdal/streamdal/blob/main/.github/workflows/sdks-go-pr.yml)
[![Discord](https://img.shields.io/badge/Community-Discord-4c57e8.svg)](https://discord.gg/streamdal)
[![Go Report Card](https://goreportcard.com/badge/github.com/streamdal/streamdal/sdks/go)](https://goreportcard.com/report/github.com/streamdal/streamdal/sdks/go)
[![codecov](https://codecov.io/github/streamdal/streamdal/graph/badge.svg?token=yYbG9PCM2k&flag=go-sdk)](https://app.codecov.io/github/streamdal/streamdal?flags[0]=go-sdk)

_**Golang SDK for [Streamdal](https://streamdal.com).**_

<sub>For more details, see the main
[streamdal repo](https://github.com/streamdal/streamdal).</sub>

---

### Documentation

See https://docs.streamdal.com

### Installation

```bash
go get github.com/streamdal/streamdal/sdks/go
```

### Example Usage

```go
package main

import (
	"fmt"
	"time"

	streamdal "github.com/streamdal/streamdal/sdks/go"
)

func main() {
	sc, _ := streamdal.New(&streamdal.Config{
		// Address of the streamdal server + gRPC API port
		ServerURL: "streamdal-server-address:8082",
		
		// Token used for authenticating with the streamdal server
		ServerToken: "1234",

		// Identify _this_ application/service (
		ServiceName: "billing-svc",
	})

	resp := sc.Process(context.Background(), &streamdal.ProcessRequest{
		OperationType: streamdal.OperationTypeConsumer,
		OperationName: "new-order-topic",
		ComponentName: "kafka",
		Data:          []byte(`{"object": {"field": true}}`),
	})

	// Check if the .Process() call completed
	if resp.Status != streamdal.ExecStatusError {
		fmt.Println("Successfully processed payload")
	}

	// Or you can inspect each individual pipeline & step result
	for _, pipeline := range resp.PipelineStatus {
		fmt.Printf("Inspecting '%d' steps in pipeline '%s'...\n", len(resp.PipelineStatus), pipeline.Name)

		for _, step := range pipeline.StepStatus {
			fmt.Printf("Step '%s' status: '%s'\n", step.Name, step.Status)
		}
	}

	// The SDK needs ~3 seconds to start up which will perform initial registration
	// with server + pull pipelines. Since this is not a long running app, we
	// want to prevent a fast exit.
	time.Sleep(5 * time.Second)
}
```

### Configuration

All configuration can be passed via `streamdal.Config{}`. Some values can be set via environment variables in 
order to support 12-Factor and usage of this SDK inside shims where `streamdal.Config{}` cannot be set.

NOTE: For the most up to date config options, refer to [go_sdk.go:Config](./go_sdk.go).

| Config Parameter | Environment Variable       | Description                                                                      | Default       |
|------------------|----------------------------|----------------------------------------------------------------------------------|---------------|
| ServerURL        | STREAMDAL_URL              | URL pointing to your instance of streamdal server's gRPC API. Ex: localhost:8082 | *empty*       |
| ServerToken      | STREAMDAL_TOKEN            | API token set in streamdal server                                                | *empty*       |
| ServiceName      | STREAMDAL_SERVICE_NAME     | Identifies this service in the streamdal console                                 | *empty*       |
| PipelineTimeout  | STREAMDAL_PIPELINE_TIMEOUT | Maximum time a pipeline can run before giving up                                 | 100ms         |
| StepTimeout      | STREAMDAL_STEP_TIMEOUT     | Maximum time a pipeline step can run before giving up                            | 10ms          |
| DryRun           | STREAMDAL_DRY_RUN          | If true, no data will be modified                                                | *false*       |
| Logger           |                            | An optional custom logger                                                        |               |
| ClientType       |                            | 1 = ClientTypeSDK, 2 = ClientTypeShim                                            | ClientTypeSDK |
| ShutdownCtx      | -                          | Optional context used by lib to detect shutdowns              |      *empty*         |

### Metrics

Metrics are published to Streamdal server and are available in Prometheus format at http://streamdal_server_url:8081/metrics

| Metric                                       | Description                                      | Labels                                                                        |
|----------------------------------------------|--------------------------------------------------|-------------------------------------------------------------------------------|
| `streamdal_counter_consume_bytes`     | Number of bytes consumed by the client     | `service`, `component_name`, `operation_name`, `pipeline_id`, `pipeline_name` |
| `streamdal_counter_consume_errors`    | Number of errors encountered while consuming payloads | `service`, `component_name`, `operation_name`, `pipeline_id`, `pipeline_name` |
| `streamdal_counter_consume_processed` | Number of payloads processed by the client | `service`, `component_name`, `operation_name`, `pipeline_id`, `pipeline_name` |
| `streamdal_counter_produce_bytes`     | Number of bytes produced by the client     | `service`, `component_name`, `operation_name`, `pipeline_id`, `pipeline_name` |
| `streamdal_counter_produce_errors`    | Number of errors encountered while producing payloads | `service`, `component_name`, `operation_name`, `pipeline_id`, `pipeline_name` |
| `streamdal_counter_produce_processed` | Number of payloads processed by the client | `service`, `component_name`, `operation_name`, `pipeline_id`, `pipeline_name` |
| `streamdal_counter_notify`            | Number of notifications sent to the server | `service`, `component_name`, `operation_name`, `pipeline_id`, `pipeline_name` |

NOTE: Metrics are **required** - they are used by the server and Console for a
number of different features such as determining whether clients are alive and
can accept "commands" and Tail requests from the Console or for displaying
throughput rates for clients in the UI.

You can tune the number of `metrics workers` that the SDK spawns by setting
`go_sdk.go:Config.MetricsWorkers`.

## Release

Any push or merge to the `main` branch with any changes in `/sdks/go/*` will 
automatically tag and release a new SDK version with `sdks/go/vX.Y.Z`.

<sub>(1) If you'd like to skip running the release action on push/merge to `main`,
include `norelease` anywhere in the commit message.</sub>
