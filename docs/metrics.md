### Prometheus Metrics

In relay mode, plumber will launch a http server exposing Prometheus metrics running at http://localhost:8080/metrics

Prometheus metrics can be pulled from Plumber by adding a new source to your `prometheus.yml` config

```yaml
scrape_configs:
- job_name: plumber
  scrape_interval: 5s
  static_configs:
  - targets:
    - your-hostname:8080
```

You may modify the listen address/port using the `PLUMBER_RELAY_HTTP_LISTEN_ADDRESS` environment variable or
the `--listen-address` flag.

The following metrics are available in addition to all golang metrics

| Metric | Type | Description                                                             |
|----|----|-------------------------------------------------------------------------|
| plumber_relay_rate | gauge | Current rare of messages being relayed to Streamdal (5 second interval) |
| plumber_relay_total | counter | Total number of events relayed to Streamdal                             | 
| plumber_read_errors | counter | Number of errors when reading messages                                  |
| plumber_grpc_errors | counter | Number of errors when making GRPC calls                                 |
