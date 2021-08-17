# Testing nats-streaming

1. Run docker streaming image
   ```bash
   docker run -p 4222:4222 -p 8222:8222 nats-streaming 
   ```
   
2. Start read
   ```bash
   plumber read nats-streaming --channel testing --cluster-id test-cluster --client-id plumber
   ```

3. Write data
   ```bash
   plumber write nats-streaming --channel testing --cluster-id test-cluster --client-id plumber-writer --input-data hello
   ```
   
Cluster ID is displayed when running the docker image. Default should be `test-cluster`


```
$  docker run -p 4222:4222 -p 8222:8222 nats-streaming
[1] 2021/08/17 19:02:04.905632 [INF] STREAM: Starting nats-streaming-server[test-cluster] version 0.22.1
```