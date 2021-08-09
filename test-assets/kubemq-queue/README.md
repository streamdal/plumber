# KubeMQ Queue test setup

1. Register for a key at kubemq.io

2. Start in docker
    ```bash
    docker run -d -p 8080:8080 -p 50000:50000 -p 9090:9090 -e KEY=.... kubemq/kubemq-standalone:latest
    ```
   
3. Write
   ```bash
   plumber write kubemq-queue --queue my-queue --input-data hello
   ```

4. Read
   ```bash
   plumber read kubemq-queue --queue my-queue
   ```