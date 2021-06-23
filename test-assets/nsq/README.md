# Testing nsq

1. `docker compose up -d`
2. Go to http://localhost:4171/lookup and create topic `testing` and channel `plumber`
3. Write data
   ```bash
   plumber write nsq --nsqd-address localhost:4150 --topic testing --input-data hello
   ```
4. Read data
   ```bash
   plumber read nsq --nsqd-address localhost:4150 --topic testing --channel plumber
   ```