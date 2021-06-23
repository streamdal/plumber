# Testing redis-streams

1. docker compose up -d redis
2. telnet localhost 6379
3. Create stream
   ```
   XADD teststream * sensor-id 1234 temperature 19.8
   ```
4. `plumber read redis-streams --streams teststream`