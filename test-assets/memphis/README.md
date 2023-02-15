# Testing Memphis

1. `docker compose -f docker-compose.yml -p memphis up`
   
   [For more detailed information about Memphis docker deployment](/memphis/deployment/docker-compose).`

2. Navigate to http://localhost:9000

3. Login as `root` with password `memphis`

4. Create Station `plumbertest`

5. `plumber read memphis --station plumbertest`

6. `plumber write memphis --station plumbertest --input "hello world"`


