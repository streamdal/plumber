# Testing rabbit-streams

1. Start container
   ```bash
   docker run -it --rm --name rabbitmq-streams -p 5552:5552 -e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="-rabbitmq_stream advertised_host localhost" pivotalrabbitmq/rabbitmq-stream
   ```
   
2. Start read
   ```bash
   plumber read rabbit-streams --declare-stream --declare-stream-size 10mb --stream testing
   ```

3. Write
   ```bash
   plumber write rabbit-streams --stream testing --input-data hello
   ```