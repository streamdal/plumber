1. Run docker image with jetstream enabled
   
   ```bash
   docker run --rm -p 4222:4222 nats -js
   ```
   
2. Create stream using CLI tools

   ```bash
   nats stream add testing
   ```

   Follow wizard steps. Enter in `testing`for subjects to consume, and select `memory` as storage backend. You can accept the default values for the rest of the options

   ```
   ? Subjects to consume testing
   ? Storage backend memory
   ? Retention Policy Work Queue
   ? Discard Policy Old
   ? Stream Messages Limit -1
   ? Per Subject Messages Limit -1
   ? Message size limit -1
   ? Maximum message age limit -1
   ? Maximum individual message size -1
   ? Duplicate tracking time window 2m0s
   ? Allow message Roll-ups No
   ? Allow message deletion Yes
   ? Allow purging subjects or the entire stream Yes
   ? Replicas 1
   ```

3. Start read:

   ```bash
   plumber read nats-jetstream --stream testing
   ```

   

4. Write:

   ```bash
   plumber write nats-jetstream --stream testing --input hello
   ```
