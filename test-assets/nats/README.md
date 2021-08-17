# Testing NATS

1. ```bash
   docker run --name nats --rm -p 4222:4222 -p 8222:8222 nats
   ```
   
2. Start Read
    ```bash
    plumber read nats --subject testing
    ```
   
3. Write:
    ```bash
    plumber write nats --subject testing --input-data hello
    ```
