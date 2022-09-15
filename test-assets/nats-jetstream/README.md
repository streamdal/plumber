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


### Testing JWT authentication

Install [nsc](https://github.com/nats-io/nsc) tool 

Create "operator" to handle auth:  
```bash
$ nsc add operator -n memory
```

Create system account: 
```bash
$ nsc add account --name SYS
```

Create user account: 
```bash
$ nsc add account --name A
```

Create user: 
```bash
$ nsc add user --name TA
```

Enable jetstream features on account: 
```bash
$ nsc edit account --name A --allow-pub ">" --js-consumer -1 --js-disk-storage -1 --js-streams -1 --js-mem-storage -1
```

Export server config:

```bash
$ nsc generate config --mem-resolver --config-file /tmp/server.conf
```

Copy relevant server auth configs to `./assets/nats-server.conf` and paste below the `jetstream { ... }` block.
Also add a line for `system_account:` and ensure its value is the account ID for the **SYS** account


```
system_account: ACJEVBKYWJDIYIQCXAOTSKFBPKEDH5PTF2DCXYR5YY2CYAMZ5MDXM7XO

// Operator "memory"
operator: eyJ0eX....

resolver: MEMORY

resolver_preload: {
  // Account "A"
  AAYXDQCWJTO7DHPWIU4LUQTNQ6FI424J2NQWVAUMIJ362S26GYT3FG6M: eyJ0eX......
  
  // Account "SYS"
  ACJEVBKYWJDIYIQCXAOTSKFBPKEDH5PTF2DCXYR5YY2CYAMZ5MDXM7XO: eyJ0eX......
}
```

Fire up nats jetstream:

```bash
$ docker compose up natsjs
```

Verify you can use credentials:

```bash
$ nats stream ls --creds ~/.local/share/nats/nsc/keys/creds/memory/A/TA.creds
```

Use with plumber:

```bash
$ plumber read nats-jetstream \
   --stream events \
   --consumer-name testing1 \
   --consumer-filter-subject events \
   --user-credentials ~/.local/share/nats/nsc/keys/creds/memory/A/TA.creds
```

### Testing NKey authentication

Install [nk](https://github.com/nats-io/nkeys/tree/master/nk) tool

Create key pair:

```bash
$ nk -gen user -pubout
SUAAPPVNUC5H42W3W7GQNQZCSZIDM24DWM4BPJOWW6PV67XRAMDEXWHW4Q
UBZ5DNQ2VZAIC4ZBTKWR2GZTM55RMGE5LPUJPA35VTJALKJRNM73RKDF
```

Add generated user key to `./assets/nats-server.conf`:
```
authorization: {
  users: [
    { nkey: UBZ5DNQ2VZAIC4ZBTKWR2GZTM55RMGE5LPUJPA35VTJALKJRNM73RKDF }
  ]
}
```

You can paste the seed key, denoted by `S` prefix into a file and use that file path for the `--nkey` argument,
or feed the key directly in the argument

```bash
$ plumber read nats-jetstream \
  --stream events \
  --consumer-name testing1 \
  --consumer-filter-subject events \
  --nkey SUAAPPVNUC5H42W3W7GQNQZCSZIDM24DWM4BPJOWW6PV67XRAMDEXWHW4Q
```