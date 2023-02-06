# Server Mode

For full documentation, see: https://docs.streamdal.com/plumber/server-mode

Below are  examples on how to run plumber in server mode

### Table of Contents

---

* [Server Mode](#server-mode)
    * [Run in standalone mode](#run-in-standalone-mode)
        * [CLI](#cli)
        * [Docker](#docker)
        * [Kubernetes Deployment](#kubernetes-deployment)
        * [Kubernetes Helm](#kubernetes-helm)
    * [Run in cluster mode](#run-in-cluster-mode)
        * [CLI](#cli-1)
        * [Docker](#docker-1)
        * [Kubernetes Deployment](#kubernetes-deployment-1)
        * [Kubernetes Helm](#kubernetes-helm-1)

## Run in standalone mode

---

### CLI

```bash
# Install plumber
$ brew tap batchcorp/public
...
$ brew install plumber
...

# Launch plumber in standalone mode
$ plumber server
INFO[0000]
█▀█ █   █ █ █▀▄▀█ █▄▄ █▀▀ █▀█
█▀▀ █▄▄ █▄█ █ ▀ █ █▄█ ██▄ █▀▄
INFO[0000] starting plumber server in 'standalone' mode...  pkg=plumber
INFO[0005] plumber server started                        pkg=plumber
```


### Docker

```bash
$ docker run --name plumber-server -p 8080:8080 \
    -v plumber-config:/Users/username/.batchsh:rw \
    batchcorp/plumber:latest
```

### Kubernetes Deployment

```yaml
---
apiVersion: v1
kind: Service
metadata:
  name: plumber-standalone
  labels:
    app.kubernetes.io/name: plumber-standalone
spec:
  type: ClusterIP
  ports:
    - port: 9090
      targetPort: 9090
      protocol: TCP
      name: grpc-api
  selector:
    app.kubernetes.io/name: plumber-standalone
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: plumber-standalone
  labels:
    app.kubernetes.io/name: plumber-standalone
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: plumber-standalone
  template:
    metadata:
      labels:
        app.kubernetes.io/name: plumber-standalone
    spec:
      containers:
        - name: plumber-standalone
          image: "batchcorp/plumber:v1.4.0"
          imagePullPolicy: IfNotPresent
          command: ["/plumber-linux", "server"]
          ports:
            - containerPort: 9090
          env:
            - name: PLUMBER_SERVER_ENABLE_CLUSTER
              value: "false"
            - name: PLUMBER_SERVER_NODE_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name

```

### Kubernetes Helm

Follow the `README.md` in https://github.com/batchcorp/plumber-helm#standalone


## Run in cluster mode

---

### CLI
```bash
# Install plumber
$ brew tap batchcorp/public
...
$ brew install plumber
...

# Git clone plumber repo (to get access to its docker-compose + assets)
$ git clone git@github.com:batchcorp/plumber.git
$ cd plumber

# Launch a NATS container
$ docker-compose up -d natsjs

# Launch plumber in cluster mode 
$ plumber server --enable-cluster
INFO[0000]
█▀█ █   █ █ █▀▄▀█ █▄▄ █▀▀ █▀█
█▀▀ █▄▄ █▄█ █ ▀ █ █▄█ ██▄ █▀▄
INFO[0000] starting plumber server in 'cluster' mode...  pkg=plumber
INFO[0015] plumber server started                        pkg=plumber
```

### Docker

```bash
# Git clone plumber repo (to get access to its docker-compose + assets)
$ git clone git@github.com:batchcorp/plumber.git
$ cd plumber

# Launch a NATS container
$ docker-compose up -d natsjs

# Launch a plumber container that points to your NATS instance
$ docker run --name plumber-server -p 9090:9090 \
    --network plumber_default \
    -e PLUMBER_SERVER_NATS_URL=nats://natsjs \
    -e PLUMBER_SERVER_ENABLE_CLUSTER=true \
    batchcorp/plumber:latest
{"level":"info","msg":"starting plumber server in 'cluster' mode...","pkg":"plumber","time":"2022-02-15T20:21:47Z"}
{"level":"info","msg":"plumber server started","pkg":"plumber","time":"2022-02-15T20:22:02Z"}
```

### Kubernetes Deployment

```yaml
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: nats-config
  namespace: default
  labels:
    app.kubernetes.io/name: nats
data:
  nats.conf: |
    # NATS Clients Port
    port: 4222

    # PID file shared with configuration reloader.
    pid_file: "/var/run/nats/nats.pid"
    http: 8222
    server_name:$POD_NAME
    jetstream {
      max_mem: 1Gi
      store_dir: /data

      max_file:5Gi
    }
    lame_duck_duration: 120s
---
apiVersion: v1
kind: Service
metadata:
  name: nats
  namespace: default
  labels:
    app.kubernetes.io/name: nats
spec:
  selector:
    app.kubernetes.io/name: nats
  clusterIP: None
  ports:
  - name: client
    port: 4222
  - name: cluster
    port: 6222
  - name: monitor
    port: 8222
  - name: metrics
    port: 7777
  - name: leafnodes
    port: 7422
  - name: gateways
    port: 7522
---
apiVersion: v1
kind: Service
metadata:
  name: plumber-cluster
  labels:
    app.kubernetes.io/name: plumber-cluster
spec:
  type: ClusterIP
  ports:
    - port: 9090
      targetPort: 9090
      protocol: TCP
      name: grpc-api
  selector:
    app.kubernetes.io/name: plumber-cluster
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nats-box
  namespace: default
  labels:
    app: nats-box
    chart: nats-0.13.0
spec:
  replicas: 1
  selector:
    matchLabels:
      app: nats-box
  template:
    metadata:
      labels:
        app: nats-box
    spec:
      volumes:
      containers:
      - name: nats-box
        image: natsio/nats-box:0.8.1
        imagePullPolicy: IfNotPresent
        resources:
          null
        env:
        - name: NATS_URL
          value: nats
        command:
         - "tail"
         - "-f"
         - "/dev/null"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: plumber-cluster
  labels:
    app.kubernetes.io/name: plumber-cluster
spec:
  replicas: 3
  selector:
    matchLabels:
      app.kubernetes.io/name: plumber-cluster
  template:
    metadata:
      labels:
        app.kubernetes.io/name: plumber-cluster
    spec:
      containers:
        - name: plumber-cluster
          image: "batchcorp/plumber:v1.4.0"
          imagePullPolicy: IfNotPresent
          command: ["/plumber-linux", "server"]
          ports:
            - containerPort: 9090
          env:
            - name: PLUMBER_SERVER_CLUSTER_ID
              value: "7EB6C7FB-9053-41B4-B456-78E64CF9D393"
            - name: PLUMBER_SERVER_ENABLE_CLUSTER
              value: "true"
            - name: PLUMBER_SERVER_NATS_URL
              value: "nats://nats.default.svc.cluster.local:4222"
            - name: PLUMBER_SERVER_USE_TLS
              value: "false"
            - name: PLUMBER_SERVER_NODE_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: nats
  namespace: default
  labels:
    app.kubernetes.io/name: nats
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: nats
  replicas: 1
  serviceName: nats
  podManagementPolicy: Parallel

  template:
    metadata:
      annotations:
        prometheus.io/path: /metrics
        prometheus.io/port: "7777"
        prometheus.io/scrape: "true"
      labels:
        app.kubernetes.io/name: nats
    spec:
      # Common volumes for the containers.
      volumes:
      - name: config-volume
        configMap:
          name: nats-config
      # Local volume shared with the reloader.
      - name: pid
        emptyDir: {}
      # Required to be able to HUP signal and apply config
      # reload to the server without restarting the pod.
      shareProcessNamespace: true
      terminationGracePeriodSeconds: 120
      containers:
      - name: nats
        image: nats:2.7.2-alpine
        imagePullPolicy: IfNotPresent
        ports:
        - containerPort: 4222
          name: client
        - containerPort: 7422
          name: leafnodes
        - containerPort: 7522
          name: gateways
        - containerPort: 6222
          name: cluster
        - containerPort: 8222
          name: monitor
        - containerPort: 7777
          name: metrics
        command:
         - "nats-server"
         - "--config"
         - "/etc/nats-config/nats.conf"
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: SERVER_NAME
          value: $(POD_NAME)
        - name: POD_NAMESPACE
          valueFrom:
            fieldRef:
              fieldPath: metadata.namespace
        - name: CLUSTER_ADVERTISE
          value: $(POD_NAME).nats.$(POD_NAMESPACE).svc.cluster.local
        volumeMounts:
          - name: config-volume
            mountPath: /etc/nats-config
          - name: pid
            mountPath: /var/run/nats
          - name: nats-js-pvc
            mountPath: /data
        livenessProbe:
          httpGet:
            path: /
            port: 8222
          initialDelaySeconds: 10
          timeoutSeconds: 5
          periodSeconds: 60
          successThreshold: 1
          failureThreshold: 3
        startupProbe:
          httpGet:
            path: /
            port: 8222
          initialDelaySeconds: 10
          timeoutSeconds: 5
          periodSeconds: 10
          successThreshold: 1
          failureThreshold: 30

        # Gracefully stop NATS Server on pod deletion or image upgrade.
        #
        lifecycle:
          preStop:
            exec:
              # Using the alpine based NATS image, we add an extra sleep that is
              # the same amount as the terminationGracePeriodSeconds to allow
              # the NATS Server to gracefully terminate the client connections.
              #
              command:
              - "/bin/sh"
              - "-c"
              - "nats-server -sl=ldm=/var/run/nats/nats.pid"
      - name: reloader
        image: natsio/nats-server-config-reloader:0.6.2
        imagePullPolicy: IfNotPresent
        resources:
          null
        command:
         - "nats-server-config-reloader"
         - "-pid"
         - "/var/run/nats/nats.pid"
         - "-config"
         - "/etc/nats-config/nats.conf"
        volumeMounts:
          - name: config-volume
            mountPath: /etc/nats-config
          - name: pid
            mountPath: /var/run/nats
      - name: metrics
        image: natsio/prometheus-nats-exporter:0.9.1
        imagePullPolicy: IfNotPresent
        args:
        - -connz
        - -routez
        - -subz
        - -varz
        - -prefix=nats
        - -use_internal_server_id
        - -jsz=all
        - http://localhost:8222/
        ports:
        - containerPort: 7777
          name: metrics


  volumeClaimTemplates:
    - metadata:
        name: nats-js-pvc
      spec:
        accessModes:
          - "ReadWriteOnce"
        resources:
          requests:
            storage: 5Gi
---
apiVersion: v1
kind: Pod
metadata:
  name: "nats-test-request-reply"
  labels:
    app.kubernetes.io/name: nats
  annotations:
    "hook": test
spec:
  containers:
    - name: nats-box
      image: synadia/nats-box
      env:
        - name: NATS_HOST
          value: nats
      command:
        - /bin/sh
        - -ec
        - |
          nats reply -s nats://$NATS_HOST:4222 'name.>' --command "echo 1" &
        - |
          "&&"
        - |
          name=$(nats request -s nats://$NATS_HOST:4222 name.test '' 2>/dev/null)
        - |
          "&&"
        - |
          [ $name = test ]

  restartPolicy: Never

```

### Kubernetes Helm

Follow the `README.md` in https://github.com/batchcorp/plumber-helm#cluster-mode