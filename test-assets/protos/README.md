These proto schemas are a copy of Batch's internal protobuf schemas used for
inter-service communication. 

These schemas represent a medium-complexity schema-set which _should_ work with
`plumber`. If they do not, there's _probably_ a problem.

NOTE: The following commands were ran to generate the protobuf go files:

```bash
$ protoc -I protos --go_out=paths=source_relative:pbs protos/*.proto
$ protoc -I protos --go_out=paths=source_relative:pbs protos/records/*.proto
$ protoc -I protos --go_out=paths=source_relative:pbs protos/destinations/*.proto
```
