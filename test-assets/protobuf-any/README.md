# google.protobuf.Any test assets

## Generate payload.bin

```bash
go run generate.go
```

## Re-generate protos

```bash
protoc -I ./ --go_out=paths=source_relative:sample --include_imports --include_source_info -o sample/protos.fds *.proto
```