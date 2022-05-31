# Shallow envelope test assets

## Generate payload.bin

```bash
go run generate.go
```

## Re-generate protos

```bash
protoc -I ./ --go_out=paths=source_relative:shallow *.proto
```