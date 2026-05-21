# Protobuf generation

The Raft gRPC API is defined in `raft.proto`.

Generate Go stubs with:

```sh
protoc --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  proto/raft.proto
```

This requires `protoc`, `protoc-gen-go`, and `protoc-gen-go-grpc`.
