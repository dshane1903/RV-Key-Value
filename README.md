# Raft KV Store

A distributed key-value store project built from scratch in Go. The current milestone implements Raft leader election, log replication, committed entry application, and a leader-only HTTP KV API.

## Current Demo

Start three nodes in separate terminals:

```sh
go run ./cmd/node --id n1 --raft-addr 127.0.0.1:9001 --http-addr 127.0.0.1:8001 --peers n2=127.0.0.1:9002,n3=127.0.0.1:9003 --data data/n1.db --kv-data data/n1-kv.db
```

```sh
go run ./cmd/node --id n2 --raft-addr 127.0.0.1:9002 --http-addr 127.0.0.1:8002 --peers n1=127.0.0.1:9001,n3=127.0.0.1:9003 --data data/n2.db --kv-data data/n2-kv.db
```

```sh
go run ./cmd/node --id n3 --raft-addr 127.0.0.1:9003 --http-addr 127.0.0.1:8003 --peers n1=127.0.0.1:9001,n2=127.0.0.1:9002 --data data/n3.db --kv-data data/n3-kv.db
```

Watch the logs for lines like:

```text
node n2 state=leader term=4 voted_for="n2"
```

Stop the leader with `Ctrl+C`. The remaining two nodes should elect a new leader at a higher term.

Write to the leader's HTTP port:

```sh
curl -X PUT --data-binary raft http://127.0.0.1:8001/kv/name
curl http://127.0.0.1:8001/kv/name
curl -X DELETE http://127.0.0.1:8001/kv/name
```

Writes sent to followers currently return `409 Conflict`.

## Development

Run tests:

```sh
go test ./...
```

Generate protobuf stubs:

```sh
make proto
```

## Status

Implemented:

- Raft persistent state and log data structures
- bbolt-backed stable storage
- `RequestVote` and `AppendEntries` protobuf/gRPC definitions
- Leader election rules
- Election timers
- Leader heartbeats
- gRPC transport adapter
- Runnable node process
- Log replication with `nextIndex` and `matchIndex`
- Commit index advancement
- KV state machine backed by bbolt
- Leader-only HTTP `PUT`, `GET`, and `DELETE`

Next:

- Leader forwarding for follower write requests
- Multi-node HTTP integration tests
- Client retries and proposal timeouts
