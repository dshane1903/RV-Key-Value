# Raft KV Store

A distributed key-value store project built from scratch in Go. The current milestone implements Raft leader election, log replication, committed entry application, and a leader-only HTTP KV API.

## Docker Demo

Start a 3-node cluster with Prometheus and Grafana:

```sh
docker compose up --build
```

The nodes expose HTTP APIs on ports `8001`, `8002`, and `8003`:

```sh
curl -X PUT --data-binary raft http://127.0.0.1:8001/kv/name
curl http://127.0.0.1:8002/kv/name
curl -X DELETE http://127.0.0.1:8003/kv/name
```

Writes sent to followers are forwarded to the known leader once a heartbeat has identified it. Prometheus is available at <http://127.0.0.1:9090>, and Grafana is available at <http://127.0.0.1:3000> with the `Raft KV Cluster` dashboard provisioned automatically.

Metrics exposed by each node at `/metrics` include:

- `raft_leader_elections_total`
- `raft_log_replication_latency_seconds`
- `raft_commit_index`
- `raft_term_current`
- `kv_requests_total`

## Local Demo

Start three nodes in separate terminals:

```sh
go run ./cmd/node --id n1 --raft-addr 127.0.0.1:9001 --http-addr 127.0.0.1:8001 --peers n2=127.0.0.1:9002,n3=127.0.0.1:9003 --peer-http n2=http://127.0.0.1:8002,n3=http://127.0.0.1:8003 --data data/n1.db --kv-data data/n1-kv.db
```

```sh
go run ./cmd/node --id n2 --raft-addr 127.0.0.1:9002 --http-addr 127.0.0.1:8002 --peers n1=127.0.0.1:9001,n3=127.0.0.1:9003 --peer-http n1=http://127.0.0.1:8001,n3=http://127.0.0.1:8003 --data data/n2.db --kv-data data/n2-kv.db
```

```sh
go run ./cmd/node --id n3 --raft-addr 127.0.0.1:9003 --http-addr 127.0.0.1:8003 --peers n1=127.0.0.1:9001,n2=127.0.0.1:9002 --peer-http n1=http://127.0.0.1:8001,n2=http://127.0.0.1:8002 --data data/n3.db --kv-data data/n3-kv.db
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

Writes sent to followers are forwarded to the known leader once a heartbeat has identified it.

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
- Follower write forwarding to the known leader
- Prometheus metrics endpoint
- Docker Compose demo with Prometheus and Grafana

Next:

- Multi-node HTTP integration tests
- Client retries and proposal timeouts
