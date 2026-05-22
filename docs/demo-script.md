# Demo Script

This is a short portfolio demo flow designed to fit in about two minutes.

## Prep

Start from a clean terminal in the repo root:

```sh
docker compose up --build
```

Wait for the logs to show one leader:

```text
node n1 state=leader term=...
```

If you want a separate terminal for commands:

```sh
docker compose logs -f n1 n2 n3
```

## Walkthrough

1. Show the cluster is running.

```sh
docker compose ps
```

2. Write a key through any node.

```sh
curl -X PUT --data-binary "hello raft" http://127.0.0.1:8001/kv/demo
```

3. Read the key from a different node.

```sh
curl http://127.0.0.1:8002/kv/demo
```

4. Stop the current leader.

```sh
docker compose stop n1
```

Use the node ID that is actually leader in the logs.

5. Watch a new leader get elected.

```sh
docker compose logs --tail=80 n2 n3
```

6. Write while the cluster is reduced to two nodes.

```sh
curl -X PUT --data-binary "after failover" http://127.0.0.1:8002/kv/failover
curl http://127.0.0.1:8003/kv/failover
```

7. Bring the old leader back.

```sh
docker compose start n1
```

8. Open Grafana.

Go to <http://127.0.0.1:3000> and open `Raft KV Cluster`. Point out:

- current term
- commit index per node
- leader election count
- KV request rate
- replication latency

## Cleanup

```sh
docker compose down
```

Use `-v` if you want to remove node data volumes too:

```sh
docker compose down -v
```
