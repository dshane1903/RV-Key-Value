# Chaos Testing

The Week 5 fault-tolerance suite uses in-memory Raft clusters to simulate failure modes without slow process orchestration.

Run it with:

```sh
go test ./raft
```

Run the process-level local smoke test with:

```sh
scripts/smoke-cluster.sh
```

## Covered Scenarios

- Majority partition elects a new leader.
- Isolated old leader steps down after the partition heals.
- Minority partition cannot elect a leader.
- Writes continue in the majority partition.
- Old leader catches up after rejoining.
- Writes halt when the leader loses majority.
- Writes recover when quorum returns.

## Notes

These tests exercise the same Raft APIs used by the gRPC node process:

- `StartElection`
- `ReplicateOnce`
- `ProposeWithRetryInterval`
- `HandleAppendEntries`

Process-level chaos with real node binaries is still a future layer, but the current suite keeps the core consensus invariants fast and deterministic.

## Process Smoke Test

`scripts/smoke-cluster.sh` builds the node binary, starts three local node processes, and verifies:

- a leader is elected
- a write sent to a follower is forwarded and replicated
- the leader can be stopped
- the remaining nodes elect a new leader
- writes continue after failover
- all child processes and temporary data are cleaned up
