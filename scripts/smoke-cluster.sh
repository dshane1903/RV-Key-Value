#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="${TMPDIR:-/tmp}/rv-kv-smoke-$$"
BIN="$TMP_DIR/rv-node"
PIDS=""

cleanup() {
	for pid in $PIDS; do
		kill "$pid" >/dev/null 2>&1 || true
	done
	for pid in $PIDS; do
		wait "$pid" >/dev/null 2>&1 || true
	done
	rm -rf "$TMP_DIR"
}
trap cleanup EXIT INT TERM

mkdir -p "$TMP_DIR"

cd "$ROOT_DIR"
go build -o "$BIN" ./cmd/node

raft_addr() {
	case "$1" in
	n1) printf "127.0.0.1:9601" ;;
	n2) printf "127.0.0.1:9602" ;;
	n3) printf "127.0.0.1:9603" ;;
	*) return 1 ;;
	esac
}

http_addr() {
	case "$1" in
	n1) printf "127.0.0.1:8601" ;;
	n2) printf "127.0.0.1:8602" ;;
	n3) printf "127.0.0.1:8603" ;;
	*) return 1 ;;
	esac
}

http_url() {
	printf "http://%s" "$(http_addr "$1")"
}

peers_for() {
	case "$1" in
	n1) printf "n2=%s,n3=%s" "$(raft_addr n2)" "$(raft_addr n3)" ;;
	n2) printf "n1=%s,n3=%s" "$(raft_addr n1)" "$(raft_addr n3)" ;;
	n3) printf "n1=%s,n2=%s" "$(raft_addr n1)" "$(raft_addr n2)" ;;
	*) return 1 ;;
	esac
}

peer_http_for() {
	case "$1" in
	n1) printf "n2=%s,n3=%s" "$(http_url n2)" "$(http_url n3)" ;;
	n2) printf "n1=%s,n3=%s" "$(http_url n1)" "$(http_url n3)" ;;
	n3) printf "n1=%s,n2=%s" "$(http_url n1)" "$(http_url n2)" ;;
	*) return 1 ;;
	esac
}

log_file() {
	printf "%s/%s.log" "$TMP_DIR" "$1"
}

start_node() {
	id="$1"
	"$BIN" \
		--id "$id" \
		--raft-addr "$(raft_addr "$id")" \
		--http-addr "$(http_addr "$id")" \
		--peers "$(peers_for "$id")" \
		--peer-http "$(peer_http_for "$id")" \
		--data "$TMP_DIR/$id.db" \
		--kv-data "$TMP_DIR/$id-kv.db" \
		> "$(log_file "$id")" 2>&1 &
	PIDS="$PIDS $!"
	printf "started %s pid=%s\n" "$id" "$!"
}

current_state() {
	id="$1"
	grep "node $id state=" "$(log_file "$id")" 2>/dev/null | tail -n 1 | sed -n 's/.*state=\([^ ]*\).*/\1/p'
}

wait_for_leader() {
	excluded="${1:-}"
	deadline=$((SECONDS + 20))
	while [ "$SECONDS" -lt "$deadline" ]; do
		for id in n1 n2 n3; do
			if [ "$id" = "$excluded" ]; then
				continue
			fi
			if [ "$(current_state "$id")" = "leader" ]; then
				printf "%s" "$id"
				return 0
			fi
		done
		sleep 0.2
	done
	printf "timed out waiting for leader\n" >&2
	return 1
}

pid_for() {
	id="$1"
	index=0
	for pid in $PIDS; do
		index=$((index + 1))
		case "$id:$index" in
		n1:1 | n2:2 | n3:3)
			printf "%s" "$pid"
			return 0
			;;
		esac
	done
	return 1
}

follower_for() {
	leader="$1"
	dead="${2:-}"
	for id in n1 n2 n3; do
		if [ "$id" != "$leader" ] && [ "$id" != "$dead" ]; then
			printf "%s" "$id"
			return 0
		fi
	done
	return 1
}

curl_status() {
	curl -s -o "$TMP_DIR/curl.out" -w "%{http_code}" "$@"
}

wait_for_put() {
	id="$1"
	key="$2"
	value="$3"
	deadline=$((SECONDS + 10))
	while [ "$SECONDS" -lt "$deadline" ]; do
		status="$(curl_status -X PUT --data-binary "$value" "$(http_url "$id")/kv/$key" || true)"
		if [ "$status" = "204" ]; then
			return 0
		fi
		sleep 0.2
	done
	printf "timed out waiting for PUT %s through %s; last status=%s body=%s\n" "$key" "$id" "$status" "$(cat "$TMP_DIR/curl.out" 2>/dev/null || true)" >&2
	return 1
}

wait_for_value() {
	id="$1"
	key="$2"
	expected="$3"
	deadline=$((SECONDS + 10))
	while [ "$SECONDS" -lt "$deadline" ]; do
		status="$(curl_status "$(http_url "$id")/kv/$key" || true)"
		body="$(cat "$TMP_DIR/curl.out" 2>/dev/null || true)"
		if [ "$status" = "200" ] && [ "$body" = "$expected" ]; then
			return 0
		fi
		sleep 0.2
	done
	printf "timed out waiting for %s on %s; last status=%s body=%s\n" "$key" "$id" "$status" "$body" >&2
	return 1
}

start_node n1
start_node n2
start_node n3

leader="$(wait_for_leader)"
follower="$(follower_for "$leader")"
printf "initial leader=%s follower_for_write=%s\n" "$leader" "$follower"

wait_for_put "$follower" demo before-failover
wait_for_value "$leader" demo before-failover
wait_for_value "$follower" demo before-failover

leader_pid="$(pid_for "$leader")"
printf "stopping leader=%s pid=%s\n" "$leader" "$leader_pid"
kill "$leader_pid"
wait "$leader_pid" >/dev/null 2>&1 || true

new_leader="$(wait_for_leader "$leader")"
new_follower="$(follower_for "$new_leader" "$leader")"
printf "new leader=%s follower_for_write=%s\n" "$new_leader" "$new_follower"

wait_for_put "$new_follower" failover after-failover
wait_for_value "$new_leader" failover after-failover
wait_for_value "$new_follower" failover after-failover

printf "smoke cluster passed\n"
