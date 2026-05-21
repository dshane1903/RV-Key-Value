package main

import "testing"

func TestParsePeers(t *testing.T) {
	ids, addrs, err := parsePeers("n2=localhost:9002,n3=localhost:9003")
	if err != nil {
		t.Fatalf("parse peers: %v", err)
	}

	if len(ids) != 2 {
		t.Fatalf("ids length = %d, want 2", len(ids))
	}
	if ids[0] != "n2" || ids[1] != "n3" {
		t.Fatalf("ids = %v, want [n2 n3]", ids)
	}
	if got := addrs["n2"]; got != "localhost:9002" {
		t.Fatalf("n2 addr = %q, want localhost:9002", got)
	}
	if got := addrs["n3"]; got != "localhost:9003" {
		t.Fatalf("n3 addr = %q, want localhost:9003", got)
	}
}

func TestParsePeersRejectsInvalidPeer(t *testing.T) {
	_, _, err := parsePeers("n2")
	if err == nil {
		t.Fatal("parse peers err = nil, want error")
	}
}

func TestParsePeersRejectsDuplicatePeer(t *testing.T) {
	_, _, err := parsePeers("n2=localhost:9002,n2=localhost:9102")
	if err == nil {
		t.Fatal("parse peers err = nil, want error")
	}
}
