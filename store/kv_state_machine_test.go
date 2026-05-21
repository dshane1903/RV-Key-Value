package store

import (
	"testing"

	"github.com/shaneduncan/rv-key-value/raft"
)

func TestEncodeDecodeCommand(t *testing.T) {
	encoded, err := EncodeCommand(Command{
		Operation: OperationPut,
		Key:       "name",
		Value:     []byte("raft"),
	})
	if err != nil {
		t.Fatalf("encode command: %v", err)
	}

	decoded, err := DecodeCommand(encoded)
	if err != nil {
		t.Fatalf("decode command: %v", err)
	}
	if decoded.Operation != OperationPut {
		t.Fatalf("operation = %s, want %s", decoded.Operation, OperationPut)
	}
	if decoded.Key != "name" {
		t.Fatalf("key = %q, want name", decoded.Key)
	}
	if got := string(decoded.Value); got != "raft" {
		t.Fatalf("value = %q, want raft", got)
	}
}

func TestDecodeCommandRejectsUnknownOperation(t *testing.T) {
	_, err := DecodeCommand([]byte(`{"operation":"get","key":"name"}`))
	if err == nil {
		t.Fatal("decode err = nil, want error")
	}
}

func TestKVStateMachineAppliesPutAndDelete(t *testing.T) {
	stateMachine := NewKVStateMachine()

	put, err := EncodeCommand(Command{
		Operation: OperationPut,
		Key:       "name",
		Value:     []byte("raft"),
	})
	if err != nil {
		t.Fatalf("encode put: %v", err)
	}
	if err := stateMachine.Apply(raft.LogEntry{Index: 1, Term: 1, Command: put}); err != nil {
		t.Fatalf("apply put: %v", err)
	}

	value, ok := stateMachine.Get("name")
	if !ok {
		t.Fatal("key not found after put")
	}
	if got := string(value); got != "raft" {
		t.Fatalf("value = %q, want raft", got)
	}

	value[0] = 'X'
	value, ok = stateMachine.Get("name")
	if !ok {
		t.Fatal("key not found after value mutation")
	}
	if got := string(value); got != "raft" {
		t.Fatalf("stored value = %q, want raft", got)
	}

	del, err := EncodeCommand(Command{
		Operation: OperationDelete,
		Key:       "name",
	})
	if err != nil {
		t.Fatalf("encode delete: %v", err)
	}
	if err := stateMachine.Apply(raft.LogEntry{Index: 2, Term: 1, Command: del}); err != nil {
		t.Fatalf("apply delete: %v", err)
	}
	if _, ok := stateMachine.Get("name"); ok {
		t.Fatal("key found after delete")
	}
}
