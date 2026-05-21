package store

import (
	"sync"

	"github.com/shaneduncan/rv-key-value/raft"
)

type KVStateMachine struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewKVStateMachine() *KVStateMachine {
	return &KVStateMachine{data: make(map[string][]byte)}
}

func (s *KVStateMachine) Apply(entry raft.LogEntry) error {
	command, err := DecodeCommand(entry.Command)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	switch command.Operation {
	case OperationPut:
		s.data[command.Key] = append([]byte(nil), command.Value...)
	case OperationDelete:
		delete(s.data, command.Key)
	}
	return nil
}

func (s *KVStateMachine) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.data[key]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), value...), true
}
