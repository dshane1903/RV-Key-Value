package store

import (
	"encoding/json"
	"sync"

	"github.com/shaneduncan/rv-key-value/raft"
	"go.etcd.io/bbolt"
)

var kvBucket = []byte("kv")

type KVStateMachine struct {
	mu   sync.RWMutex
	data map[string][]byte
	db   *bbolt.DB
}

func NewKVStateMachine() *KVStateMachine {
	return &KVStateMachine{data: make(map[string][]byte)}
}

func NewBoltKVStateMachine(db *bbolt.DB) (*KVStateMachine, error) {
	stateMachine := &KVStateMachine{
		data: make(map[string][]byte),
		db:   db,
	}
	if db == nil {
		return stateMachine, nil
	}

	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(kvBucket)
		return err
	}); err != nil {
		return nil, err
	}
	if err := stateMachine.load(); err != nil {
		return nil, err
	}

	return stateMachine, nil
}

func (s *KVStateMachine) Apply(entry raft.LogEntry) error {
	command, err := DecodeCommand(entry.Command)
	if err != nil {
		return err
	}

	if s.db != nil {
		if err := s.applyBolt(command); err != nil {
			return err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	switch command.Operation {
	case OperationNoop:
		return nil
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

func (s *KVStateMachine) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data := make(map[string][]byte, len(s.data))
	for key, value := range s.data {
		data[key] = append([]byte(nil), value...)
	}
	return json.Marshal(data)
}

func (s *KVStateMachine) RestoreSnapshot(snapshot []byte) error {
	if len(snapshot) == 0 {
		return nil
	}

	var data map[string][]byte
	if err := json.Unmarshal(snapshot, &data); err != nil {
		return err
	}

	restored := make(map[string][]byte, len(data))
	for key, value := range data {
		restored[key] = append([]byte(nil), value...)
	}

	if s.db != nil {
		if err := s.restoreBolt(restored); err != nil {
			return err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = restored
	return nil
}

func (s *KVStateMachine) load() error {
	return s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(kvBucket)
		if bucket == nil {
			return nil
		}

		return bucket.ForEach(func(key, value []byte) error {
			s.data[string(key)] = append([]byte(nil), value...)
			return nil
		})
	})
}

func (s *KVStateMachine) restoreBolt(data map[string][]byte) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		if err := tx.DeleteBucket(kvBucket); err != nil && err != bbolt.ErrBucketNotFound {
			return err
		}
		bucket, err := tx.CreateBucketIfNotExists(kvBucket)
		if err != nil {
			return err
		}
		for key, value := range data {
			if err := bucket.Put([]byte(key), value); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *KVStateMachine) applyBolt(command Command) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(kvBucket)
		if err != nil {
			return err
		}

		switch command.Operation {
		case OperationNoop:
			return nil
		case OperationPut:
			return bucket.Put([]byte(command.Key), command.Value)
		case OperationDelete:
			return bucket.Delete([]byte(command.Key))
		default:
			return nil
		}
	})
}
